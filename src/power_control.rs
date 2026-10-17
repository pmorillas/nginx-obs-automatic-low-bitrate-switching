use std::{
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

use rust_i18n::t;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::{self, Instant},
};
use tracing::{debug, error, info, warn};

use crate::{
    chat,
    state::{ClientStatus, State},
};

/// Environment variable that overrides `powerControl.token`.
pub const TOKEN_ENV: &str = "NOALBS_POWER_TOKEN";

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const WATCH_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const DEFAULT_COOLDOWN_SECS: u64 = 60;

/// Optional HTTP API that can power the broadcasting software on.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PowerControlConfig {
    pub api_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

impl fmt::Debug for PowerControlConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PowerControlConfig")
            .field("api_url", &self.api_url)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Power state reported by the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Power {
    Off,
    Starting,
    On,
    Unavailable,
    #[serde(other)]
    Unknown,
}

impl Power {
    fn is_active(self) -> bool {
        matches!(self, Power::Starting | Power::On)
    }
}

/// Outcome of the last stream start requested through the API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartResult {
    Started,
    Failed,
    Expired,
    Cancelled,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Answer to a wake request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeResult {
    Waking,
    AlreadyOn,
    Unavailable,
    Cooldown,
    Disabled,
    #[serde(other)]
    Unknown,
}

/// Response of `GET {apiUrl}/status`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PowerStatus {
    pub power: Power,
    #[serde(default = "default_true")]
    pub wake_enabled: bool,
    #[serde(default)]
    pub streaming: Option<bool>,
    #[serde(default)]
    pub pending_start: Option<PendingStart>,
    #[serde(default)]
    pub last_start: Option<LastStart>,
    #[serde(default, deserialize_with = "lenient_u64")]
    pub idle_shutdown_minutes: Option<u64>,
}

/// A stream start waiting for the broadcasting software to be ready.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingStart {
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// The most recent stream start handled by the API.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastStart {
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub result: StartResult,
    #[serde(default)]
    pub at: Option<String>,
}

/// Response of `POST {apiUrl}/wake`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WakeResponse {
    pub result: WakeResult,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default, deserialize_with = "lenient_u64")]
    pub retry_after_seconds: Option<u64>,
}

#[derive(Deserialize)]
struct CancelStartResponse {
    #[serde(default)]
    cancelled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WakeRequest<'a> {
    start_stream: bool,
    requested_by: &'a str,
    platform: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelStartRequest<'a> {
    requested_by: &'a str,
}

fn default_true() -> bool {
    true
}

fn lenient_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;

    Ok(value
        .and_then(|v| v.as_f64())
        .filter(|n| n.is_finite() && *n >= 0.0)
        .map(|n| n.ceil() as u64))
}

/// Any of these means the API is treated as unreachable.
#[derive(Debug, thiserror::Error)]
pub enum PowerError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("unexpected HTTP status {0}")]
    Status(reqwest::StatusCode),
    #[error("invalid response body: {0}")]
    Body(#[from] serde_json::Error),
}

/// HTTP client for the power control API.
pub struct PowerClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl PowerClient {
    pub fn new(api_url: &str, token: String) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(format!("noalbs/{}", crate::VERSION))
            .build()?;

        Ok(Self {
            http,
            base_url: api_url.trim_end_matches('/').to_owned(),
            token,
        })
    }

    pub async fn status(&self) -> Result<PowerStatus, PowerError> {
        self.call(self.http.get(self.url("status"))).await
    }

    pub async fn wake(
        &self,
        start_stream: bool,
        requested_by: &str,
        platform: &chat::ChatPlatform,
    ) -> Result<WakeResponse, PowerError> {
        let body = WakeRequest {
            start_stream,
            requested_by,
            platform: platform_name(platform),
        };

        self.call(self.http.post(self.url("wake")).json(&body))
            .await
    }

    /// Returns whether a pending stream start was cancelled.
    pub async fn cancel_start(&self, requested_by: &str) -> Result<bool, PowerError> {
        let body = CancelStartRequest { requested_by };
        let res: CancelStartResponse = self
            .call(self.http.post(self.url("cancel-start")).json(&body))
            .await?;

        Ok(res.cancelled)
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path)
    }

    async fn call<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<T, PowerError> {
        let res = request.bearer_auth(&self.token).send().await?;
        let status = res.status();

        if status == reqwest::StatusCode::UNAUTHORIZED {
            error!("Power control API rejected the token (HTTP 401)");
        }

        if status != reqwest::StatusCode::OK {
            return Err(PowerError::Status(status));
        }

        let body = res.text().await?;
        Ok(serde_json::from_str(&body)?)
    }
}

fn platform_name(platform: &chat::ChatPlatform) -> &'static str {
    match platform {
        chat::ChatPlatform::Twitch => "twitch",
        chat::ChatPlatform::Kick => "kick",
    }
}

/// The env token wins over the config token; empty values count as missing.
pub fn resolve_token(env_token: Option<String>, config_token: Option<&str>) -> Option<String> {
    env_token.filter(|t| !t.trim().is_empty()).or_else(|| {
        config_token
            .filter(|t| !t.trim().is_empty())
            .map(str::to_owned)
    })
}

/// A chat reply produced by the power control flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notice {
    WakingWithStream,
    StartQueued,
    Waking,
    Ready { minutes: u64 },
    ReadyNoIdle,
    Live,
    StartFailed,
    StartExpired,
    StartCancelled,
    WakeFailed,
    Timeout,
    OffHint,
    OffNoWake,
    StopWhileOff,
    StartingWait,
    StatusOff,
    StatusStarting,
    StatusStartingWithStream,
    StatusOn,
    StatusLive,
    AlreadyOn,
    Cooldown { seconds: u64 },
    Unavailable,
    Error,
    NotConfigured,
    ObsUsage,
}

impl Notice {
    /// Renders the chat message in `lang`, showing commands with `prefix`.
    pub fn render(&self, lang: &str, prefix: &str) -> String {
        match self {
            Notice::WakingWithStream => t!("power.wakingWithStream", locale = lang),
            Notice::StartQueued => t!("power.startQueued", locale = lang),
            Notice::Waking => t!("power.waking", locale = lang),
            Notice::Ready { minutes } => t!(
                "power.ready",
                locale = lang,
                prefix = prefix,
                minutes = minutes
            ),
            Notice::ReadyNoIdle => t!("power.readyNoIdle", locale = lang, prefix = prefix),
            Notice::Live => t!("power.live", locale = lang),
            Notice::StartFailed => t!("power.startFailed", locale = lang, prefix = prefix),
            Notice::StartExpired => t!("power.startExpired", locale = lang, prefix = prefix),
            Notice::StartCancelled => t!("power.startCancelled", locale = lang),
            Notice::WakeFailed => t!("power.wakeFailed", locale = lang),
            Notice::Timeout => t!("power.timeout", locale = lang),
            Notice::OffHint => t!("power.offHint", locale = lang, prefix = prefix),
            Notice::OffNoWake => t!("power.offNoWake", locale = lang),
            Notice::StopWhileOff => t!("power.stopWhileOff", locale = lang),
            Notice::StartingWait => t!("power.startingWait", locale = lang),
            Notice::StatusOff => t!("power.statusOff", locale = lang),
            Notice::StatusStarting => t!("power.statusStarting", locale = lang),
            Notice::StatusStartingWithStream => {
                t!("power.statusStartingWithStream", locale = lang)
            }
            Notice::StatusOn => t!("power.statusOn", locale = lang),
            Notice::StatusLive => t!("power.statusLive", locale = lang),
            Notice::AlreadyOn => t!("power.alreadyOn", locale = lang),
            Notice::Cooldown { seconds } => {
                t!("power.cooldown", locale = lang, seconds = seconds)
            }
            Notice::Unavailable => t!("power.unavailable", locale = lang),
            Notice::Error => t!("power.error", locale = lang),
            Notice::NotConfigured => t!("power.notConfigured", locale = lang),
            Notice::ObsUsage => t!("power.obsUsage", locale = lang, prefix = prefix),
        }
    }
}

/// How a chat command depends on the broadcasting software.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatedCommand {
    Start,
    Stop,
    Other,
}

impl GatedCommand {
    /// Returns `None` for commands that work without the broadcasting software.
    pub fn from_command(command: &chat::Command) -> Option<Self> {
        use chat::Command;

        match command {
            Command::Start => Some(Self::Start),
            Command::Stop => Some(Self::Stop),
            Command::Switch
            | Command::LiveScene
            | Command::PrivacyScene
            | Command::StartingScene
            | Command::EndingScene
            | Command::Fix
            | Command::Refresh
            | Command::Rec
            | Command::Source
            | Command::Collection
            | Command::ServerInfo => Some(Self::Other),
            _ => None,
        }
    }
}

/// Result of [`gate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateAction {
    /// Run the command as usual.
    Proceed,
    Reply(Notice),
    /// Ask the API to power on and start the stream once ready.
    WakeWithStream {
        queued: bool,
    },
    CancelStart,
}

/// Decides what a command that needs the broadcasting software does while it is not connected.
pub fn gate(command: GatedCommand, status: &PowerStatus) -> GateAction {
    match status.power {
        Power::On | Power::Unknown => GateAction::Proceed,
        Power::Unavailable => GateAction::Reply(Notice::Unavailable),
        Power::Off if !status.wake_enabled => GateAction::Reply(Notice::OffNoWake),
        Power::Off => match command {
            GatedCommand::Start => GateAction::WakeWithStream { queued: false },
            GatedCommand::Stop => GateAction::Reply(Notice::StopWhileOff),
            GatedCommand::Other => GateAction::Reply(Notice::OffHint),
        },
        Power::Starting => match command {
            GatedCommand::Start => GateAction::WakeWithStream { queued: true },
            GatedCommand::Stop if status.pending_start.is_some() => GateAction::CancelStart,
            GatedCommand::Stop | GatedCommand::Other => GateAction::Reply(Notice::StartingWait),
        },
    }
}

/// Result of [`wake_outcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeOutcome {
    Waking { request_id: Option<String> },
    AlreadyOn,
    Reply(Notice),
}

/// Maps a wake response (`None` when the API was unreachable) to what happens next.
pub fn wake_outcome(response: Option<&WakeResponse>) -> WakeOutcome {
    let Some(response) = response else {
        return WakeOutcome::Reply(Notice::Error);
    };

    match response.result {
        WakeResult::Waking => WakeOutcome::Waking {
            request_id: response.request_id.clone(),
        },
        WakeResult::AlreadyOn => WakeOutcome::AlreadyOn,
        WakeResult::Cooldown => WakeOutcome::Reply(Notice::Cooldown {
            seconds: response
                .retry_after_seconds
                .unwrap_or(DEFAULT_COOLDOWN_SECS),
        }),
        WakeResult::Unavailable => WakeOutcome::Reply(Notice::Unavailable),
        WakeResult::Disabled => WakeOutcome::Reply(Notice::OffNoWake),
        WakeResult::Unknown => WakeOutcome::Reply(Notice::Error),
    }
}

/// Reply for the status subcommand.
pub fn status_notice(status: &PowerStatus) -> Notice {
    match status.power {
        Power::Off => Notice::StatusOff,
        Power::Starting if status.pending_start.is_some() => Notice::StatusStartingWithStream,
        Power::Starting => Notice::StatusStarting,
        Power::On if status.streaming == Some(true) => Notice::StatusLive,
        Power::On => Notice::StatusOn,
        Power::Unavailable => Notice::Unavailable,
        Power::Unknown => Notice::Error,
    }
}

/// What a watcher waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Goal {
    PowerOnly,
    Stream { request_id: Option<String> },
}

/// Combines a new request with the running goal; a stream goal is never downgraded.
pub fn merge_goal(current: &Goal, requested: Goal) -> Goal {
    match (current, requested) {
        (Goal::Stream { request_id: old }, Goal::Stream { request_id: new }) => Goal::Stream {
            request_id: new.or_else(|| old.clone()),
        },
        (_, stream @ Goal::Stream { .. }) => stream,
        (current, Goal::PowerOnly) => current.clone(),
    }
}

/// Result of [`watcher_step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatcherEvent {
    Wait,
    /// Stop watching and post the notice, if any.
    Done(Option<Notice>),
}

/// Evaluates one status poll; `seen_active` covers the previous polls only.
pub fn watcher_step(
    goal: &Goal,
    status: &PowerStatus,
    seen_active: bool,
    obs_connected: bool,
) -> WatcherEvent {
    match goal {
        Goal::Stream { request_id } => {
            if status.streaming == Some(true) {
                return WatcherEvent::Done(Some(Notice::Live));
            }

            if let (Some(id), Some(last)) = (request_id, &status.last_start)
                && last.request_id.as_deref() == Some(id.as_str())
            {
                match last.result {
                    StartResult::Started => return WatcherEvent::Done(Some(Notice::Live)),
                    StartResult::Failed => return WatcherEvent::Done(Some(Notice::StartFailed)),
                    StartResult::Expired => return WatcherEvent::Done(Some(Notice::StartExpired)),
                    StartResult::Cancelled => return WatcherEvent::Done(None),
                    StartResult::Unknown => {}
                }
            }
        }
        Goal::PowerOnly => {
            if status.power == Power::On && obs_connected {
                let notice = match status.idle_shutdown_minutes {
                    Some(minutes) => Notice::Ready { minutes },
                    None => Notice::ReadyNoIdle,
                };

                return WatcherEvent::Done(Some(notice));
            }
        }
    }

    if seen_active && matches!(status.power, Power::Off | Power::Unavailable) {
        return WatcherEvent::Done(Some(Notice::WakeFailed));
    }

    WatcherEvent::Wait
}

/// Whether the broadcasting software should retry connecting right away.
pub fn should_nudge(status: &PowerStatus, obs_connected: bool) -> bool {
    status.power == Power::On && !obs_connected
}

#[derive(Default)]
struct WatcherSlot {
    next_id: u64,
    active: Option<ActiveWatcher>,
}

struct ActiveWatcher {
    id: u64,
    goal: Goal,
}

/// Where and how a watcher reports back.
pub struct WatchContext {
    pub chat_sender: Arc<dyn chat::ChatLogic>,
    pub channel: String,
    pub lang: String,
    pub prefix: String,
    pub state: Weak<RwLock<State>>,
    pub reconnect: Arc<Notify>,
}

/// Power control integration of one NOALBS user.
pub struct PowerControl {
    client: Arc<PowerClient>,
    watcher: Arc<Mutex<WatcherSlot>>,
}

impl PowerControl {
    /// Returns `None` when power control is not configured or not usable.
    pub fn from_config(config: Option<&PowerControlConfig>) -> Option<Self> {
        let config = config?;
        let token = resolve_token(std::env::var(TOKEN_ENV).ok(), config.token.as_deref());

        Self::new(config, token)
    }

    fn new(config: &PowerControlConfig, token: Option<String>) -> Option<Self> {
        if config.api_url.trim().is_empty() {
            warn!("powerControl.apiUrl is empty, power control disabled");
            return None;
        }

        let Some(token) = token else {
            warn!(
                "powerControl is set but no token was found ({} or powerControl.token), power control disabled",
                TOKEN_ENV
            );
            return None;
        };

        let client = match PowerClient::new(config.api_url.trim(), token) {
            Ok(c) => c,
            Err(e) => {
                error!("Unable to create the power control client: {}", e);
                return None;
            }
        };

        info!("Power control enabled");

        Some(Self {
            client: Arc::new(client),
            watcher: Arc::new(Mutex::new(WatcherSlot::default())),
        })
    }

    pub fn client(&self) -> &PowerClient {
        &self.client
    }

    /// Starts a watcher for `goal`, or merges it into the running one; returns the watched goal.
    pub async fn watch(&self, goal: Goal, ctx: WatchContext) -> Goal {
        let mut slot = self.watcher.lock().await;

        if let Some(active) = slot.active.as_mut() {
            active.goal = merge_goal(&active.goal, goal);
            debug!(
                "Power watcher {} now waits for {:?}",
                active.id, active.goal
            );
            return active.goal.clone();
        }

        slot.next_id += 1;
        let id = slot.next_id;
        slot.active = Some(ActiveWatcher {
            id,
            goal: goal.clone(),
        });
        drop(slot);

        debug!("Power watcher {} started for {:?}", id, goal);
        tokio::spawn(run_watcher(
            self.client.clone(),
            self.watcher.clone(),
            id,
            ctx,
        ));

        goal
    }

    /// Stops the running watcher without posting anything.
    pub async fn stop_watching(&self) {
        self.watcher.lock().await.active = None;
    }
}

async fn run_watcher(
    client: Arc<PowerClient>,
    slot: Arc<Mutex<WatcherSlot>>,
    id: u64,
    ctx: WatchContext,
) {
    let deadline = Instant::now() + WATCH_TIMEOUT;
    let mut seen_active = false;

    loop {
        time::sleep(POLL_INTERVAL).await;

        let obs_connected = match ctx.state.upgrade() {
            Some(state) => {
                state.read().await.broadcasting_software.status == ClientStatus::Connected
            }
            None => {
                let mut slot = slot.lock().await;
                if slot.active.as_ref().is_some_and(|a| a.id == id) {
                    slot.active = None;
                }
                return;
            }
        };

        let status = client
            .status()
            .await
            .inspect_err(|e| debug!("Power watcher {} poll failed: {}", id, e))
            .ok();

        let event = {
            let mut slot = slot.lock().await;
            let Some(active) = slot.active.as_ref().filter(|a| a.id == id) else {
                debug!("Power watcher {} stopped", id);
                return;
            };

            let mut event = match &status {
                Some(s) => watcher_step(&active.goal, s, seen_active, obs_connected),
                None => WatcherEvent::Wait,
            };

            if event == WatcherEvent::Wait && Instant::now() >= deadline {
                event = WatcherEvent::Done(Some(Notice::Timeout));
            }

            if matches!(event, WatcherEvent::Done(_)) {
                slot.active = None;
            }

            event
        };

        if let Some(s) = &status {
            seen_active |= s.power.is_active();

            if should_nudge(s, obs_connected) {
                ctx.reconnect.notify_one();
            }
        }

        if let WatcherEvent::Done(notice) = event {
            debug!("Power watcher {} done: {:?}", id, notice);

            if let Some(notice) = notice {
                let msg = notice.render(&ctx.lang, &ctx.prefix);
                ctx.chat_sender.send_message(ctx.channel.clone(), msg).await;
            }

            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(power: Power) -> PowerStatus {
        PowerStatus {
            power,
            wake_enabled: true,
            streaming: None,
            pending_start: None,
            last_start: None,
            idle_shutdown_minutes: Some(30),
        }
    }

    fn pending() -> Option<PendingStart> {
        Some(PendingStart {
            request_id: Some("req-1".to_string()),
            expires_at: Some("2026-10-17T12:00:00Z".to_string()),
        })
    }

    fn last_start(id: &str, result: StartResult) -> Option<LastStart> {
        Some(LastStart {
            request_id: Some(id.to_string()),
            result,
            at: None,
        })
    }

    fn stream_goal(id: &str) -> Goal {
        Goal::Stream {
            request_id: Some(id.to_string()),
        }
    }

    #[test]
    fn parses_full_status() {
        let s: PowerStatus = serde_json::from_str(
            r#"{
                "power": "starting",
                "wakeEnabled": false,
                "streaming": null,
                "pendingStart": { "requestId": "req-1", "expiresAt": "2026-10-17T12:00:00Z" },
                "lastStart": { "requestId": "req-0", "result": "failed", "at": "2026-10-17T11:00:00Z" },
                "idleShutdownMinutes": 30,
                "somethingNew": { "nested": true }
            }"#,
        )
        .unwrap();

        assert_eq!(s.power, Power::Starting);
        assert!(!s.wake_enabled);
        assert_eq!(s.streaming, None);
        assert_eq!(s.pending_start, pending());
        assert_eq!(
            s.last_start,
            Some(LastStart {
                request_id: Some("req-0".to_string()),
                result: StartResult::Failed,
                at: Some("2026-10-17T11:00:00Z".to_string()),
            })
        );
        assert_eq!(s.idle_shutdown_minutes, Some(30));
    }

    #[test]
    fn parses_minimal_status_with_defaults() {
        let s: PowerStatus = serde_json::from_str(r#"{ "power": "on" }"#).unwrap();

        assert_eq!(s.power, Power::On);
        assert!(s.wake_enabled);
        assert_eq!(s.streaming, None);
        assert_eq!(s.pending_start, None);
        assert_eq!(s.last_start, None);
        assert_eq!(s.idle_shutdown_minutes, None);
    }

    #[test]
    fn parses_unknown_values_without_failing() {
        let s: PowerStatus = serde_json::from_str(
            r#"{
                "power": "hibernating",
                "lastStart": { "requestId": "req-1", "result": "exploded" },
                "idleShutdownMinutes": "soon"
            }"#,
        )
        .unwrap();

        assert_eq!(s.power, Power::Unknown);
        assert_eq!(s.last_start.unwrap().result, StartResult::Unknown);
        assert_eq!(s.idle_shutdown_minutes, None);

        let s: PowerStatus =
            serde_json::from_str(r#"{ "power": "off", "idleShutdownMinutes": 29.5 }"#).unwrap();
        assert_eq!(s.idle_shutdown_minutes, Some(30));
    }

    #[test]
    fn rejects_status_without_power() {
        assert!(serde_json::from_str::<PowerStatus>(r#"{ "streaming": true }"#).is_err());
        assert!(serde_json::from_str::<PowerStatus>("<html>").is_err());
    }

    #[test]
    fn parses_wake_responses() {
        let w: WakeResponse = serde_json::from_str(
            r#"{ "result": "waking", "requestId": "req-1", "retryAfterSeconds": null }"#,
        )
        .unwrap();
        assert_eq!(w.result, WakeResult::Waking);
        assert_eq!(w.request_id.as_deref(), Some("req-1"));
        assert_eq!(w.retry_after_seconds, None);

        let w: WakeResponse =
            serde_json::from_str(r#"{ "result": "cooldown", "retryAfterSeconds": 120 }"#).unwrap();
        assert_eq!(w.result, WakeResult::Cooldown);
        assert_eq!(w.retry_after_seconds, Some(120));

        let w: WakeResponse = serde_json::from_str(r#"{ "result": "already_on" }"#).unwrap();
        assert_eq!(w.result, WakeResult::AlreadyOn);

        let w: WakeResponse = serde_json::from_str(r#"{ "result": "maybe" }"#).unwrap();
        assert_eq!(w.result, WakeResult::Unknown);
    }

    #[test]
    fn token_resolution() {
        assert_eq!(
            resolve_token(Some("env".into()), Some("cfg")).as_deref(),
            Some("env")
        );
        assert_eq!(resolve_token(None, Some("cfg")).as_deref(), Some("cfg"));
        assert_eq!(
            resolve_token(Some(" ".into()), Some("cfg")).as_deref(),
            Some("cfg")
        );
        assert_eq!(resolve_token(None, Some("")), None);
        assert_eq!(resolve_token(None, None), None);
    }

    #[test]
    fn missing_token_disables_power_control() {
        let config = PowerControlConfig {
            api_url: "http://localhost:1/power".to_string(),
            token: None,
        };
        assert!(PowerControl::new(&config, None).is_none());
        assert!(PowerControl::new(&config, Some("t".into())).is_some());

        let empty_url = PowerControlConfig {
            api_url: " ".to_string(),
            token: None,
        };
        assert!(PowerControl::new(&empty_url, Some("t".into())).is_none());
    }

    #[test]
    fn debug_redacts_token() {
        let config = PowerControlConfig {
            api_url: "http://localhost/power".to_string(),
            token: Some("super-secret".to_string()),
        };

        assert!(!format!("{:?}", config).contains("super-secret"));
    }

    #[test]
    fn gated_commands() {
        use chat::Command;

        assert_eq!(
            GatedCommand::from_command(&Command::Start),
            Some(GatedCommand::Start)
        );
        assert_eq!(
            GatedCommand::from_command(&Command::Stop),
            Some(GatedCommand::Stop)
        );

        for c in [
            Command::Switch,
            Command::LiveScene,
            Command::PrivacyScene,
            Command::StartingScene,
            Command::EndingScene,
            Command::Fix,
            Command::Refresh,
            Command::Rec,
            Command::Source,
            Command::Collection,
            Command::ServerInfo,
        ] {
            assert_eq!(GatedCommand::from_command(&c), Some(GatedCommand::Other));
        }

        for c in [
            Command::Bitrate,
            Command::Obs,
            Command::Noalbs,
            Command::Trigger,
            Command::Sourceinfo,
            Command::Version,
            Command::Unknown("x".to_string()),
        ] {
            assert_eq!(GatedCommand::from_command(&c), None);
        }
    }

    #[test]
    fn gate_on_or_unknown_proceeds() {
        for power in [Power::On, Power::Unknown] {
            for c in [GatedCommand::Start, GatedCommand::Stop, GatedCommand::Other] {
                assert_eq!(gate(c, &status(power)), GateAction::Proceed);
            }
        }
    }

    #[test]
    fn gate_unavailable() {
        for c in [GatedCommand::Start, GatedCommand::Stop, GatedCommand::Other] {
            assert_eq!(
                gate(c, &status(Power::Unavailable)),
                GateAction::Reply(Notice::Unavailable)
            );
        }
    }

    #[test]
    fn gate_off() {
        let off = status(Power::Off);
        assert_eq!(
            gate(GatedCommand::Start, &off),
            GateAction::WakeWithStream { queued: false }
        );
        assert_eq!(
            gate(GatedCommand::Stop, &off),
            GateAction::Reply(Notice::StopWhileOff)
        );
        assert_eq!(
            gate(GatedCommand::Other, &off),
            GateAction::Reply(Notice::OffHint)
        );
    }

    #[test]
    fn gate_off_without_wake() {
        let off = PowerStatus {
            wake_enabled: false,
            ..status(Power::Off)
        };

        for c in [GatedCommand::Start, GatedCommand::Stop, GatedCommand::Other] {
            assert_eq!(gate(c, &off), GateAction::Reply(Notice::OffNoWake));
        }
    }

    #[test]
    fn gate_starting() {
        let starting = status(Power::Starting);
        assert_eq!(
            gate(GatedCommand::Start, &starting),
            GateAction::WakeWithStream { queued: true }
        );
        assert_eq!(
            gate(GatedCommand::Stop, &starting),
            GateAction::Reply(Notice::StartingWait)
        );
        assert_eq!(
            gate(GatedCommand::Other, &starting),
            GateAction::Reply(Notice::StartingWait)
        );

        let with_pending = PowerStatus {
            pending_start: pending(),
            ..status(Power::Starting)
        };
        assert_eq!(
            gate(GatedCommand::Stop, &with_pending),
            GateAction::CancelStart
        );
        assert_eq!(
            gate(GatedCommand::Start, &with_pending),
            GateAction::WakeWithStream { queued: true }
        );
        assert_eq!(
            gate(GatedCommand::Other, &with_pending),
            GateAction::Reply(Notice::StartingWait)
        );
    }

    #[test]
    fn wake_outcomes() {
        let wake = |result, request_id: Option<&str>, retry| WakeResponse {
            result,
            request_id: request_id.map(str::to_owned),
            retry_after_seconds: retry,
        };

        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::Waking, Some("req-1"), None))),
            WakeOutcome::Waking {
                request_id: Some("req-1".to_string())
            }
        );
        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::AlreadyOn, None, None))),
            WakeOutcome::AlreadyOn
        );
        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::Cooldown, None, Some(120)))),
            WakeOutcome::Reply(Notice::Cooldown { seconds: 120 })
        );
        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::Cooldown, None, None))),
            WakeOutcome::Reply(Notice::Cooldown {
                seconds: DEFAULT_COOLDOWN_SECS
            })
        );
        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::Unavailable, None, None))),
            WakeOutcome::Reply(Notice::Unavailable)
        );
        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::Disabled, None, None))),
            WakeOutcome::Reply(Notice::OffNoWake)
        );
        assert_eq!(
            wake_outcome(Some(&wake(WakeResult::Unknown, None, None))),
            WakeOutcome::Reply(Notice::Error)
        );
        assert_eq!(wake_outcome(None), WakeOutcome::Reply(Notice::Error));
    }

    #[test]
    fn status_notices() {
        assert_eq!(status_notice(&status(Power::Off)), Notice::StatusOff);
        assert_eq!(
            status_notice(&status(Power::Starting)),
            Notice::StatusStarting
        );
        assert_eq!(
            status_notice(&PowerStatus {
                pending_start: pending(),
                ..status(Power::Starting)
            }),
            Notice::StatusStartingWithStream
        );
        assert_eq!(status_notice(&status(Power::On)), Notice::StatusOn);
        assert_eq!(
            status_notice(&PowerStatus {
                streaming: Some(false),
                ..status(Power::On)
            }),
            Notice::StatusOn
        );
        assert_eq!(
            status_notice(&PowerStatus {
                streaming: Some(true),
                ..status(Power::On)
            }),
            Notice::StatusLive
        );
        assert_eq!(
            status_notice(&status(Power::Unavailable)),
            Notice::Unavailable
        );
        assert_eq!(status_notice(&status(Power::Unknown)), Notice::Error);
    }

    #[test]
    fn goal_merge_never_downgrades() {
        let s1 = stream_goal("req-1");
        let s2 = stream_goal("req-2");
        let no_id = Goal::Stream { request_id: None };

        assert_eq!(
            merge_goal(&Goal::PowerOnly, Goal::PowerOnly),
            Goal::PowerOnly
        );
        assert_eq!(merge_goal(&Goal::PowerOnly, s1.clone()), s1);
        assert_eq!(merge_goal(&s1, Goal::PowerOnly), s1);
        assert_eq!(merge_goal(&s1, s2.clone()), s2);
        assert_eq!(merge_goal(&s1, no_id.clone()), s1);
        assert_eq!(merge_goal(&no_id, s1.clone()), s1);
    }

    #[test]
    fn watcher_stream_goes_live() {
        let goal = stream_goal("req-1");
        let live = PowerStatus {
            streaming: Some(true),
            ..status(Power::On)
        };
        assert_eq!(
            watcher_step(&goal, &live, true, false),
            WatcherEvent::Done(Some(Notice::Live))
        );

        let goal_no_id = Goal::Stream { request_id: None };
        assert_eq!(
            watcher_step(&goal_no_id, &live, false, true),
            WatcherEvent::Done(Some(Notice::Live))
        );
    }

    #[test]
    fn watcher_stream_results() {
        let goal = stream_goal("req-1");
        let cases = [
            (StartResult::Started, WatcherEvent::Done(Some(Notice::Live))),
            (
                StartResult::Failed,
                WatcherEvent::Done(Some(Notice::StartFailed)),
            ),
            (
                StartResult::Expired,
                WatcherEvent::Done(Some(Notice::StartExpired)),
            ),
            (StartResult::Cancelled, WatcherEvent::Done(None)),
            (StartResult::Unknown, WatcherEvent::Wait),
        ];

        for (result, expected) in cases {
            let s = PowerStatus {
                last_start: last_start("req-1", result),
                ..status(Power::On)
            };
            assert_eq!(watcher_step(&goal, &s, true, true), expected, "{result:?}");
        }
    }

    #[test]
    fn watcher_stream_ignores_other_requests() {
        let goal = stream_goal("req-2");
        let s = PowerStatus {
            last_start: last_start("req-1", StartResult::Failed),
            ..status(Power::On)
        };
        assert_eq!(watcher_step(&goal, &s, true, true), WatcherEvent::Wait);

        let goal_no_id = Goal::Stream { request_id: None };
        assert_eq!(
            watcher_step(&goal_no_id, &s, true, true),
            WatcherEvent::Wait
        );
    }

    #[test]
    fn watcher_stream_waits_while_powering_on() {
        let goal = stream_goal("req-1");
        assert_eq!(
            watcher_step(&goal, &status(Power::Starting), false, false),
            WatcherEvent::Wait
        );
        assert_eq!(
            watcher_step(&goal, &status(Power::On), true, true),
            WatcherEvent::Wait
        );
    }

    #[test]
    fn watcher_power_only_ready() {
        let on = status(Power::On);
        assert_eq!(
            watcher_step(&Goal::PowerOnly, &on, true, true),
            WatcherEvent::Done(Some(Notice::Ready { minutes: 30 }))
        );

        let no_idle = PowerStatus {
            idle_shutdown_minutes: None,
            ..status(Power::On)
        };
        assert_eq!(
            watcher_step(&Goal::PowerOnly, &no_idle, false, true),
            WatcherEvent::Done(Some(Notice::ReadyNoIdle))
        );
    }

    #[test]
    fn watcher_power_only_waits_for_connection() {
        assert_eq!(
            watcher_step(&Goal::PowerOnly, &status(Power::On), true, false),
            WatcherEvent::Wait
        );
        assert_eq!(
            watcher_step(&Goal::PowerOnly, &status(Power::Starting), true, false),
            WatcherEvent::Wait
        );
    }

    #[test]
    fn watcher_seen_active_rule() {
        for goal in [Goal::PowerOnly, stream_goal("req-1")] {
            for power in [Power::Off, Power::Unavailable] {
                assert_eq!(
                    watcher_step(&goal, &status(power), false, false),
                    WatcherEvent::Wait,
                    "{goal:?} {power:?} before active"
                );
                assert_eq!(
                    watcher_step(&goal, &status(power), true, false),
                    WatcherEvent::Done(Some(Notice::WakeFailed)),
                    "{goal:?} {power:?} after active"
                );
            }

            assert_eq!(
                watcher_step(&goal, &status(Power::Unknown), true, false),
                WatcherEvent::Wait
            );
        }
    }

    #[test]
    fn nudge_only_when_on_and_disconnected() {
        assert!(should_nudge(&status(Power::On), false));
        assert!(!should_nudge(&status(Power::On), true));
        assert!(!should_nudge(&status(Power::Starting), false));
        assert!(!should_nudge(&status(Power::Off), false));
    }

    #[test]
    fn notices_render_in_english_with_params() {
        assert_eq!(
            Notice::ObsUsage.render("en", "#"),
            "Usage: #obs on | #obs status"
        );

        let ready = Notice::Ready { minutes: 30 }.render("en", "!");
        assert!(ready.contains("!start") && ready.contains("30"), "{ready}");

        let cooldown = Notice::Cooldown { seconds: 120 }.render("es", "!");
        assert!(cooldown.contains("120"), "{cooldown}");
    }

    #[test]
    fn notices_fall_back_to_english() {
        assert_eq!(
            Notice::Live.render("de", "!"),
            Notice::Live.render("en", "!")
        );
        assert!(!Notice::Live.render("de", "!").contains("power.live"));
    }

    mod http {
        use super::super::*;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        struct Captured {
            request_line: String,
            headers: Vec<(String, String)>,
            body: String,
        }

        impl Captured {
            fn header(&self, name: &str) -> Option<&str> {
                self.headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.as_str())
            }
        }

        async fn read_request(stream: &mut tokio::net::TcpStream) -> Captured {
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];

            let header_end = loop {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "connection closed before the headers ended");
                buf.extend_from_slice(&chunk[..n]);

                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };

            let head = String::from_utf8(buf[..header_end].to_vec()).unwrap();
            let mut lines = head.split("\r\n");
            let request_line = lines.next().unwrap().to_string();
            let headers: Vec<(String, String)> = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .collect();

            let length = headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.parse::<usize>().unwrap())
                .unwrap_or(0);

            while buf.len() < header_end + length {
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0, "connection closed before the body ended");
                buf.extend_from_slice(&chunk[..n]);
            }

            Captured {
                request_line,
                headers,
                body: String::from_utf8(buf[header_end..header_end + length].to_vec()).unwrap(),
            }
        }

        /// Serves one canned response per connection and returns the captured requests.
        async fn serve(
            listener: TcpListener,
            responses: Vec<(u16, &'static str)>,
        ) -> Vec<Captured> {
            let mut captured = Vec::new();

            for (code, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                captured.push(read_request(&mut stream).await);

                let response = format!(
                    "HTTP/1.1 {code} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }

            captured
        }

        #[tokio::test]
        async fn client_sends_auth_user_agent_and_json() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(serve(
                listener,
                vec![
                    (
                        200,
                        r#"{"power":"off","wakeEnabled":true,"idleShutdownMinutes":30}"#,
                    ),
                    (
                        200,
                        r#"{"result":"waking","requestId":"req-1","retryAfterSeconds":null}"#,
                    ),
                    (200, r#"{"cancelled":true}"#),
                    (401, r#"{"message":"Unauthorized"}"#),
                    (200, "not json"),
                ],
            ));

            let client = PowerClient::new(
                &format!("http://{addr}/internal/power/"),
                "test-token".into(),
            )
            .unwrap();

            let status = client.status().await.unwrap();
            assert_eq!(status.power, Power::Off);
            assert_eq!(status.idle_shutdown_minutes, Some(30));

            let wake = client
                .wake(true, "some_admin", &chat::ChatPlatform::Kick)
                .await
                .unwrap();
            assert_eq!(wake.result, WakeResult::Waking);
            assert_eq!(wake.request_id.as_deref(), Some("req-1"));

            assert!(client.cancel_start("some_admin").await.unwrap());

            assert!(matches!(
                client.status().await,
                Err(PowerError::Status(s)) if s == reqwest::StatusCode::UNAUTHORIZED
            ));
            assert!(matches!(client.status().await, Err(PowerError::Body(_))));

            let requests = server.await.unwrap();
            let user_agent = format!("noalbs/{}", crate::VERSION);

            for r in &requests {
                assert_eq!(r.header("authorization"), Some("Bearer test-token"));
                assert_eq!(r.header("user-agent"), Some(user_agent.as_str()));
            }

            assert_eq!(
                requests[0].request_line,
                "GET /internal/power/status HTTP/1.1"
            );
            assert!(requests[0].body.is_empty());

            assert_eq!(
                requests[1].request_line,
                "POST /internal/power/wake HTTP/1.1"
            );
            assert_eq!(requests[1].header("content-type"), Some("application/json"));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&requests[1].body).unwrap(),
                serde_json::json!({
                    "startStream": true,
                    "requestedBy": "some_admin",
                    "platform": "kick"
                })
            );

            assert_eq!(
                requests[2].request_line,
                "POST /internal/power/cancel-start HTTP/1.1"
            );
            assert_eq!(requests[2].header("content-type"), Some("application/json"));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&requests[2].body).unwrap(),
                serde_json::json!({ "requestedBy": "some_admin" })
            );
        }

        #[tokio::test]
        async fn unreachable_api_is_an_error() {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);

            let client = PowerClient::new(&format!("http://{addr}"), "t".into()).unwrap();
            assert!(matches!(client.status().await, Err(PowerError::Request(_))));
        }
    }
}
