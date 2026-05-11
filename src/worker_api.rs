use std::{
    collections::{HashMap, VecDeque},
    error::Error as StdError,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use libsignal_core::DeviceId;
use libsignal_protocol::kem;
use libsignal_protocol::{
    KyberPreKeyId, PreKeyId, PreKeySignalMessage, SignalMessage, SignedPreKeyId,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::http_retry::{
    is_connect_stage_reqwest_error, is_transient_reqwest_error, is_transient_status,
    retry_transient_http_async, RetryDecision,
};
use crate::key_repository::{
    OneTimePrekeyStorable, PrekeyBundleBatchStorable, PrekeyBundleStorable,
};
use crate::signal_participant::SignalParticipant;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    RegisterParticipant,
    GeneratePrekeyBundle,
    PublishPrekeyBundle,
    EstablishSessions {
        participants: Vec<String>,
    },
    EncryptMessage {
        recipient: String,
        message: String,
        #[serde(default)]
        conversation_size: Option<usize>,
    },
    DecryptMessage {
        sender: String,
        profile: bool,
        #[serde(default)]
        conversation_size: Option<usize>,
    },
    ProcessPending {
        max_messages: Option<usize>,
    },
    ShowParticipantState,
    RemoveParticipants {
        participants: Vec<String>,
    },
}

impl Command {
    pub fn kind(&self) -> &'static str {
        match self {
            Command::RegisterParticipant => "RegisterParticipant",
            Command::GeneratePrekeyBundle => "GeneratePrekeyBundle",
            Command::PublishPrekeyBundle => "PublishPrekeyBundle",
            Command::EstablishSessions { .. } => "EstablishSessions",
            Command::EncryptMessage { .. } => "EncryptMessage",
            Command::DecryptMessage { .. } => "DecryptMessage",
            Command::ProcessPending { .. } => "ProcessPending",
            Command::ShowParticipantState => "ShowParticipantState",
            Command::RemoveParticipants { .. } => "RemoveParticipants",
        }
    }

    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Command::RegisterParticipant
                | Command::GeneratePrekeyBundle
                | Command::PublishPrekeyBundle
                | Command::EstablishSessions { .. }
                | Command::EncryptMessage { .. }
                | Command::DecryptMessage { .. }
                | Command::ProcessPending { .. }
                | Command::RemoveParticipants { .. }
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequestEnvelope {
    pub request_id: String,
    pub command: Command,
    #[serde(default)]
    pub phase: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IncomingCommandRequest {
    Envelope(CommandRequestEnvelope),
    Raw(Command),
}

impl IncomingCommandRequest {
    pub fn into_parts(self) -> (Option<String>, Command, Option<String>) {
        match self {
            IncomingCommandRequest::Envelope(envelope) => {
                (Some(envelope.request_id), envelope.command, envelope.phase)
            }
            IncomingCommandRequest::Raw(command) => (None, command, None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub status: String,
    pub message: String,
}

impl CommandResponse {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            status: "ok".to_string(),
            message: message.into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommandMetrics {
    pub artifact_size_bytes: Option<usize>,
    pub participant_count: Option<usize>,
    pub conversation_size: Option<usize>,
    pub prekey_bundle_count: Option<usize>,
    pub session_count: Option<usize>,
    pub ratchet_step_count: Option<usize>,
    pub ciphertext_bytes: Option<usize>,
    pub plaintext_bytes: Option<usize>,
}

impl CommandMetrics {
    fn merge_message(&mut self, other: &CommandMetrics) {
        self.artifact_size_bytes = add_options(self.artifact_size_bytes, other.artifact_size_bytes);
        self.prekey_bundle_count = add_options(self.prekey_bundle_count, other.prekey_bundle_count);
        self.session_count = add_options(self.session_count, other.session_count);
        self.ratchet_step_count = add_options(self.ratchet_step_count, other.ratchet_step_count);
        self.ciphertext_bytes = add_options(self.ciphertext_bytes, other.ciphertext_bytes);
        self.plaintext_bytes = add_options(self.plaintext_bytes, other.plaintext_bytes);
    }
}

#[derive(Debug, Clone)]
pub struct CommandOutcome {
    pub message: String,
    pub metrics: CommandMetrics,
}

impl CommandOutcome {
    fn new(message: impl Into<String>, metrics: CommandMetrics) -> Self {
        Self {
            message: message.into(),
            metrics,
        }
    }

    fn message(message: impl Into<String>) -> Self {
        Self::new(message, CommandMetrics::default())
    }
}

fn add_options(a: Option<usize>, b: Option<usize>) -> Option<usize> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.saturating_add(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandRequest {
    pub items: Vec<BatchCommandItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandItem {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub command: Command,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub profile: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandResponse {
    pub items: Vec<BatchCommandResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchCommandResult {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub response: CommandResponse,
}

#[derive(Debug, Clone)]
struct CachedCommandResponse {
    response: CommandResponse,
    completed_at: Instant,
}

#[derive(Debug)]
pub struct CompletedCommandCache {
    entries: HashMap<String, CachedCommandResponse>,
    order: VecDeque<String>,
    max_entries: usize,
    ttl: Duration,
}

impl CompletedCommandCache {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_entries,
            ttl,
        }
    }

    pub fn get(&mut self, request_id: &str) -> Option<CommandResponse> {
        self.prune_expired();
        self.entries
            .get(request_id)
            .map(|cached| cached.response.clone())
    }

    pub fn insert(&mut self, request_id: String, response: CommandResponse) {
        if self.max_entries == 0 {
            return;
        }

        self.prune_expired();

        if !self.entries.contains_key(&request_id) {
            self.order.push_back(request_id.clone());
        }

        self.entries.insert(
            request_id,
            CachedCommandResponse {
                response,
                completed_at: Instant::now(),
            },
        );

        while self.entries.len() > self.max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    fn prune_expired(&mut self) {
        if self.ttl.is_zero() {
            self.entries.clear();
            self.order.clear();
            return;
        }

        let now = Instant::now();

        while let Some(request_id) = self.order.front() {
            let expired = self
                .entries
                .get(request_id)
                .map(|cached| now.duration_since(cached.completed_at) > self.ttl)
                .unwrap_or(true);

            if !expired {
                break;
            }

            let request_id = self.order.pop_front().expect("front checked above");
            self.entries.remove(&request_id);
        }
    }
}

static CONTROL_HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(worker_http_connect_timeout_ms()))
        .timeout(Duration::from_millis(worker_http_request_timeout_ms()))
        .pool_max_idle_per_host(control_http_pool_max_idle_per_host())
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .build()
        .expect("failed to build HTTP client")
});

static OUTBOUND_HTTP_SEMAPHORE: Lazy<tokio::sync::Semaphore> =
    Lazy::new(|| tokio::sync::Semaphore::new(worker_outbound_http_permits()));

fn control_http_pool_max_idle_per_host() -> usize {
    std::env::var("SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(32)
}

fn worker_http_connect_timeout_ms() -> u64 {
    std::env::var("SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(5_000)
}

fn worker_http_request_timeout_ms() -> u64 {
    std::env::var("SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30_000)
}

fn worker_outbound_http_permits() -> usize {
    std::env::var("SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|permits| *permits > 0)
        .unwrap_or(32)
}

fn control_http_client() -> &'static reqwest::Client {
    &CONTROL_HTTP_CLIENT
}

async fn acquire_http_permit() -> tokio::sync::SemaphorePermit<'static> {
    OUTBOUND_HTTP_SEMAPHORE
        .acquire()
        .await
        .expect("HTTP semaphore was closed")
}

fn transient_or_fatal<T>(err: reqwest::Error) -> RetryDecision<T> {
    if is_transient_reqwest_error(&err) {
        RetryDecision::Transient(reqwest_error_diagnostic(&err))
    } else {
        RetryDecision::Fatal(anyhow!(err))
    }
}

fn reqwest_error_diagnostic(err: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    parts.push(format!("top_level={}", err));
    parts.push(format!("is_connect={}", err.is_connect()));
    parts.push(format!("is_timeout={}", err.is_timeout()));
    parts.push(format!("is_request={}", err.is_request()));
    parts.push(format!("is_body={}", err.is_body()));
    parts.push(format!(
        "connect_stage={}",
        is_connect_stage_reqwest_error(err)
    ));

    let mut source = err.source();
    let mut idx = 0usize;
    while let Some(err) = source {
        parts.push(format!("source[{}]={}", idx, err));
        source = err.source();
        idx += 1;
    }

    parts.join("; ")
}

async fn read_response_text(response: reqwest::Response) -> String {
    response.text().await.unwrap_or_default()
}

pub async fn kr_post_bytes(
    kr_url: &str,
    path: &str,
    bytes: Vec<u8>,
    op: &str,
    participant_id: &str,
) -> Result<()> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || {
        let request_bytes = bytes.clone();
        async {
            let _permit = acquire_http_permit().await;
            let response = match http.post(&url).body(request_bytes).send().await {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if status.is_success() {
                return RetryDecision::Success(());
            }

            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            RetryDecision::Fatal(anyhow!("KR POST failed with status {}: {}", status, body))
        }
    })
    .await
}

pub async fn kr_post_empty(kr_url: &str, path: &str, op: &str, participant_id: &str) -> Result<()> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.post(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let body = read_response_text(response).await;

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!(
            "KR POST empty failed with status {}: {}",
            status,
            body
        ))
    })
    .await
}

pub async fn kr_put_json<T: Serialize>(
    kr_url: &str,
    path: &str,
    value: &T,
    op: &str,
    participant_id: &str,
) -> Result<()> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.put(&url).json(value).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let response_body = read_response_text(response).await;

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, response_body));
        }

        RetryDecision::Fatal(anyhow!(
            "KR PUT failed with status {}: {}",
            status,
            response_body
        ))
    })
    .await
}

pub async fn kr_get_bytes(
    kr_url: &str,
    path: &str,
    op: &str,
    participant_id: &str,
) -> Result<Vec<u8>> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.get(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!("KR GET failed with status {}: {}", status, body));
        }

        match response.bytes().await {
            Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

pub async fn kr_get_json<T: for<'de> Deserialize<'de>>(
    kr_url: &str,
    path: &str,
    op: &str,
    participant_id: &str,
) -> Result<T> {
    let url = format!("{kr_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(participant_id), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.get(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!("KR GET failed with status {}: {}", status, body));
        }

        match response.json::<T>().await {
            Ok(value) => RetryDecision::Success(value),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

pub async fn relay_post_message(
    relay_url: &str,
    conversation_id: &str,
    sender: &str,
    recipients: &[String],
    bytes: Vec<u8>,
) -> Result<()> {
    let url = format!(
        "{}/conversation/{}/message/{}",
        relay_url.trim_end_matches('/'),
        conversation_id,
        sender
    );

    let recipients_header = recipients.join(",");
    let http = control_http_client();

    retry_transient_http_async("relay.publish_message", Some(sender), &url, || {
        let recipients_header = recipients_header.clone();
        let request_bytes = bytes.clone();
        async {
            let _permit = acquire_http_permit().await;
            let response = match http
                .post(&url)
                .header("x-recipients", recipients_header)
                .body(request_bytes)
                .send()
                .await
            {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if status.is_success() {
                return RetryDecision::Success(());
            }

            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            RetryDecision::Fatal(anyhow!(
                "Relay POST failed with status {}: {}",
                status,
                body
            ))
        }
    })
    .await
}

pub async fn relay_get_message(relay_url: &str, recipient: &str) -> Result<Vec<u8>> {
    let url = format!("{}/message/{}", relay_url.trim_end_matches('/'), recipient);
    let http = control_http_client();

    retry_transient_http_async("relay.fetch_message", Some(recipient), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.get(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response).await;

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!(
                "Relay GET failed with status {}: {}",
                status,
                body
            ));
        }

        match response.bytes().await {
            Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

#[derive(Debug, Clone, Deserialize)]
struct PendingMessageResponse {
    id: String,
    conversation_id: String,
    sender: String,
    message_hex: String,
}

async fn relay_get_pending_message(
    relay_url: &str,
    recipient: &str,
) -> Result<PendingMessageResponse> {
    let url = format!(
        "{}/message/{}/pending",
        relay_url.trim_end_matches('/'),
        recipient
    );
    let http = control_http_client();

    retry_transient_http_async(
        "relay.fetch_pending_message",
        Some(recipient),
        &url,
        || async {
            let _permit = acquire_http_permit().await;
            let response = match http.get(&url).send().await {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if !status.is_success() {
                let body = read_response_text(response).await;

                if is_transient_status(status) {
                    return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
                }

                return RetryDecision::Fatal(anyhow!(
                    "Relay pending message GET failed with status {}: {}",
                    status,
                    body
                ));
            }

            match response.json::<PendingMessageResponse>().await {
                Ok(message) => RetryDecision::Success(message),
                Err(err) => transient_or_fatal(err),
            }
        },
    )
    .await
}

async fn relay_ack_message(relay_url: &str, recipient: &str, message_id: &str) -> Result<()> {
    let url = format!(
        "{}/message/{}/ack/{}",
        relay_url.trim_end_matches('/'),
        recipient,
        message_id
    );
    let http = control_http_client();

    retry_transient_http_async("relay.ack_message", Some(recipient), &url, || async {
        let _permit = acquire_http_permit().await;
        let response = match http.post(&url).send().await {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let body = read_response_text(response).await;

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!(
            "Relay ack message failed with status {}: {}",
            status,
            body
        ))
    })
    .await
}

fn looks_like_duplicate_receive(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("replay")
        || lower.contains("duplicate")
        || lower.contains("already")
        || lower.contains("generation")
        || lower.contains("out of order")
        || lower.contains("stale")
}

fn ciphertext_from_bytes(bytes: &[u8]) -> Result<libsignal_protocol::CiphertextMessage> {
    if let Ok(msg) = PreKeySignalMessage::try_from(bytes) {
        return Ok(libsignal_protocol::CiphertextMessage::PreKeySignalMessage(
            msg,
        ));
    }
    if let Ok(msg) = SignalMessage::try_from(bytes) {
        return Ok(libsignal_protocol::CiphertextMessage::SignalMessage(msg));
    }
    Err(anyhow!("unrecognized ciphertext message type"))
}

async fn receive_message_delivery(
    participant: &mut SignalParticipant,
    relay_url: &str,
    _profile: bool,
    conversation_size: usize,
) -> Result<CommandOutcome> {
    let delivery = relay_get_pending_message(relay_url, &participant.name).await?;
    let message_bytes = hex::decode(&delivery.message_hex).with_context(|| {
        format!(
            "decode pending message id={} conversation={} sender={}",
            delivery.id, delivery.conversation_id, delivery.sender
        )
    })?;

    let sender_address = libsignal_core::ProtocolAddress::new(
        delivery.sender.clone(),
        DeviceId::new(1).expect("valid device id"),
    );
    let ciphertext = match ciphertext_from_bytes(message_bytes.as_slice()) {
        Ok(ct) => ct,
        Err(_) => {
            relay_ack_message(relay_url, &participant.name, &delivery.id).await?;
            return Ok(CommandOutcome::new(
                format!(
                    "message already processed (deserialize failed): message_id={} conversation={} sender={}",
                    delivery.id, delivery.conversation_id, delivery.sender
                ),
                CommandMetrics {
                    conversation_size: Some(conversation_size),
                    ciphertext_bytes: Some(message_bytes.len()),
                    ..Default::default()
                },
            ));
        }
    };

    match participant.decrypt_message(&sender_address, &ciphertext) {
        Ok(plaintext) => {
            relay_ack_message(relay_url, &participant.name, &delivery.id).await?;
            let text = String::from_utf8_lossy(&plaintext).to_string();
            let plaintext_len = plaintext.len();
            Ok(CommandOutcome::new(
                format!(
                    "pairwise message received: {}; message_id={} conversation={} sender={}",
                    text, delivery.id, delivery.conversation_id, delivery.sender
                ),
                CommandMetrics {
                    artifact_size_bytes: Some(message_bytes.len()),
                    conversation_size: Some(conversation_size),
                    session_count: Some(1),
                    ratchet_step_count: Some(1),
                    ciphertext_bytes: Some(message_bytes.len()),
                    plaintext_bytes: Some(plaintext_len),
                    ..Default::default()
                },
            ))
        }
        Err(err) => {
            let text = format!("{:#}", err);
            if looks_like_duplicate_receive(&text) {
                relay_ack_message(relay_url, &participant.name, &delivery.id).await?;
                return Ok(CommandOutcome::new(
                    format!(
                        "pairwise message already processed: message_id={} conversation={} sender={}",
                        delivery.id, delivery.conversation_id, delivery.sender
                    ),
                    CommandMetrics {
                        conversation_size: Some(conversation_size),
                        ciphertext_bytes: Some(message_bytes.len()),
                        ..Default::default()
                    },
                ));
            }

            Err(anyhow!(text))
        }
    }
}

async fn process_pending(
    participant: &mut SignalParticipant,
    relay_url: &str,
    max_messages: Option<usize>,
) -> Result<CommandOutcome> {
    let max_messages = max_messages.unwrap_or(usize::MAX);
    let mut remaining = max_messages;

    let mut messages_processed = 0usize;
    let mut metrics = CommandMetrics::default();
    let mut errors = Vec::new();

    while remaining > 0 {
        match receive_message_delivery(participant, relay_url, false, 2).await {
            Ok(outcome) => {
                messages_processed += 1;
                metrics.merge_message(&outcome.metrics);
                remaining = remaining.saturating_sub(1);
            }
            Err(err) => {
                let text = format!("{:#}", err);
                if text.contains("404 Not Found") {
                    break;
                }
                errors.push(format!("message error={}", text));
                break;
            }
        }
    }

    if errors.is_empty() {
        Ok(CommandOutcome::new(
            format!(
                "process_pending processed; messages_processed={} errors=[]",
                messages_processed,
            ),
            metrics,
        ))
    } else {
        Err(anyhow!(
            "process_pending errors; messages_processed={} errors={:?}",
            messages_processed,
            errors
        ))
    }
}

pub async fn handle_command(
    participant: &mut SignalParticipant,
    kr_url: &str,
    relay_url: &str,
    command: Command,
) -> Result<CommandOutcome> {
    match command {
        Command::RegisterParticipant => Ok(CommandOutcome::new(
            format!("participant {} registered", participant.name),
            CommandMetrics {
                participant_count: Some(1),
                ..Default::default()
            },
        )),

        Command::GeneratePrekeyBundle => {
            let bundles = participant.generate_prekey_bundles()?;
            let first = bundles
                .first()
                .ok_or_else(|| anyhow!("no prekey bundles generated"))?;
            let mut one_time_prekeys = Vec::new();
            let mut signed_prekey_fallback = false;
            for bundle in &bundles {
                match (bundle.pre_key_id()?, bundle.pre_key_public()?) {
                    (Some(prekey_id), Some(prekey_public)) => {
                        one_time_prekeys.push(OneTimePrekeyStorable {
                            prekey_id: prekey_id.into(),
                            prekey_public: prekey_public.serialize().to_vec(),
                        });
                    }
                    (None, None) => signed_prekey_fallback = true,
                    (Some(id), None) => {
                        return Err(anyhow!("prekey_id {} was present without public key", id));
                    }
                    (None, Some(_)) => {
                        return Err(anyhow!("prekey public key was present without id"));
                    }
                }
            }
            let batch = PrekeyBundleBatchStorable {
                registration_id: first.registration_id()?,
                device_id: first.device_id()?.into(),
                signed_prekey_id: first.signed_pre_key_id()?.into(),
                signed_prekey_public: first.signed_pre_key_public()?.serialize().to_vec(),
                signed_prekey_signature: first.signed_pre_key_signature()?.to_vec(),
                identity_key_public: first.identity_key()?.public_key().serialize().to_vec(),
                kyber_prekey_id: first.kyber_pre_key_id()?.into(),
                kyber_prekey_public: first.kyber_pre_key_public()?.serialize().to_vec(),
                kyber_prekey_signature: first.kyber_pre_key_signature()?.to_vec(),
                one_time_prekeys,
                signed_prekey_fallback,
            };
            let artifact_size_bytes = serde_json::to_vec(&batch)?.len();
            let bundle_count =
                batch.one_time_prekeys.len() + usize::from(batch.signed_prekey_fallback);
            let path = format!("/prekey-bundles/{}", participant.name);
            kr_put_json(
                kr_url,
                &path,
                &batch,
                "store_prekey_bundles",
                &participant.name,
            )
            .await?;
            Ok(CommandOutcome::new(
                format!(
                    "prekey bundle generated and published for {}; bundles={}",
                    participant.name, bundle_count
                ),
                CommandMetrics {
                    artifact_size_bytes: Some(artifact_size_bytes),
                    prekey_bundle_count: Some(bundle_count),
                    participant_count: Some(1),
                    ..Default::default()
                },
            ))
        }

        Command::PublishPrekeyBundle => {
            participant.store_own_prekeys()?;
            Ok(CommandOutcome::new(
                format!("prekeys stored locally for {}", participant.name),
                CommandMetrics {
                    prekey_bundle_count: Some(participant.remaining_prekeys() + 1),
                    participant_count: Some(1),
                    ..Default::default()
                },
            ))
        }

        Command::EstablishSessions { participants } => {
            let mut established = 0usize;
            let mut existing = 0usize;
            let mut fetched = 0usize;

            for peer in &participants {
                let peer_address = libsignal_core::ProtocolAddress::new(
                    peer.clone(),
                    DeviceId::new(1).expect("valid device id"),
                );

                if participant.has_session_with(&peer_address) {
                    existing += 1;
                    continue;
                }

                let path = format!("/prekey-bundle/{peer}");
                let bundle_storable: PrekeyBundleStorable =
                    kr_get_json(kr_url, &path, "fetch_prekey_bundle", &participant.name).await?;
                fetched += 1;

                let prekey_public = match (
                    bundle_storable.prekey_id,
                    bundle_storable.prekey_public.as_ref(),
                ) {
                    (Some(_), Some(bytes)) => Some(
                        libsignal_core::curve::PublicKey::deserialize(bytes)
                            .map_err(|e| anyhow!("invalid prekey public: {}", e))?,
                    ),
                    (Some(id), None) => {
                        return Err(anyhow!(
                            "prekey_id {} was present without prekey_public",
                            id
                        ));
                    }
                    (None, Some(_)) => {
                        return Err(anyhow!("prekey_public was present without prekey_id"));
                    }
                    (None, None) => None,
                };

                let identity_key = libsignal_protocol::IdentityKey::new(
                    libsignal_core::curve::PublicKey::deserialize(
                        &bundle_storable.identity_key_public,
                    )
                    .map_err(|e| anyhow!("invalid identity key: {}", e))?,
                );

                let kyber_prekey_public =
                    kem::PublicKey::deserialize(&bundle_storable.kyber_prekey_public)
                        .map_err(|e| anyhow!("invalid kyber prekey: {}", e))?;

                let bundle = libsignal_protocol::PreKeyBundle::new(
                    bundle_storable.registration_id,
                    DeviceId::new(
                        bundle_storable
                            .device_id
                            .try_into()
                            .map_err(|_| anyhow!("invalid device_id"))?,
                    )
                    .map_err(|_| anyhow!("invalid device_id value"))?,
                    bundle_storable.prekey_id.map(|id| {
                        (
                            PreKeyId::from(id),
                            prekey_public.expect("validated prekey_public"),
                        )
                    }),
                    SignedPreKeyId::from(bundle_storable.signed_prekey_id),
                    libsignal_core::curve::PublicKey::deserialize(
                        &bundle_storable.signed_prekey_public,
                    )
                    .map_err(|e| anyhow!("invalid signed prekey: {}", e))?,
                    bundle_storable.signed_prekey_signature,
                    KyberPreKeyId::from(bundle_storable.kyber_prekey_id),
                    kyber_prekey_public,
                    bundle_storable.kyber_prekey_signature,
                    identity_key,
                )?;

                participant.establish_session_from_bundle(&peer_address, &bundle)?;
                let consume_path = format!("/prekey-bundle/{}/consume", peer);
                kr_post_empty(kr_url, &consume_path, "consume_prekey", &participant.name)
                    .await
                    .ok();
                established += 1;
            }

            Ok(CommandOutcome::new(
                format!(
                    "session establishment: new={} existing={} total_target={}",
                    established,
                    existing,
                    participants.len()
                ),
                CommandMetrics {
                    participant_count: Some(participants.len().saturating_add(1)),
                    conversation_size: Some(participants.len().saturating_add(1)),
                    prekey_bundle_count: Some(fetched),
                    session_count: Some(established.saturating_add(existing)),
                    ratchet_step_count: Some(established),
                    ..Default::default()
                },
            ))
        }

        Command::EncryptMessage {
            recipient,
            message,
            conversation_size,
        } => {
            let conversation_size = conversation_size.unwrap_or(2);
            let plaintext = message.into_bytes();
            let plaintext_bytes = plaintext.len();
            let recipient_address = libsignal_core::ProtocolAddress::new(
                recipient.clone(),
                DeviceId::new(1).expect("valid device id"),
            );

            let ciphertext = participant.encrypt_message(&recipient_address, &plaintext)?;
            let ciphertext_bytes = ciphertext.serialize().to_vec();
            let ciphertext_len = ciphertext_bytes.len();

            let conversation_id = format!("conversation-{}", participant.name);
            relay_post_message(
                relay_url,
                &conversation_id,
                &participant.name,
                &[recipient.clone()],
                ciphertext_bytes,
            )
            .await?;

            Ok(CommandOutcome::new(
                format!("pairwise message encrypted and sent to {}", recipient),
                CommandMetrics {
                    artifact_size_bytes: Some(ciphertext_len),
                    conversation_size: Some(conversation_size),
                    session_count: Some(1),
                    ratchet_step_count: Some(1),
                    ciphertext_bytes: Some(ciphertext_len),
                    plaintext_bytes: Some(plaintext_bytes),
                    ..Default::default()
                },
            ))
        }

        Command::DecryptMessage {
            sender: _,
            profile: _,
            conversation_size,
        } => {
            receive_message_delivery(
                participant,
                relay_url,
                false,
                conversation_size.unwrap_or(2),
            )
            .await
        }

        Command::ProcessPending { max_messages } => {
            process_pending(participant, relay_url, max_messages).await
        }

        Command::ShowParticipantState => Ok(CommandOutcome::message(format!(
            "participant={} remaining_prekeys={} address={:?}",
            participant.name,
            participant.remaining_prekeys(),
            participant.address,
        ))),

        Command::RemoveParticipants { participants } => Ok(CommandOutcome::new(
            format!(
                "participants {:?} deactivated; local sessions retained",
                participants
            ),
            CommandMetrics {
                participant_count: Some(participants.len()),
                ..Default::default()
            },
        )),
    }
}
