use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use futures_util::future::try_join_all;
use once_cell::sync::Lazy;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::client::{Client, CommitReceiveOutcome, EpochChangeOutput};
use crate::http_retry::{
    is_transient_reqwest_error, is_transient_status, retry_transient_http_async, RetryDecision,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    CreateGroup,
    GenerateKeyPackage,
    AddMembers { members: Vec<String> },
    JoinFromWelcome,
    SendApplicationMessage { message: String },
    ReceiveApplicationMessage { profile: bool },
    SelfUpdate,
    RemoveMembers { members: Vec<String> },
    ReceiveCommit,
    ShowGroupState,
}

impl Command {
    pub fn kind(&self) -> &'static str {
        match self {
            Command::CreateGroup => "CreateGroup",
            Command::GenerateKeyPackage => "GenerateKeyPackage",
            Command::AddMembers { .. } => "AddMembers",
            Command::JoinFromWelcome => "JoinFromWelcome",
            Command::SendApplicationMessage { .. } => "SendApplicationMessage",
            Command::ReceiveApplicationMessage { .. } => "ReceiveApplicationMessage",
            Command::SelfUpdate => "SelfUpdate",
            Command::RemoveMembers { .. } => "RemoveMembers",
            Command::ReceiveCommit => "ReceiveCommit",
            Command::ShowGroupState => "ShowGroupState",
        }
    }

    pub fn is_mls_mutating(&self) -> bool {
        matches!(
            self,
            Command::CreateGroup
                | Command::AddMembers { .. }
                | Command::JoinFromWelcome
                | Command::SelfUpdate
                | Command::RemoveMembers { .. }
                | Command::ReceiveCommit
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRequestEnvelope {
    pub request_id: String,
    pub command: Command,
    #[serde(default)]
    pub expected_epoch: Option<u64>,
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
    pub fn into_parts(self) -> (Option<String>, Command, Option<u64>, Option<String>) {
        match self {
            IncomingCommandRequest::Envelope(envelope) => (
                Some(envelope.request_id),
                envelope.command,
                envelope.expected_epoch,
                envelope.phase,
            ),
            IncomingCommandRequest::Raw(command) => (None, command, None, None),
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

#[derive(Debug, Serialize)]
struct GroupStatePutRequest {
    members: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum PendingIntent {
    AddMembers {
        members: Vec<String>,
        key_package_bytes_list: Vec<Vec<u8>>,
    },
    RemoveMembers {
        members: Vec<String>,
    },
    SelfUpdate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_command_cache_replays_same_request_once() {
        let mut cache = CompletedCommandCache::new(8, Duration::from_secs(60));
        let response = CommandResponse::ok("mutating command processed");

        assert!(cache.get("req-1").is_none());

        cache.insert("req-1".to_string(), response.clone());

        let replayed = cache
            .get("req-1")
            .expect("cached response should be available");
        assert_eq!(replayed.status, response.status);
        assert_eq!(replayed.message, response.message);
    }

    #[test]
    fn completed_command_cache_is_bounded() {
        let mut cache = CompletedCommandCache::new(2, Duration::from_secs(60));

        cache.insert("req-1".to_string(), CommandResponse::ok("one"));
        cache.insert("req-2".to_string(), CommandResponse::ok("two"));
        cache.insert("req-3".to_string(), CommandResponse::ok("three"));

        assert!(cache.get("req-1").is_none());
        assert!(cache.get("req-2").is_some());
        assert!(cache.get("req-3").is_some());
    }
}

pub enum DsPostResult {
    Ok,
    Conflict(String),
}

static CONTROL_HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(control_http_pool_max_idle_per_host())
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .build()
        .expect("failed to build shared worker control HTTP client")
});

fn control_http_pool_max_idle_per_host() -> usize {
    std::env::var("OPENMLS_BENCH_HTTP_POOL_MAX_IDLE_PER_HOST")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(32)
}

fn control_http_client() -> &'static reqwest::Client {
    &CONTROL_HTTP_CLIENT
}

fn transient_or_fatal<T>(err: reqwest::Error) -> RetryDecision<T> {
    if is_transient_reqwest_error(&err) {
        RetryDecision::Transient(err.to_string())
    } else {
        RetryDecision::Fatal(anyhow!(err))
    }
}

async fn read_response_text(response: reqwest::Response) -> String {
    response.text().await.unwrap_or_default()
}

pub async fn ds_post_bytes_allow_conflict(
    ds_url: &str,
    path: &str,
    bytes: Vec<u8>,
    op: &str,
    client_id: &str,
) -> Result<DsPostResult> {
    let url = format!("{ds_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(client_id), &url, || {
        let request_bytes = bytes.clone();
        async {
            let response = match http.post(&url).body(request_bytes).send().await {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if status.is_success() {
                return RetryDecision::Success(DsPostResult::Ok);
            }

            let body = read_response_text(response).await;

            if status == StatusCode::CONFLICT {
                return RetryDecision::Success(DsPostResult::Conflict(body));
            }

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            RetryDecision::Fatal(anyhow!("DS POST failed with status {}: {}", status, body))
        }
    })
    .await
}

pub async fn ds_post_bytes(
    ds_url: &str,
    path: &str,
    bytes: Vec<u8>,
    op: &str,
    client_id: &str,
) -> Result<()> {
    match ds_post_bytes_allow_conflict(ds_url, path, bytes, op, client_id).await? {
        DsPostResult::Ok => Ok(()),
        DsPostResult::Conflict(message) => Err(anyhow!("Unexpected DS conflict: {}", message)),
    }
}

pub async fn ds_put_json<T: Serialize>(
    ds_url: &str,
    path: &str,
    body: &T,
    op: &str,
    client_id: &str,
) -> Result<()> {
    let url = format!("{ds_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(client_id), &url, || async {
        let response = match http.put(&url).json(body).send().await {
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
            "DS PUT failed with status {}: {}",
            status,
            response_body
        ))
    })
    .await
}

pub async fn ds_get_bytes(ds_url: &str, path: &str, op: &str, client_id: &str) -> Result<Vec<u8>> {
    let url = format!("{ds_url}{path}");
    let http = control_http_client();

    retry_transient_http_async(op, Some(client_id), &url, || async {
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

            return RetryDecision::Fatal(anyhow!("DS GET failed with status {}: {}", status, body));
        }

        match response.bytes().await {
            Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
            Err(err) => transient_or_fatal(err),
        }
    })
    .await
}

pub async fn relay_post_application_message(
    relay_url: &str,
    group_id: &str,
    sender: &str,
    recipients: &[String],
    bytes: Vec<u8>,
) -> Result<()> {
    let url = format!(
        "{}/group/{}/application-message/{}",
        relay_url.trim_end_matches('/'),
        group_id,
        sender
    );

    let recipients_header = recipients.join(",");
    let http = control_http_client();

    retry_transient_http_async(
        "relay.publish_application_message",
        Some(sender),
        &url,
        || {
            let recipients_header = recipients_header.clone();
            let request_bytes = bytes.clone();
            async {
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
        },
    )
    .await
}

pub async fn relay_get_application_message(relay_url: &str, recipient: &str) -> Result<Vec<u8>> {
    let url = format!(
        "{}/application-message/{}",
        relay_url.trim_end_matches('/'),
        recipient
    );
    let http = control_http_client();

    retry_transient_http_async(
        "relay.fetch_application_message",
        Some(recipient),
        &url,
        || async {
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
        },
    )
    .await
}

pub async fn update_ds_group_state(client: &Client, ds_url: &str) -> Result<()> {
    let group_id = client.group_id_hex()?;
    let epoch = client.current_epoch_u64()?;
    let members = client.member_names()?;

    let path = format!("/group/{group_id}/state/{epoch}");
    let body = GroupStatePutRequest { members };

    ds_put_json(ds_url, &path, &body, "update_group_state", &client.name).await
}

pub async fn publish_epoch_change(
    client: &mut Client,
    ds_url: &str,
    result: EpochChangeOutput,
) -> Result<DsPostResult> {
    let group_id = client.group_id_hex()?;
    let epoch = client.current_epoch_u64()?;
    let path = format!("/group/{group_id}/commit/{}/{epoch}", client.name);

    ds_post_bytes_allow_conflict(
        ds_url,
        &path,
        result.commit_bytes,
        "submit_commit",
        &client.name,
    )
    .await
}

pub async fn try_start_intent(
    client: &mut Client,
    ds_url: &str,
    intent: &PendingIntent,
) -> Result<DsPostResult> {
    let result = match intent {
        PendingIntent::AddMembers {
            members,
            key_package_bytes_list,
        } => client.add_members(key_package_bytes_list, members)?,
        PendingIntent::RemoveMembers { members } => client.remove_members(members)?,
        PendingIntent::SelfUpdate => client.self_update()?,
    };

    match publish_epoch_change(client, ds_url, result).await? {
        DsPostResult::Ok => Ok(DsPostResult::Ok),
        DsPostResult::Conflict(message) => {
            client.rollback_pending_commit()?;
            Ok(DsPostResult::Conflict(message))
        }
    }
}

pub async fn maybe_retry_pending_intent(
    client: &mut Client,
    ds_url: &str,
    queued_intent: &mut Option<PendingIntent>,
) -> Result<Option<String>> {
    let Some(intent) = queued_intent.clone() else {
        return Ok(None);
    };

    match try_start_intent(client, ds_url, &intent).await? {
        DsPostResult::Ok => {
            *queued_intent = None;

            let text = match intent {
                PendingIntent::AddMembers { members, .. } => {
                    format!(
                        "queued add_members for {:?} was retried and published",
                        members
                    )
                }
                PendingIntent::RemoveMembers { members } => {
                    format!(
                        "queued remove_members for {:?} was retried and published",
                        members
                    )
                }
                PendingIntent::SelfUpdate => {
                    "queued self_update was retried and published".to_string()
                }
            };

            Ok(Some(text))
        }
        DsPostResult::Conflict(message) => {
            *queued_intent = Some(intent);
            Ok(Some(format!(
                "queued intent retry still conflicted and remains queued: {}",
                message
            )))
        }
    }
}

pub async fn handle_command(
    client: &mut Client,
    ds_url: &str,
    relay_url: &str,
    queued_intent: &mut Option<PendingIntent>,
    command: Command,
) -> Result<String> {
    match command {
        Command::CreateGroup => {
            client.create_group()?;
            update_ds_group_state(client, ds_url).await?;
            Ok("group created and DS group state registered".to_string())
        }

        Command::GenerateKeyPackage => {
            let key_package_bytes = client.generate_key_package()?;
            let path = format!("/keypackage/{}", client.name);
            ds_post_bytes(
                ds_url,
                &path,
                key_package_bytes,
                "store_keypackage",
                &client.name,
            )
            .await?;
            Ok(format!("key package uploaded for {}", client.name))
        }

        Command::AddMembers { members } => {
            let client_id = client.name.clone();
            let key_package_fetches = members.iter().map(|member| {
                let kp_path = format!("/keypackage/{member}");
                let client_id = client_id.clone();
                async move {
                    ds_get_bytes(ds_url, &kp_path, "fetch_keypackage", &client_id)
                        .await
                        .with_context(|| format!("fetch key package for {}", member))
                }
            });
            let key_package_bytes_list = try_join_all(key_package_fetches).await?;

            let intent = PendingIntent::AddMembers {
                members: members.clone(),
                key_package_bytes_list,
            };

            match try_start_intent(client, ds_url, &intent).await? {
                DsPostResult::Ok => Ok(format!(
                    "members {:?} added locally in one commit; commit published, waiting for DS echo",
                    members
                )),
                DsPostResult::Conflict(message) => {
                    *queued_intent = Some(intent);
                    Ok(format!(
                        "add_members for {:?} lost the epoch race and was queued for retry: {}",
                        members, message
                    ))
                }
            }
        }

        Command::JoinFromWelcome => {
            let welcome_path = format!("/welcome/{}", client.name);
            let tree_path = format!("/ratchet-tree/{}", client.name);

            let welcome_bytes =
                ds_get_bytes(ds_url, &welcome_path, "fetch_welcome", &client.name).await?;
            let ratchet_tree_bytes =
                ds_get_bytes(ds_url, &tree_path, "fetch_ratchet_tree", &client.name).await?;

            client.join_from_welcome(&welcome_bytes, &ratchet_tree_bytes)?;

            Ok(format!("{} joined from welcome", client.name))
        }

        Command::SendApplicationMessage { message } => {
            let message_bytes = client.send_application_message(message.as_bytes())?;
            let group_id = client.group_id_hex()?;
            let sender = client.name.clone();

            let mut recipients = client.member_names()?;
            recipients.retain(|recipient| recipient != &sender);

            relay_post_application_message(
                relay_url,
                &group_id,
                &sender,
                &recipients,
                message_bytes,
            )
            .await?;

            Ok("application message broadcast to group".to_string())
        }

        Command::ReceiveApplicationMessage { profile } => {
            let message_bytes = relay_get_application_message(relay_url, &client.name).await?;
            let plaintext = client.receive_application_message(&message_bytes, profile)?;
            let text = String::from_utf8_lossy(&plaintext).to_string();
            Ok(format!("application message received: {}", text))
        }

        Command::SelfUpdate => {
            let intent = PendingIntent::SelfUpdate;

            match try_start_intent(client, ds_url, &intent).await? {
                DsPostResult::Ok => Ok("self_update commit published to group".to_string()),
                DsPostResult::Conflict(message) => {
                    *queued_intent = Some(intent);
                    Ok(format!(
                        "self_update lost the epoch race and was queued for retry: {}",
                        message
                    ))
                }
            }
        }

        Command::RemoveMembers { members } => {
            let intent = PendingIntent::RemoveMembers {
                members: members.clone(),
            };

            match try_start_intent(client, ds_url, &intent).await? {
                DsPostResult::Ok => Ok(format!(
                    "members {:?} removed locally; group commit published",
                    members
                )),
                DsPostResult::Conflict(message) => {
                    *queued_intent = Some(intent);
                    Ok(format!(
                        "remove_members for {:?} lost the epoch race and was queued for retry: {}",
                        members, message
                    ))
                }
            }
        }

        Command::ReceiveCommit => {
            let path = format!("/commit/{}", client.name);
            let commit_bytes = ds_get_bytes(ds_url, &path, "fetch_commit", &client.name).await?;

            match client.receive_commit(&commit_bytes)? {
                CommitReceiveOutcome::ExternalCommitApplied { self_removed } => {
                    if self_removed {
                        *queued_intent = None;
                        Ok("external commit received and processed; this client was removed and local group state was cleared".to_string())
                    } else {
                        update_ds_group_state(client, ds_url).await?;

                        let retry_message =
                            maybe_retry_pending_intent(client, ds_url, queued_intent).await?;

                        match retry_message {
                            Some(text) => Ok(format!(
                                "external commit received and processed; DS group state updated; {}",
                                text
                            )),
                            None => Ok(
                                "external commit received and processed; DS group state updated"
                                    .to_string(),
                            ),
                        }
                    }
                }

                CommitReceiveOutcome::OwnCommitAccepted {
                    self_removed,
                    welcome_recipients,
                    welcome_bytes,
                    ratchet_tree_bytes,
                } => {
                    if self_removed {
                        *queued_intent = None;
                        Ok("own commit accepted from DS; this client was removed and local group state was cleared".to_string())
                    } else {
                        if let (Some(welcome), Some(tree)) = (welcome_bytes, ratchet_tree_bytes) {
                            let client_id = client.name.clone();
                            let publish_tasks = welcome_recipients.iter().map(|recipient| {
                                let recipient = recipient.clone();
                                let welcome = welcome.clone();
                                let tree = tree.clone();
                                let client_id = client_id.clone();
                                async move {
                                    let welcome_path = format!("/welcome/{recipient}");
                                    ds_post_bytes(
                                        ds_url,
                                        &welcome_path,
                                        welcome.clone(),
                                        "store_welcome",
                                        &client_id,
                                    )
                                    .await?;

                                    let tree_path = format!("/ratchet-tree/{recipient}");
                                    ds_post_bytes(
                                        ds_url,
                                        &tree_path,
                                        tree.clone(),
                                        "store_ratchet_tree",
                                        &client_id,
                                    )
                                    .await
                                }
                            });
                            try_join_all(publish_tasks).await?;
                        }

                        update_ds_group_state(client, ds_url).await?;
                        Ok("own commit accepted from DS; local state updated and welcome/tree published"
                            .to_string())
                    }
                }
            }
        }

        Command::ShowGroupState => {
            let group_id = client.group_id_hex()?;
            let epoch = client.current_epoch_u64()?;
            let members = client.member_names()?;

            Ok(format!(
                "group_id={}, epoch={}, members={:?}",
                group_id, epoch, members
            ))
        }
    }
}
