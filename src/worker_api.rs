use std::time::Duration;

use anyhow::{anyhow, Result};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::client::{Client, CommitReceiveOutcome, EpochChangeOutput};
use crate::http_retry::{
    is_transient_reqwest_error, is_transient_status, retry_transient_http, RetryDecision,
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

pub enum DsPostResult {
    Ok,
    Conflict(String),
}

fn control_http_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|err| anyhow!("Failed to build HTTP client: {}", err))
}

fn transient_or_fatal<T>(err: reqwest::Error) -> RetryDecision<T> {
    if is_transient_reqwest_error(&err) {
        RetryDecision::Transient(err.to_string())
    } else {
        RetryDecision::Fatal(anyhow!(err))
    }
}

fn read_response_text(response: reqwest::blocking::Response) -> String {
    response.text().unwrap_or_default()
}

pub fn ds_post_bytes_allow_conflict(
    ds_url: &str,
    path: &str,
    bytes: Vec<u8>,
    op: &str,
    client_id: &str,
) -> Result<DsPostResult> {
    let url = format!("{ds_url}{path}");
    let http = control_http_client()?;

    retry_transient_http(op, Some(client_id), &url, || {
        let response = match http.post(&url).body(bytes.clone()).send() {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(DsPostResult::Ok);
        }

        let body = read_response_text(response);

        if status == StatusCode::CONFLICT {
            return RetryDecision::Success(DsPostResult::Conflict(body));
        }

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!("DS POST failed with status {}: {}", status, body))
    })
}

pub fn ds_post_bytes(
    ds_url: &str,
    path: &str,
    bytes: Vec<u8>,
    op: &str,
    client_id: &str,
) -> Result<()> {
    match ds_post_bytes_allow_conflict(ds_url, path, bytes, op, client_id)? {
        DsPostResult::Ok => Ok(()),
        DsPostResult::Conflict(message) => Err(anyhow!("Unexpected DS conflict: {}", message)),
    }
}

pub fn ds_put_json<T: Serialize>(
    ds_url: &str,
    path: &str,
    body: &T,
    op: &str,
    client_id: &str,
) -> Result<()> {
    let url = format!("{ds_url}{path}");
    let http = control_http_client()?;

    retry_transient_http(op, Some(client_id), &url, || {
        let response = match http.put(&url).json(body).send() {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let response_body = read_response_text(response);

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, response_body));
        }

        RetryDecision::Fatal(anyhow!(
            "DS PUT failed with status {}: {}",
            status,
            response_body
        ))
    })
}

pub fn ds_get_bytes(ds_url: &str, path: &str, op: &str, client_id: &str) -> Result<Vec<u8>> {
    let url = format!("{ds_url}{path}");
    let http = control_http_client()?;

    retry_transient_http(op, Some(client_id), &url, || {
        let response = match http.get(&url).send() {
            Ok(response) => response,
            Err(err) => return transient_or_fatal(err),
        };

        let status = response.status();

        if !status.is_success() {
            let body = read_response_text(response);

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            return RetryDecision::Fatal(anyhow!("DS GET failed with status {}: {}", status, body));
        }

        match response.bytes() {
            Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
            Err(err) => transient_or_fatal(err),
        }
    })
}

pub fn relay_post_application_message(
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
    let http = control_http_client()?;

    retry_transient_http(
        "relay.publish_application_message",
        Some(sender),
        &url,
        || {
            let response = match http
                .post(&url)
                .header("x-recipients", recipients_header.clone())
                .body(bytes.clone())
                .send()
            {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if status.is_success() {
                return RetryDecision::Success(());
            }

            let body = read_response_text(response);

            if is_transient_status(status) {
                return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
            }

            RetryDecision::Fatal(anyhow!(
                "Relay POST failed with status {}: {}",
                status,
                body
            ))
        },
    )
}

pub fn relay_get_application_message(relay_url: &str, recipient: &str) -> Result<Vec<u8>> {
    let url = format!(
        "{}/application-message/{}",
        relay_url.trim_end_matches('/'),
        recipient
    );
    let http = control_http_client()?;

    retry_transient_http(
        "relay.fetch_application_message",
        Some(recipient),
        &url,
        || {
            let response = match http.get(&url).send() {
                Ok(response) => response,
                Err(err) => return transient_or_fatal(err),
            };

            let status = response.status();

            if !status.is_success() {
                let body = read_response_text(response);

                if is_transient_status(status) {
                    return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
                }

                return RetryDecision::Fatal(anyhow!(
                    "Relay GET failed with status {}: {}",
                    status,
                    body
                ));
            }

            match response.bytes() {
                Ok(bytes) => RetryDecision::Success(bytes.to_vec()),
                Err(err) => transient_or_fatal(err),
            }
        },
    )
}

pub fn update_ds_group_state(client: &Client, ds_url: &str) -> Result<()> {
    let group_id = client.group_id_hex()?;
    let epoch = client.current_epoch_u64()?;
    let members = client.member_names()?;

    let path = format!("/group/{group_id}/state/{epoch}");
    let body = GroupStatePutRequest { members };

    ds_put_json(ds_url, &path, &body, "update_group_state", &client.name)
}

pub fn publish_epoch_change(
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
}

pub fn try_start_intent(
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

    match publish_epoch_change(client, ds_url, result)? {
        DsPostResult::Ok => Ok(DsPostResult::Ok),
        DsPostResult::Conflict(message) => {
            client.rollback_pending_commit()?;
            Ok(DsPostResult::Conflict(message))
        }
    }
}

pub fn maybe_retry_pending_intent(
    client: &mut Client,
    ds_url: &str,
    queued_intent: &mut Option<PendingIntent>,
) -> Result<Option<String>> {
    let Some(intent) = queued_intent.clone() else {
        return Ok(None);
    };

    match try_start_intent(client, ds_url, &intent)? {
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

pub fn handle_command(
    client: &mut Client,
    ds_url: &str,
    relay_url: &str,
    queued_intent: &mut Option<PendingIntent>,
    command: Command,
) -> Result<String> {
    match command {
        Command::CreateGroup => {
            client.create_group()?;
            update_ds_group_state(client, ds_url)?;
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
            )?;
            Ok(format!("key package uploaded for {}", client.name))
        }

        Command::AddMembers { members } => {
            let mut key_package_bytes_list = Vec::with_capacity(members.len());

            for member in &members {
                let kp_path = format!("/keypackage/{member}");
                let key_package_bytes =
                    ds_get_bytes(ds_url, &kp_path, "fetch_keypackage", &client.name)?;
                key_package_bytes_list.push(key_package_bytes);
            }

            let intent = PendingIntent::AddMembers {
                members: members.clone(),
                key_package_bytes_list,
            };

            match try_start_intent(client, ds_url, &intent)? {
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

            let welcome_bytes = ds_get_bytes(ds_url, &welcome_path, "fetch_welcome", &client.name)?;
            let ratchet_tree_bytes =
                ds_get_bytes(ds_url, &tree_path, "fetch_ratchet_tree", &client.name)?;

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
            )?;

            Ok("application message broadcast to group".to_string())
        }

        Command::ReceiveApplicationMessage { profile } => {
            let message_bytes = relay_get_application_message(relay_url, &client.name)?;
            let plaintext = client.receive_application_message(&message_bytes, profile)?;
            let text = String::from_utf8_lossy(&plaintext).to_string();
            Ok(format!("application message received: {}", text))
        }

        Command::SelfUpdate => {
            let intent = PendingIntent::SelfUpdate;

            match try_start_intent(client, ds_url, &intent)? {
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

            match try_start_intent(client, ds_url, &intent)? {
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
            let commit_bytes = ds_get_bytes(ds_url, &path, "fetch_commit", &client.name)?;

            match client.receive_commit(&commit_bytes)? {
                CommitReceiveOutcome::ExternalCommitApplied { self_removed } => {
                    if self_removed {
                        *queued_intent = None;
                        Ok("external commit received and processed; this client was removed and local group state was cleared".to_string())
                    } else {
                        update_ds_group_state(client, ds_url)?;

                        let retry_message =
                            maybe_retry_pending_intent(client, ds_url, queued_intent)?;

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
                            for recipient in &welcome_recipients {
                                let welcome_path = format!("/welcome/{recipient}");
                                ds_post_bytes(
                                    ds_url,
                                    &welcome_path,
                                    welcome.clone(),
                                    "store_welcome",
                                    &client.name,
                                )?;

                                let tree_path = format!("/ratchet-tree/{recipient}");
                                ds_post_bytes(
                                    ds_url,
                                    &tree_path,
                                    tree.clone(),
                                    "store_ratchet_tree",
                                    &client.name,
                                )?;
                            }
                        }

                        update_ds_group_state(client, ds_url)?;
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
