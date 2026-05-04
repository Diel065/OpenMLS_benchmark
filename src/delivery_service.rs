use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use crate::debug::debug_logs_enabled;
use crate::service_metrics::ServiceMetrics;

type MessageBytes = Arc<Vec<u8>>;
type InboxQueue = Arc<Mutex<VecDeque<MessageBytes>>>;

#[derive(Clone, Debug)]
pub struct GroupInfo {
    pub current_epoch: u64,
    pub members: Vec<String>,
}

#[derive(Clone, Debug)]
struct GroupState {
    current_epoch: u64,
    members: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitDeliveryMode {
    PerRecipient,
    GroupLog,
}

#[derive(Clone, Debug)]
struct GroupLogCommit {
    epoch: u64,
    sender: String,
    recipients: Vec<String>,
    commit_bytes: MessageBytes,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingCommit {
    pub group_id: String,
    pub epoch: u64,
    pub sender: String,
    pub commit_hex: String,
}

pub struct DeliveryService {
    key_packages: Mutex<HashMap<String, Vec<u8>>>,
    commit_inboxes: RwLock<HashMap<String, InboxQueue>>,
    commit_cursors: Mutex<HashMap<(String, String), u64>>,
    group_commit_log: RwLock<HashMap<String, Vec<GroupLogCommit>>>,
    welcome_inboxes: RwLock<HashMap<String, InboxQueue>>,
    ratchet_tree_inboxes: RwLock<HashMap<String, InboxQueue>>,
    groups: RwLock<HashMap<String, GroupState>>,
    delivery_mode: CommitDeliveryMode,
    metrics: ServiceMetrics,
}

impl DeliveryService {
    pub fn new() -> Self {
        let delivery_mode = match std::env::var("OPENMLS_DS_DELIVERY_MODE")
            .unwrap_or_else(|_| "per-recipient".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "group-log" | "group_log" | "grouplog" => CommitDeliveryMode::GroupLog,
            _ => CommitDeliveryMode::PerRecipient,
        };

        Self {
            key_packages: Mutex::new(HashMap::new()),
            commit_inboxes: RwLock::new(HashMap::new()),
            commit_cursors: Mutex::new(HashMap::new()),
            group_commit_log: RwLock::new(HashMap::new()),
            welcome_inboxes: RwLock::new(HashMap::new()),
            ratchet_tree_inboxes: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
            delivery_mode,
            metrics: ServiceMetrics::new(),
        }
    }

    pub fn metrics(&self) -> &ServiceMetrics {
        &self.metrics
    }

    pub fn publish_key_package(&self, owner: &str, key_package_bytes: Vec<u8>) {
        self.key_packages
            .lock()
            .unwrap()
            .insert(owner.to_string(), key_package_bytes);
    }

    pub fn fetch_key_package(&self, owner: &str) -> Option<Vec<u8>> {
        self.key_packages.lock().unwrap().remove(owner)
    }

    pub fn put_group_state(
        &self,
        group_id: &str,
        epoch: u64,
        members: Vec<String>,
    ) -> Result<(), String> {
        let mut groups = self.groups.write().unwrap();

        match groups.get_mut(group_id) {
            Some(state) => {
                if epoch != state.current_epoch {
                    return Err(format!(
                        "Group state update mismatch for group '{}': expected epoch {}, got {}",
                        group_id, state.current_epoch, epoch
                    ));
                }

                state.members = members;
                Ok(())
            }
            None => {
                groups.insert(
                    group_id.to_string(),
                    GroupState {
                        current_epoch: epoch,
                        members,
                    },
                );
                Ok(())
            }
        }
    }

    pub fn get_group_state(&self, group_id: &str) -> Option<GroupInfo> {
        self.groups
            .read()
            .unwrap()
            .get(group_id)
            .map(|state| GroupInfo {
                current_epoch: state.current_epoch,
                members: state.members.clone(),
            })
    }

    pub fn publish_group_commit(
        &self,
        group_id: &str,
        sender: &str,
        epoch: u64,
        commit_bytes: Vec<u8>,
    ) -> Result<(), String> {
        let recipients = {
            let mut groups = self.groups.write().unwrap();
            let state = groups
                .get_mut(group_id)
                .ok_or_else(|| format!("Unknown group_id '{}'", group_id))?;

            if epoch != state.current_epoch {
                return Err(format!(
                    "Commit epoch mismatch for group '{}': expected {}, got {}",
                    group_id, state.current_epoch, epoch
                ));
            }

            let recipients = state.members.clone();
            state.current_epoch += 1;
            recipients
        };

        let shared_commit = Arc::new(commit_bytes);

        match self.delivery_mode {
            CommitDeliveryMode::PerRecipient => {
                for recipient in recipients {
                    self.inbox_queue(&self.commit_inboxes, &recipient)
                        .lock()
                        .unwrap()
                        .push_back(Arc::clone(&shared_commit));
                }
            }
            CommitDeliveryMode::GroupLog => {
                self.group_commit_log
                    .write()
                    .unwrap()
                    .entry(group_id.to_string())
                    .or_default()
                    .push(GroupLogCommit {
                        epoch: epoch + 1,
                        sender: sender.to_string(),
                        recipients,
                        commit_bytes: Arc::clone(&shared_commit),
                    });
            }
        }

        if debug_logs_enabled() {
            println!(
                "[DS] Accepted commit for group={} epoch={} from sender={}",
                group_id, epoch, sender
            );
        }

        Ok(())
    }

    pub fn fetch_commit(&self, recipient: &str) -> Option<Vec<u8>> {
        match self.delivery_mode {
            CommitDeliveryMode::PerRecipient => self.pop_inbox(&self.commit_inboxes, recipient),
            CommitDeliveryMode::GroupLog => self.fetch_group_log_commit_compat(recipient),
        }
    }

    pub fn fetch_pending_commits(&self, client: &str, after_epoch: u64) -> Vec<PendingCommit> {
        let groups = self.group_commit_log.read().unwrap();
        let mut commits = Vec::new();

        for (group_id, group_commits) in groups.iter() {
            for commit in group_commits {
                if commit.epoch <= after_epoch || !commit.recipients.iter().any(|id| id == client) {
                    continue;
                }

                commits.push(PendingCommit {
                    group_id: group_id.clone(),
                    epoch: commit.epoch,
                    sender: commit.sender.clone(),
                    commit_hex: hex::encode(commit.commit_bytes.as_ref()),
                });
            }
        }

        commits.sort_by(|left, right| {
            left.epoch
                .cmp(&right.epoch)
                .then_with(|| left.group_id.cmp(&right.group_id))
        });
        commits
    }

    pub fn publish_welcome(&self, recipient: &str, welcome_bytes: Vec<u8>) {
        self.inbox_queue(&self.welcome_inboxes, recipient)
            .lock()
            .unwrap()
            .push_back(Arc::new(welcome_bytes));
    }

    pub fn fetch_welcome(&self, recipient: &str) -> Option<Vec<u8>> {
        self.pop_inbox(&self.welcome_inboxes, recipient)
    }

    pub fn publish_ratchet_tree(&self, recipient: &str, ratchet_tree_bytes: Vec<u8>) {
        self.inbox_queue(&self.ratchet_tree_inboxes, recipient)
            .lock()
            .unwrap()
            .push_back(Arc::new(ratchet_tree_bytes));
    }

    pub fn fetch_ratchet_tree(&self, recipient: &str) -> Option<Vec<u8>> {
        self.pop_inbox(&self.ratchet_tree_inboxes, recipient)
    }

    fn inbox_queue(
        &self,
        inboxes: &RwLock<HashMap<String, InboxQueue>>,
        recipient: &str,
    ) -> InboxQueue {
        if let Some(queue) = inboxes.read().unwrap().get(recipient).cloned() {
            return queue;
        }

        let mut inboxes = inboxes.write().unwrap();
        inboxes
            .entry(recipient.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }

    fn pop_inbox(
        &self,
        inboxes: &RwLock<HashMap<String, InboxQueue>>,
        recipient: &str,
    ) -> Option<Vec<u8>> {
        let queue = inboxes.read().unwrap().get(recipient).cloned()?;
        let bytes = queue
            .lock()
            .unwrap()
            .pop_front()
            .map(|bytes| bytes.as_ref().clone());
        bytes
    }

    fn fetch_group_log_commit_compat(&self, recipient: &str) -> Option<Vec<u8>> {
        let groups = self.group_commit_log.read().unwrap();
        let mut cursors = self.commit_cursors.lock().unwrap();
        let mut selected: Option<(String, u64, MessageBytes)> = None;

        for (group_id, commits) in groups.iter() {
            let cursor_key = (recipient.to_string(), group_id.clone());
            let after_epoch = cursors.get(&cursor_key).copied().unwrap_or(0);

            for commit in commits {
                if commit.epoch <= after_epoch
                    || !commit.recipients.iter().any(|id| id == recipient)
                {
                    continue;
                }

                let should_replace = selected
                    .as_ref()
                    .map(|(_, selected_epoch, _)| commit.epoch < *selected_epoch)
                    .unwrap_or(true);

                if should_replace {
                    selected = Some((
                        group_id.clone(),
                        commit.epoch,
                        Arc::clone(&commit.commit_bytes),
                    ));
                }
                break;
            }
        }

        let (group_id, epoch, bytes) = selected?;
        cursors.insert((recipient.to_string(), group_id), epoch);
        Some(bytes.as_ref().clone())
    }
}
