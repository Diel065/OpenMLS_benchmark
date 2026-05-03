use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use crate::debug::debug_logs_enabled;

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

pub struct DeliveryService {
    key_packages: Mutex<HashMap<String, Vec<u8>>>,
    commit_inboxes: RwLock<HashMap<String, InboxQueue>>,
    welcome_inboxes: RwLock<HashMap<String, InboxQueue>>,
    ratchet_tree_inboxes: RwLock<HashMap<String, InboxQueue>>,
    groups: RwLock<HashMap<String, GroupState>>,
}

impl DeliveryService {
    pub fn new() -> Self {
        Self {
            key_packages: Mutex::new(HashMap::new()),
            commit_inboxes: RwLock::new(HashMap::new()),
            welcome_inboxes: RwLock::new(HashMap::new()),
            ratchet_tree_inboxes: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),
        }
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
        for recipient in recipients {
            self.inbox_queue(&self.commit_inboxes, &recipient)
                .lock()
                .unwrap()
                .push_back(Arc::clone(&shared_commit));
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
        self.pop_inbox(&self.commit_inboxes, recipient)
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
}
