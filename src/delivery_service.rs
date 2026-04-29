use std::collections::{HashMap, VecDeque};

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
    key_packages: HashMap<String, Vec<u8>>,
    commit_inboxes: HashMap<String, VecDeque<Vec<u8>>>,
    welcome_inboxes: HashMap<String, VecDeque<Vec<u8>>>,
    ratchet_tree_inboxes: HashMap<String, VecDeque<Vec<u8>>>,
    groups: HashMap<String, GroupState>,
}

impl DeliveryService {
    pub fn new() -> Self {
        Self {
            key_packages: HashMap::new(),
            commit_inboxes: HashMap::new(),
            welcome_inboxes: HashMap::new(),
            ratchet_tree_inboxes: HashMap::new(),
            groups: HashMap::new(),
        }
    }

    pub fn publish_key_package(&mut self, owner: &str, key_package_bytes: Vec<u8>) {
        self.key_packages
            .insert(owner.to_string(), key_package_bytes);
    }

    pub fn fetch_key_package(&mut self, owner: &str) -> Option<Vec<u8>> {
        self.key_packages.remove(owner)
    }

    pub fn put_group_state(
        &mut self,
        group_id: &str,
        epoch: u64,
        members: Vec<String>,
    ) -> Result<(), String> {
        match self.groups.get_mut(group_id) {
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
                self.groups.insert(
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
        self.groups.get(group_id).map(|state| GroupInfo {
            current_epoch: state.current_epoch,
            members: state.members.clone(),
        })
    }

    pub fn publish_group_commit(
        &mut self,
        group_id: &str,
        sender: &str,
        epoch: u64,
        commit_bytes: Vec<u8>,
    ) -> Result<(), String> {
        let state = self
            .groups
            .get_mut(group_id)
            .ok_or_else(|| format!("Unknown group_id '{}'", group_id))?;

        if epoch != state.current_epoch {
            return Err(format!(
                "Commit epoch mismatch for group '{}': expected {}, got {}",
                group_id, state.current_epoch, epoch
            ));
        }

        let recipients = state.members.clone();

        for recipient in recipients {
            self.commit_inboxes
                .entry(recipient)
                .or_default()
                .push_back(commit_bytes.clone());
        }

        state.current_epoch += 1;

        println!(
            "[DS] Accepted commit for group={} epoch={} from sender={}",
            group_id, epoch, sender
        );

        Ok(())
    }

    pub fn fetch_commit(&mut self, recipient: &str) -> Option<Vec<u8>> {
        self.commit_inboxes
            .get_mut(recipient)
            .and_then(|queue| queue.pop_front())
    }

    pub fn publish_welcome(&mut self, recipient: &str, welcome_bytes: Vec<u8>) {
        self.welcome_inboxes
            .entry(recipient.to_string())
            .or_default()
            .push_back(welcome_bytes);
    }

    pub fn fetch_welcome(&mut self, recipient: &str) -> Option<Vec<u8>> {
        self.welcome_inboxes
            .get_mut(recipient)
            .and_then(|queue| queue.pop_front())
    }

    pub fn publish_ratchet_tree(&mut self, recipient: &str, ratchet_tree_bytes: Vec<u8>) {
        self.ratchet_tree_inboxes
            .entry(recipient.to_string())
            .or_default()
            .push_back(ratchet_tree_bytes);
    }

    pub fn fetch_ratchet_tree(&mut self, recipient: &str) -> Option<Vec<u8>> {
        self.ratchet_tree_inboxes
            .get_mut(recipient)
            .and_then(|queue| queue.pop_front())
    }
}
