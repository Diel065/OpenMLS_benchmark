use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};

use crate::debug::debug_logs_enabled;

#[derive(Clone, Debug)]
struct RelayEnvelope {
    group_id: String,
    sender: String,
    message_bytes: Arc<Vec<u8>>,
}

pub struct MessageRelay {
    application_inboxes: RwLock<HashMap<String, Arc<Mutex<VecDeque<RelayEnvelope>>>>>,
}

impl MessageRelay {
    pub fn new() -> Self {
        Self {
            application_inboxes: RwLock::new(HashMap::new()),
        }
    }

    pub fn publish_group_application_message(
        &self,
        group_id: &str,
        sender: &str,
        recipients: &[String],
        message_bytes: Vec<u8>,
    ) -> Result<(), String> {
        if recipients.is_empty() {
            return Err("No recipients were provided to the message relay".to_string());
        }

        let mut delivered = 0usize;
        let shared_message = Arc::new(message_bytes);

        for recipient in recipients {
            if recipient == sender {
                continue;
            }

            self.inbox_queue(recipient)
                .lock()
                .unwrap()
                .push_back(RelayEnvelope {
                    group_id: group_id.to_string(),
                    sender: sender.to_string(),
                    message_bytes: Arc::clone(&shared_message),
                });

            delivered += 1;
        }

        if debug_logs_enabled() {
            println!(
                "[RELAY] Broadcast application message for group={} from sender={} to {} recipients",
                group_id, sender, delivered
            );
        }

        Ok(())
    }

    pub fn fetch_application_message(&self, recipient: &str) -> Option<Vec<u8>> {
        let queue = self
            .application_inboxes
            .read()
            .unwrap()
            .get(recipient)
            .cloned()?;
        let envelope = queue.lock().unwrap().pop_front();

        if let Some(envelope) = envelope {
            if debug_logs_enabled() {
                println!(
                    "[RELAY] Delivered application message for group={} from sender={} to recipient={}",
                    envelope.group_id, envelope.sender, recipient
                );
            }
            Some(envelope.message_bytes.as_ref().clone())
        } else {
            None
        }
    }

    fn inbox_queue(&self, recipient: &str) -> Arc<Mutex<VecDeque<RelayEnvelope>>> {
        if let Some(queue) = self
            .application_inboxes
            .read()
            .unwrap()
            .get(recipient)
            .cloned()
        {
            return queue;
        }

        let mut inboxes = self.application_inboxes.write().unwrap();
        inboxes
            .entry(recipient.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }
}
