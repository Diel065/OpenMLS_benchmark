use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug)]
struct RelayEnvelope {
    group_id: String,
    sender: String,
    message_bytes: Vec<u8>,
}

pub struct MessageRelay {
    application_inboxes: HashMap<String, VecDeque<RelayEnvelope>>,
}

impl MessageRelay {
    pub fn new() -> Self {
        Self {
            application_inboxes: HashMap::new(),
        }
    }

    pub fn publish_group_application_message(
        &mut self,
        group_id: &str,
        sender: &str,
        recipients: &[String],
        message_bytes: Vec<u8>,
    ) -> Result<(), String> {
        if recipients.is_empty() {
            return Err("No recipients were provided to the message relay".to_string());
        }

        let mut delivered = 0usize;

        for recipient in recipients {
            if recipient == sender {
                continue;
            }

            self.application_inboxes
                .entry(recipient.clone())
                .or_default()
                .push_back(RelayEnvelope {
                    group_id: group_id.to_string(),
                    sender: sender.to_string(),
                    message_bytes: message_bytes.clone(),
                });

            delivered += 1;
        }

        println!(
            "[RELAY] Broadcast application message for group={} from sender={} to {} recipients",
            group_id, sender, delivered
        );

        Ok(())
    }

    pub fn fetch_application_message(&mut self, recipient: &str) -> Option<Vec<u8>> {
        let envelope = self
            .application_inboxes
            .get_mut(recipient)
            .and_then(|queue| queue.pop_front());

        if let Some(envelope) = envelope {
            println!(
                "[RELAY] Delivered application message for group={} from sender={} to recipient={}",
                envelope.group_id, envelope.sender, recipient
            );
            Some(envelope.message_bytes)
        } else {
            None
        }
    }
}
