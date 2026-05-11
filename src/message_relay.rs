use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, RwLock,
};

use crate::debug::debug_logs_enabled;
use crate::service_metrics::ServiceMetrics;

#[derive(Clone, Debug)]
struct RelayEnvelope {
    id: String,
    conversation_id: String,
    sender: String,
    message_bytes: Arc<Vec<u8>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingMessage {
    pub id: String,
    pub conversation_id: String,
    pub sender: String,
    pub message_hex: String,
}

pub struct MessageRelay {
    inboxes: RwLock<HashMap<String, Arc<Mutex<VecDeque<RelayEnvelope>>>>>,
    next_message_id: AtomicU64,
    metrics: ServiceMetrics,
}

impl MessageRelay {
    pub fn new() -> Self {
        Self {
            inboxes: RwLock::new(HashMap::new()),
            next_message_id: AtomicU64::new(1),
            metrics: ServiceMetrics::new(),
        }
    }

    pub fn metrics(&self) -> &ServiceMetrics {
        &self.metrics
    }

    pub fn publish_pairwise_message(
        &self,
        conversation_id: &str,
        sender: &str,
        recipients: &[String],
        message_bytes: Vec<u8>,
    ) -> Result<(), String> {
        if recipients.is_empty() {
            return Err("No recipients provided".to_string());
        }

        let mut delivered = 0usize;
        let shared = Arc::new(message_bytes);
        let message_seq = self.next_message_id.fetch_add(1, Ordering::Relaxed);

        for recipient in recipients {
            if recipient == sender {
                continue;
            }

            self.inbox_queue(recipient)
                .lock()
                .unwrap()
                .push_back(RelayEnvelope {
                    id: pairwise_message_id(conversation_id, sender, message_seq, recipient),
                    conversation_id: conversation_id.to_string(),
                    sender: sender.to_string(),
                    message_bytes: Arc::clone(&shared),
                });

            delivered += 1;
        }

        if debug_logs_enabled() {
            println!(
                "[RELAY] Pairwise message for conversation={} from={} to {} recipients",
                conversation_id, sender, delivered
            );
        }

        Ok(())
    }

    pub fn fetch_message(&self, recipient: &str) -> Option<Vec<u8>> {
        self.fetch_pending_message_record(recipient)
            .map(|envelope| envelope.message_bytes.as_ref().clone())
    }

    pub fn fetch_pending_message(&self, recipient: &str) -> Option<PendingMessage> {
        self.fetch_pending_message_record(recipient)
            .map(|envelope| PendingMessage {
                id: envelope.id,
                conversation_id: envelope.conversation_id,
                sender: envelope.sender,
                message_hex: hex::encode(envelope.message_bytes.as_ref()),
            })
    }

    pub fn ack_message(&self, recipient: &str, message_id: &str) -> bool {
        let queue = self.inboxes.read().unwrap().get(recipient).cloned();

        let Some(queue) = queue else {
            return false;
        };

        let mut queue = queue.lock().unwrap();
        let before = queue.len();
        queue.retain(|envelope| envelope.id != message_id);
        before != queue.len()
    }

    fn fetch_pending_message_record(&self, recipient: &str) -> Option<RelayEnvelope> {
        let queue = self.inboxes.read().unwrap().get(recipient).cloned()?;
        let envelope = queue.lock().unwrap().front().cloned();

        if let Some(envelope) = &envelope {
            if debug_logs_enabled() {
                println!(
                    "[RELAY] Replayed pending message id={} conversation={} sender={} recipient={}",
                    envelope.id, envelope.conversation_id, envelope.sender, recipient
                );
            }
        }

        envelope
    }

    fn inbox_queue(&self, recipient: &str) -> Arc<Mutex<VecDeque<RelayEnvelope>>> {
        if let Some(queue) = self.inboxes.read().unwrap().get(recipient).cloned() {
            return queue;
        }

        let mut inboxes = self.inboxes.write().unwrap();
        inboxes
            .entry(recipient.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }
}

impl Default for MessageRelay {
    fn default() -> Self {
        Self::new()
    }
}

fn pairwise_message_id(conversation_id: &str, sender: &str, seq: u64, recipient: &str) -> String {
    format!("{conversation_id}:{sender}:{seq}:{recipient}")
}
