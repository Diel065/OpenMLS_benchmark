use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Semaphore};

use signal_benchmark::debug::{debug_logs_enabled, worker_debug_logs_enabled};
use signal_benchmark::signal_metrics::SignalProfileEvent;
use signal_benchmark::signal_participant::SignalParticipant;
use signal_benchmark::worker_api::{
    handle_command, Command, CommandResponse, CompletedCommandCache, IncomingCommandRequest,
};

const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 128;
const DEFAULT_PACKED_INTERNAL_PARALLELISM: usize = 4;

struct ParticipantSlot {
    participant: SignalParticipant,
    #[allow(dead_code)]
    profile_enabled: bool,
    #[allow(dead_code)]
    profile_path: Option<PathBuf>,
    response_cache: CompletedCommandCache,
    debug_enabled: bool,
}

struct WorkerProcessState {
    physical_worker_id: String,
    kr_url: String,
    relay_url: String,
    participant_handles: HashMap<String, ParticipantActorHandle>,
    internal_parallelism: usize,
    participant_ids: Vec<String>,
    profile_enabled_ids: Vec<String>,
}

struct WorkerCommandEnvelope {
    request_id: Option<String>,
    command: Command,
    phase: Option<String>,
    enqueued_at: Instant,
    enqueued_unix_ms: u128,
    queue_depth_estimate: usize,
    response_tx: oneshot::Sender<CommandResponse>,
}

type CommandTx = mpsc::Sender<WorkerCommandEnvelope>;

struct ParticipantActorHandle {
    tx: CommandTx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandItem {
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
struct BatchCommandRequest {
    pub items: Vec<BatchCommandItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandResponse {
    pub items: Vec<BatchCommandResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchCommandResult {
    pub participant_id: String,
    #[serde(default)]
    pub request_id: Option<String>,
    pub response: CommandResponse,
}

fn parse_args() -> Result<(
    String,
    Option<Vec<String>>,
    Option<Vec<String>>,
    Option<String>,
    usize,
    SocketAddr,
    String,
    String,
)> {
    let mut args = std::env::args().skip(1);

    let mut name: Option<String> = None;
    let mut participants: Option<Vec<String>> = None;
    let mut profile_enabled_ids: Option<Vec<String>> = None;
    let mut profile_path_template: Option<String> = None;
    let mut listen_addr: Option<SocketAddr> = None;
    let mut packed_parallelism: Option<usize> = None;
    let mut kr_url: Option<String> = None;
    let mut relay_url: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                name = args.next();
            }
            "--participants" => {
                if let Some(raw) = args.next() {
                    participants = Some(raw.split(',').map(|s| s.trim().to_string()).collect());
                }
            }
            "--profile-enabled-participant-ids" => {
                if let Some(raw) = args.next() {
                    profile_enabled_ids =
                        Some(raw.split(',').map(|s| s.trim().to_string()).collect());
                }
            }
            "--profile-path-template" => {
                profile_path_template = args.next();
            }
            "--packed-worker-internal-parallelism" => {
                if let Some(raw) = args.next() {
                    packed_parallelism = raw.parse().ok();
                }
            }
            "--kr-url" => {
                kr_url = args.next();
            }
            "--relay-url" => {
                relay_url = args.next();
            }
            "--listen-addr" => {
                let raw = args
                    .next()
                    .ok_or_else(|| anyhow!("Missing value after --listen-addr"))?;
                let parsed: SocketAddr = raw
                    .parse()
                    .map_err(|e| anyhow!("Invalid --listen-addr '{}': {}", raw, e))?;
                listen_addr = Some(parsed);
            }
            _ => {}
        }
    }

    let name = name.ok_or_else(|| anyhow!("Missing --name"))?;
    let kr_url = kr_url.ok_or_else(|| anyhow!("Missing --kr-url"))?;
    let relay_url = relay_url.ok_or_else(|| anyhow!("Missing --relay-url"))?;
    let listen_addr = listen_addr.unwrap_or_else(|| "127.0.0.1:8080".parse().unwrap());

    let participant_ids = if let Some(ref c) = participants {
        c.clone()
    } else {
        vec![name.clone()]
    };

    let parallelism = packed_parallelism.unwrap_or(DEFAULT_PACKED_INTERNAL_PARALLELISM);

    Ok((
        name,
        Some(participant_ids),
        profile_enabled_ids,
        profile_path_template,
        parallelism,
        listen_addr,
        kr_url,
        relay_url,
    ))
}

async fn health() -> &'static str {
    "ok"
}

async fn participant_health(
    Path(participant_id): Path<String>,
    State(state): State<Arc<WorkerProcessState>>,
) -> Json<CommandResponse> {
    if state.participant_handles.contains_key(&participant_id) {
        Json(CommandResponse::ok("ok"))
    } else {
        Json(CommandResponse::error(format!(
            "participant {} not found",
            participant_id
        )))
    }
}

async fn list_participants(
    State(state): State<Arc<WorkerProcessState>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "physical_worker_id": state.physical_worker_id,
        "participant_ids": state.participant_ids,
        "internal_parallelism": state.internal_parallelism,
    }))
}

async fn run_command(
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let (request_id, command, phase) = request.into_parts();

    if state.participant_handles.len() == 1 {
        let (participant_id, handle) = state.participant_handles.iter().next().unwrap();
        let (_, response) = send_to_participant_actor(
            handle,
            participant_id,
            request_id,
            command,
            phase.as_deref(),
        )
        .await;
        return Json(response);
    }

    Json(CommandResponse::error(
        "Multi-participant worker requires /participant/:id/command or /batch-command",
    ))
}

async fn run_command_for_participant(
    Path(participant_id): Path<String>,
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<IncomingCommandRequest>,
) -> Json<CommandResponse> {
    let (request_id, command, phase) = request.into_parts();

    let handle = match state.participant_handles.get(&participant_id) {
        Some(h) => h,
        None => {
            return Json(CommandResponse::error(format!(
                "participant {} not found",
                participant_id
            )))
        }
    };

    let (_, response) = send_to_participant_actor(
        handle,
        &participant_id,
        request_id,
        command,
        phase.as_deref(),
    )
    .await;
    Json(response)
}

async fn run_batch_command(
    State(state): State<Arc<WorkerProcessState>>,
    Json(request): Json<BatchCommandRequest>,
) -> Json<BatchCommandResponse> {
    let semaphore = Arc::new(Semaphore::new(state.internal_parallelism));

    let mut tasks = Vec::new();
    for item in request.items {
        let state = Arc::clone(&state);
        let sem = Arc::clone(&semaphore);
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let handle = match state.participant_handles.get(&item.participant_id) {
                Some(h) => h,
                None => {
                    return BatchCommandResult {
                        participant_id: item.participant_id.clone(),
                        request_id: item.request_id.clone(),
                        response: CommandResponse::error(format!(
                            "participant {} not found",
                            item.participant_id
                        )),
                    };
                }
            };

            let (_, response) = send_to_participant_actor(
                handle,
                &item.participant_id,
                item.request_id.clone(),
                item.command,
                item.phase.as_deref(),
            )
            .await;

            BatchCommandResult {
                participant_id: item.participant_id,
                request_id: item.request_id,
                response,
            }
        }));
    }

    let mut results = Vec::new();
    for task in tasks {
        if let Ok(result) = task.await {
            results.push(result);
        }
    }

    Json(BatchCommandResponse { items: results })
}

async fn send_to_participant_actor(
    handle: &ParticipantActorHandle,
    _participant_id: &str,
    request_id: Option<String>,
    command: Command,
    phase: Option<&str>,
) -> (String, CommandResponse) {
    let (response_tx, response_rx) = oneshot::channel();
    let queue_depth_estimate = handle
        .tx
        .max_capacity()
        .saturating_sub(handle.tx.capacity());

    let envelope = WorkerCommandEnvelope {
        request_id: request_id.clone(),
        command,
        phase: phase.map(ToOwned::to_owned),
        enqueued_at: Instant::now(),
        enqueued_unix_ms: unix_ms_now(),
        queue_depth_estimate,
        response_tx,
    };

    if handle.tx.send(envelope).await.is_err() {
        let rid = request_id.unwrap_or_else(|| "unknown".to_string());
        return (
            rid,
            CommandResponse::error("worker command actor is not running"),
        );
    }

    let response = match response_rx.await {
        Ok(r) => r,
        Err(e) => CommandResponse::error(format!("worker command actor dropped response: {}", e)),
    };

    let rid = request_id.unwrap_or_else(|| "unknown".to_string());
    (rid, response)
}

async fn participant_command_actor(
    participant_id: String,
    physical_worker_id: String,
    mut rx: mpsc::Receiver<WorkerCommandEnvelope>,
    mut slot: ParticipantSlot,
    kr_url: String,
    relay_url: String,
) {
    while let Some(envelope) = rx.recv().await {
        let request_id = envelope.request_id.as_deref().unwrap_or("-");
        let command_name = envelope.command.kind();
        let phase = envelope.phase.as_deref().unwrap_or("-");

        if let Some(request_id) = envelope.request_id.as_deref() {
            if let Some(response) = slot.response_cache.get(request_id) {
                if slot.debug_enabled {
                    eprintln!(
                        "[WORKER {}] command request_id={} command={} phase={} cache_hit=true queue_depth={} enqueued_unix_ms={} finish_unix_ms={} enqueued_ms_ago={} result_status={}",
                        participant_id,
                        request_id,
                        command_name,
                        phase,
                        envelope.queue_depth_estimate,
                        envelope.enqueued_unix_ms,
                        unix_ms_now(),
                        envelope.enqueued_at.elapsed().as_millis(),
                        response.status
                    );
                }

                let _ = envelope.response_tx.send(response);
                continue;
            }
        }

        let start = Instant::now();
        let start_unix_ms = unix_ms_now();

        if slot.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} cache_hit=false queue_depth={} enqueued_unix_ms={} start_unix_ms={} enqueue_wait_ms={}",
                participant_id,
                request_id,
                command_name,
                phase,
                envelope.queue_depth_estimate,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                envelope.enqueued_at.elapsed().as_millis(),
            );
        }

        let result =
            handle_command(&mut slot.participant, &kr_url, &relay_url, envelope.command).await;

        let wall_ns = start.elapsed().as_nanos();
        let mut metrics = None;
        let response = match result {
            Ok(outcome) => {
                metrics = Some(outcome.metrics);
                CommandResponse::ok(outcome.message)
            }
            Err(err) => CommandResponse::error(err.to_string()),
        };

        if let Some(ref profile_path) = slot.profile_path {
            if let Some(parent) = profile_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(profile_path)
            {
                let metrics = metrics.unwrap_or_default();
                let event = SignalProfileEvent {
                    ts_unix_ns: start_unix_ms * 1_000_000,
                    op: command_name.to_string(),
                    implementation: "libsignal".to_string(),
                    wall_ns,
                    cpu_thread_ns: None,
                    alloc_bytes: None,
                    alloc_count: None,
                    artifact_size_bytes: metrics.artifact_size_bytes,
                    participant_count: metrics.participant_count,
                    conversation_size: metrics.conversation_size,
                    prekey_bundle_count: metrics.prekey_bundle_count,
                    session_count: metrics.session_count,
                    ratchet_step_count: metrics.ratchet_step_count,
                    ciphertext_bytes: metrics.ciphertext_bytes,
                    plaintext_bytes: metrics.plaintext_bytes,
                    pid: std::process::id(),
                    thread_id: format!("{:?}", std::thread::current().id()),
                    run_id: env_nonempty("SIGNAL_PROFILE_RUN_ID"),
                    scenario: env_nonempty("SIGNAL_PROFILE_SCENARIO"),
                    node_name: env_nonempty("SIGNAL_PROFILE_NODE")
                        .or_else(|| Some(physical_worker_id.clone())),
                    pod_name: env_nonempty("HOSTNAME").or_else(|| Some(physical_worker_id.clone())),
                };
                if let Ok(json_line) = serde_json::to_string(&event) {
                    let _ = std::io::Write::write(&mut file, json_line.as_bytes());
                    let _ = std::io::Write::write(&mut file, b"\n");
                }
            }
        }

        if slot.debug_enabled {
            eprintln!(
                "[WORKER {}] command request_id={} command={} phase={} enqueued_unix_ms={} start_unix_ms={} finish_unix_ms={} finish_ms={} result_status={}",
                participant_id,
                request_id,
                command_name,
                phase,
                envelope.enqueued_unix_ms,
                start_unix_ms,
                unix_ms_now(),
                start.elapsed().as_millis(),
                response.status,
            );
        }

        if let Some(request_id) = envelope.request_id {
            slot.response_cache.insert(request_id, response.clone());
        }

        let _ = envelope.response_tx.send(response);
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

async fn debug_layout(State(state): State<Arc<WorkerProcessState>>) -> Json<serde_json::Value> {
    let participants_info: Vec<serde_json::Value> = state
        .participant_ids
        .iter()
        .map(|id| {
            serde_json::json!({
                "participant_id": id,
                "profile_enabled": state.profile_enabled_ids.contains(id),
            })
        })
        .collect();

    Json(serde_json::json!({
        "physical_worker_id": state.physical_worker_id,
        "kr_url": state.kr_url,
        "relay_url": state.relay_url,
        "internal_parallelism": state.internal_parallelism,
        "participants": participants_info,
    }))
}

fn unix_ms_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn command_queue_capacity() -> usize {
    std::env::var("SIGNAL_WORKER_COMMAND_QUEUE_CAPACITY")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|capacity| *capacity > 0)
        .unwrap_or(DEFAULT_COMMAND_QUEUE_CAPACITY)
}

fn idempotency_cache_size() -> usize {
    std::env::var("SIGNAL_WORKER_IDEMPOTENCY_CACHE_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16_384)
}

fn idempotency_cache_ttl() -> Duration {
    let seconds = std::env::var("SIGNAL_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(21_600);

    Duration::from_secs(seconds)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (
        physical_worker_id,
        participant_ids_opt,
        profile_enabled_ids_opt,
        profile_path_template_opt,
        internal_parallelism,
        listen_addr,
        worker_kr_url,
        worker_relay_url,
    ) = parse_args()?;

    let participant_ids = participant_ids_opt.unwrap_or_else(|| vec![physical_worker_id.clone()]);
    let profile_enabled_set: std::collections::HashSet<String> = profile_enabled_ids_opt
        .unwrap_or_else(|| participant_ids.clone())
        .into_iter()
        .collect();

    let profile_template = profile_path_template_opt;

    let queue_capacity = command_queue_capacity();
    let cache_size = idempotency_cache_size();
    let cache_ttl = idempotency_cache_ttl();

    let mut participant_ids_list: Vec<String> = Vec::new();
    let mut profile_enabled_ids_list: Vec<String> = Vec::new();
    let mut participant_handles: HashMap<String, ParticipantActorHandle> = HashMap::new();

    for participant_id in &participant_ids {
        let debug_enabled = worker_debug_logs_enabled(participant_id) || debug_logs_enabled();
        let is_profile_enabled = profile_enabled_set.contains(participant_id);

        let profile_path = if is_profile_enabled {
            if let Some(ref template) = profile_template {
                Some(PathBuf::from(
                    template.replace("{participant_id}", participant_id),
                ))
            } else {
                None
            }
        } else {
            None
        };

        let participant = SignalParticipant::new(participant_id)?;
        let slot = ParticipantSlot {
            participant,
            profile_enabled: is_profile_enabled,
            profile_path,
            response_cache: CompletedCommandCache::new(cache_size, cache_ttl),
            debug_enabled,
        };

        let (command_tx, command_rx) = mpsc::channel(queue_capacity);
        participant_handles.insert(
            participant_id.clone(),
            ParticipantActorHandle { tx: command_tx },
        );

        let kr = worker_kr_url.clone();
        let relay = worker_relay_url.clone();
        let pid = participant_id.clone();

        let physical_id = physical_worker_id.clone();
        tokio::spawn(participant_command_actor(
            pid,
            physical_id,
            command_rx,
            slot,
            kr,
            relay,
        ));
        participant_ids_list.push(participant_id.clone());
        if is_profile_enabled {
            profile_enabled_ids_list.push(participant_id.clone());
        }
    }

    let state = Arc::new(WorkerProcessState {
        physical_worker_id,
        kr_url: worker_kr_url.clone(),
        relay_url: worker_relay_url.clone(),
        participant_handles,
        internal_parallelism,
        participant_ids: participant_ids_list,
        profile_enabled_ids: profile_enabled_ids_list,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/participants", get(list_participants))
        .route(
            "/participant/{participant_id}/health",
            get(participant_health),
        )
        .route("/command", post(run_command))
        .route(
            "/participant/{participant_id}/command",
            post(run_command_for_participant),
        )
        .route("/batch-command", post(run_batch_command))
        .route("/debug/layout", get(debug_layout))
        .with_state(Arc::clone(&state));

    let is_packed = state.participant_handles.len() > 1;
    let debug_any = state
        .participant_handles
        .keys()
        .any(|id| worker_debug_logs_enabled(id) || debug_logs_enabled());

    if debug_any {
        let participant_list: Vec<_> = state.participant_handles.keys().cloned().collect();
        eprintln!(
            "[WORKER {}] starting on http://{} with KR={} RELAY={} participants={:?} internal_parallelism={} is_packed={}",
            state.physical_worker_id,
            listen_addr,
            worker_kr_url,
            worker_relay_url,
            participant_list,
            state.internal_parallelism,
            is_packed
        );
    }

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .map_err(|e| anyhow!("Could not bind worker listener on {}: {}", listen_addr, e))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow!("Worker server crashed: {}", e))?;

    Ok(())
}
