use std::{
    collections::VecDeque,
    error::Error as StdError,
    fs::{self, File},
    future::Future,
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use futures_util::future::{join_all, try_join_all};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use crate::http_retry::{
    is_connect_stage_reqwest_error, is_transient_reqwest_error, is_transient_status,
    retry_transient_http_async, RetryDecision,
};
use crate::worker_api::{Command, CommandRequestEnvelope, CommandResponse};

const WORKER_COMMAND_MAX_ATTEMPTS: usize = 10;
const WORKER_COMMAND_INITIAL_DELAY: Duration = Duration::from_millis(100);
const WORKER_COMMAND_MAX_DELAY: Duration = Duration::from_secs(3);
const DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST: usize = 32;
const DEFAULT_MAX_FANOUT_PARALLELISM: usize = 32;
const ADAPTIVE_FANOUT_START: usize = 32;
const FANOUT_LATENCY_SPIKE_P95_MS: u128 = 5_000;
const FANOUT_STABLE_INCREASE_AFTER: usize = 20;

static WORKER_COMMAND_REQUEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct StaircaseConfig {
    pub preflight_only: bool,
    pub ds_url: String,
    pub workers: Vec<WorkerSpec>,
    pub min_size: usize,
    pub max_size: Option<usize>,
    pub step_size: usize,
    pub roundtrips: usize,
    pub update_rounds: usize,
    pub app_rounds: usize,
    pub max_update_samples_per_plateau: usize,
    pub max_app_samples_per_payload: usize,
    pub payload_sizes: Vec<usize>,
    pub run_id: String,
    pub scenario: String,
    pub output_dir: String,
    pub worker_health_timeout_seconds: u64,
    pub worker_health_poll_ms: u64,
    pub max_fanout_parallelism: usize,
    pub fanout_adaptive: bool,
    pub http_pool_max_idle_per_host: usize,
}

#[derive(Debug, Clone)]
pub struct WorkerSpec {
    pub id: String,
    pub url: String,
}

#[derive(Debug, Clone)]
struct GroupStateSnapshot {
    group_id: String,
    epoch: u64,
    members: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NetworkPhaseMetrics {
    pub phase: String,
    pub group_size: usize,
    pub operation: String,
    pub request_count: usize,
    pub recipient_count: usize,
    pub max_parallelism: usize,
    pub wall_ms: u128,
    pub retry_count: usize,
    pub retry_sleep_ms: u128,
    pub failures: usize,
    pub worker_latency_p50_ms: Option<u128>,
    pub worker_latency_p95_ms: Option<u128>,
    pub worker_latency_p99_ms: Option<u128>,
    pub worker_latency_max_ms: Option<u128>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ProfileEvent {
    ts_unix_ns: u128,
    op: String,
    implementation: String,
    wall_ns: u128,
    cpu_thread_ns: Option<u128>,
    alloc_bytes: Option<u64>,
    alloc_count: Option<u64>,
    artifact_size_bytes: Option<usize>,
    encrypted_group_info_bytes: Option<usize>,
    encrypted_secrets_count: Option<usize>,
    group_epoch: Option<u64>,
    tree_size: Option<u32>,
    member_count: Option<usize>,
    invitee_count: Option<isize>,
    ciphersuite: Option<String>,
    app_msg_plaintext_bytes: Option<usize>,
    app_msg_padding_bytes: Option<usize>,
    app_msg_ciphertext_bytes: Option<usize>,
    aad_bytes: Option<usize>,
    pid: u32,
    thread_id: String,
    run_id: Option<String>,
    scenario: Option<String>,
    node_name: Option<String>,
    pod_name: Option<String>,
}

struct Progress {
    total_units: usize,
    completed_units: usize,
    start: Instant,
}

impl Progress {
    fn new(total_units: usize) -> Self {
        Self {
            total_units: total_units.max(1),
            completed_units: 0,
            start: Instant::now(),
        }
    }

    fn tick(&mut self, label: &str) {
        self.completed_units = (self.completed_units + 1).min(self.total_units);
        self.render(label);
    }

    fn render(&self, label: &str) {
        let width = 32usize;
        let ratio = self.completed_units as f64 / self.total_units as f64;
        let filled = ((ratio * width as f64).round() as usize).min(width);

        let mut bar = String::with_capacity(width);
        for _ in 0..filled {
            bar.push('#');
        }
        for _ in filled..width {
            bar.push('-');
        }

        let elapsed = self.start.elapsed();
        let eta = if self.completed_units == 0 {
            None
        } else {
            let elapsed_secs = elapsed.as_secs_f64();
            let per_unit = elapsed_secs / self.completed_units as f64;
            let remaining = self.total_units.saturating_sub(self.completed_units) as f64;
            Some(Duration::from_secs_f64(per_unit * remaining))
        };

        let percent = ratio * 100.0;
        let eta_text = eta
            .map(format_hms)
            .unwrap_or_else(|| "--:--:--".to_string());

        eprint!(
            "\r[{}] {:6.2}% | {}/{} units | elapsed {} | ETA {} | {}",
            bar,
            percent,
            self.completed_units,
            self.total_units,
            format_hms(elapsed),
            eta_text,
            label
        );
        let _ = io::stderr().flush();
    }

    fn finish(&self) {
        eprintln!();
    }
}

#[derive(Debug)]
struct FanoutController {
    max_parallelism: usize,
    current_parallelism: usize,
    adaptive: bool,
    stable_successes: usize,
}

impl FanoutController {
    fn new(max_parallelism: usize, adaptive: bool) -> Self {
        let max_parallelism = max_parallelism.max(1);
        let current_parallelism = if adaptive {
            ADAPTIVE_FANOUT_START.min(max_parallelism).max(1)
        } else {
            max_parallelism
        };

        Self {
            max_parallelism,
            current_parallelism,
            adaptive,
            stable_successes: 0,
        }
    }

    fn parallelism(&self) -> usize {
        self.current_parallelism.max(1)
    }

    fn record(&mut self, phase: &str, operation: &str, summary: &FanoutSummary) {
        if !self.adaptive {
            return;
        }

        let p95 = summary.latency_p95_ms.unwrap_or(0);
        let should_reduce = summary.failures > 0 || p95 >= FANOUT_LATENCY_SPIKE_P95_MS;

        if should_reduce {
            let previous = self.current_parallelism;
            self.current_parallelism = (self.current_parallelism / 2).max(1);
            self.stable_successes = 0;

            if self.current_parallelism != previous {
                eprintln!(
                    "[fanout-adaptive] phase={} operation={} reducing parallelism {} -> {} failures={} p95_ms={}",
                    phase,
                    operation,
                    previous,
                    self.current_parallelism,
                    summary.failures,
                    p95
                );
            }
            return;
        }

        self.stable_successes += 1;
        if self.stable_successes >= FANOUT_STABLE_INCREASE_AFTER
            && self.current_parallelism < self.max_parallelism
        {
            let previous = self.current_parallelism;
            self.current_parallelism = (self.current_parallelism + 4).min(self.max_parallelism);
            self.stable_successes = 0;

            eprintln!(
                "[fanout-adaptive] phase={} operation={} increasing parallelism {} -> {} p95_ms={}",
                phase, operation, previous, self.current_parallelism, p95
            );
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FanoutSummary {
    request_count: usize,
    failures: usize,
    wall_ms: u128,
    latency_p50_ms: Option<u128>,
    latency_p95_ms: Option<u128>,
    latency_p99_ms: Option<u128>,
    latency_max_ms: Option<u128>,
}

#[derive(Debug, Clone)]
struct ExpectedGroupState {
    group_id: String,
    epoch: u64,
    members: Vec<String>,
}

#[derive(Debug, Clone)]
enum ExpectedReceiveCommitState {
    Group(ExpectedGroupState),
    Removed { expected_epoch: u64 },
}

impl From<GroupStateSnapshot> for ExpectedGroupState {
    fn from(snapshot: GroupStateSnapshot) -> Self {
        Self {
            group_id: snapshot.group_id,
            epoch: snapshot.epoch,
            members: snapshot.members,
        }
    }
}

impl ExpectedGroupState {
    fn matches(&self, snapshot: &GroupStateSnapshot) -> bool {
        snapshot.group_id == self.group_id
            && snapshot.epoch == self.epoch
            && snapshot.members == self.members
    }
}

pub fn parse_worker_specs(raw_specs: &[String]) -> Result<Vec<WorkerSpec>> {
    let mut workers = Vec::with_capacity(raw_specs.len());

    for raw in raw_specs {
        let spec = parse_worker_spec(raw)?;
        if workers.iter().any(|w: &WorkerSpec| w.id == spec.id) {
            return Err(anyhow!("Duplicate worker id '{}'", spec.id));
        }
        workers.push(spec);
    }

    if workers.is_empty() {
        return Err(anyhow!("At least one worker must be provided"));
    }

    Ok(workers)
}

pub fn run_dir_for(output_dir: &str, run_id: &str) -> PathBuf {
    PathBuf::from(output_dir).join(run_id)
}

pub fn run_staircase_benchmark(config: StaircaseConfig) -> Result<()> {
    let worker_threads = std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(4);

    eprintln!(
        "[runtime] benchmark runner using multi-thread Tokio runtime with {} worker threads",
        worker_threads
    );

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .context("Failed to build benchmark runner Tokio runtime")?
        .block_on(run_staircase_benchmark_async(config))
}

async fn run_staircase_benchmark_async(config: StaircaseConfig) -> Result<()> {
    let max_size = validate_config(&config, config.workers.len())?;

    let run_dir = run_dir_for(&config.output_dir, &config.run_id);
    fs::create_dir_all(&run_dir)?;

    let max_fanout_parallelism = effective_max_fanout_parallelism(config.max_fanout_parallelism);
    let mut fanout = FanoutController::new(max_fanout_parallelism, config.fanout_adaptive);
    let http_pool_max_idle_per_host =
        effective_http_pool_max_idle_per_host(config.http_pool_max_idle_per_host);

    eprintln!(
        "[network] runner http_pool_max_idle_per_host={} max_fanout_parallelism={} fanout_adaptive={} initial_effective_fanout_parallelism={}",
        http_pool_max_idle_per_host,
        max_fanout_parallelism,
        config.fanout_adaptive,
        fanout.parallelism()
    );

    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(http_pool_max_idle_per_host)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .build()
        .context("Failed to build HTTP client")?;

    wait_for_health(&http, &config.ds_url, Duration::from_secs(10))
        .await
        .with_context(|| format!("DS at {} is not healthy", config.ds_url))?;

    let worker_health_timeout = Duration::from_secs(config.worker_health_timeout_seconds);
    let worker_health_poll = Duration::from_millis(config.worker_health_poll_ms);

    eprintln!(
        "[preflight] waiting up to {} for {} workers to become healthy",
        format_hms(worker_health_timeout),
        config.workers.len()
    );

    wait_for_all_workers_healthy(
        &http,
        &config.workers,
        worker_health_timeout,
        worker_health_poll,
        max_fanout_parallelism,
    )
    .await?;

    if config.preflight_only {
        eprintln!("[preflight] preflight-only mode complete; skipping MLS benchmark logic");
        return Ok(());
    }

    let plateau_sequence = build_plateau_sequence(
        config.min_size,
        max_size,
        config.step_size,
        config.roundtrips,
    );

    let total_units = estimate_total_units(
        &plateau_sequence,
        config.update_rounds,
        config.app_rounds,
        config.max_update_samples_per_plateau,
        config.max_app_samples_per_payload,
        config.payload_sizes.len(),
    );

    eprintln!(
        "Scenario plan: plateaus={:?}, payload_sizes={:?}, update_cap={}, app_cap={}, total_units≈{}",
        plateau_sequence,
        config.payload_sizes,
        config.max_update_samples_per_plateau,
        config.max_app_samples_per_payload,
        total_units
    );

    let mut progress = Progress::new(total_units);
    progress.render("starting");

    let leader = config.workers[0].clone();
    let mut active = vec![leader.clone()];
    let mut idle: VecDeque<WorkerSpec> = config.workers.iter().skip(1).cloned().collect();

    create_group(&http, &leader, &mut progress).await?;
    let active_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
    let initial_state =
        ensure_converged(&http, &active, &active_ids, max_fanout_parallelism).await?;
    eprintln!(
        "\nInitial convergence: group_id={}, epoch={}, members={:?}",
        initial_state.group_id, initial_state.epoch, initial_state.members
    );

    for (plateau_idx, &target_size) in plateau_sequence.iter().enumerate() {
        eprintln!(
            "\n=== Plateau {}/{} | target active members = {} ===",
            plateau_idx + 1,
            plateau_sequence.len(),
            target_size
        );

        transition_to_size(
            &http,
            &mut active,
            &mut idle,
            target_size,
            &mut fanout,
            &mut progress,
        )
        .await?;

        let active_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
        let state = ensure_converged(&http, &active, &active_ids, max_fanout_parallelism).await?;
        eprintln!(
            "\n[plateau {}] converged at epoch {} with members {:?}",
            target_size, state.epoch, state.members
        );

        run_update_phase(
            &http,
            &active,
            target_size,
            config.update_rounds,
            config.max_update_samples_per_plateau,
            &mut fanout,
            &mut progress,
        )
        .await?;

        let state_after_updates =
            ensure_converged(&http, &active, &active_ids, max_fanout_parallelism).await?;
        eprintln!(
            "\n[plateau {}] post-update convergence at epoch {}",
            target_size, state_after_updates.epoch
        );

        run_application_phase(
            &http,
            &active,
            target_size,
            config.app_rounds,
            config.max_app_samples_per_payload,
            &config.payload_sizes,
            &mut fanout,
            &mut progress,
        )
        .await?;

        eprintln!("\n=== Plateau {} complete ===", target_size);
    }

    progress.finish();

    let worker_ids: Vec<String> = config.workers.iter().map(|w| w.id.clone()).collect();
    aggregate_csv(&run_dir, &worker_ids)?;

    println!(
        "HTTP staircase benchmark finished. Output in {}",
        run_dir.display()
    );
    Ok(())
}

fn effective_max_fanout_parallelism(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }

    DEFAULT_MAX_FANOUT_PARALLELISM
}

fn effective_http_pool_max_idle_per_host(configured: usize) -> usize {
    if configured > 0 {
        configured
    } else {
        DEFAULT_HTTP_POOL_MAX_IDLE_PER_HOST
    }
}

fn format_hms(d: Duration) -> String {
    let total = d.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn parse_worker_spec(raw: &str) -> Result<WorkerSpec> {
    let (id, url) = raw
        .split_once('=')
        .ok_or_else(|| anyhow!("Invalid worker '{}', expected ID=URL", raw))?;

    let id = id.trim();
    let url = url.trim().trim_end_matches('/');

    if id.is_empty() {
        return Err(anyhow!("Worker id cannot be empty in '{}'", raw));
    }
    if url.is_empty() {
        return Err(anyhow!("Worker url cannot be empty in '{}'", raw));
    }

    Ok(WorkerSpec {
        id: id.to_string(),
        url: url.to_string(),
    })
}

async fn wait_for_health(http: &reqwest::Client, base_url: &str, timeout: Duration) -> Result<()> {
    let url = format!("{}/health", base_url.trim_end_matches('/'));
    let per_request_timeout = timeout.min(Duration::from_secs(5));

    retry_transient_http_async("ds.health", None, &url, || async {
        let response = match http.get(&url).timeout(per_request_timeout).send().await {
            Ok(response) => response,
            Err(err) if is_transient_reqwest_error(&err) => {
                return RetryDecision::Transient(err.to_string())
            }
            Err(err) => return RetryDecision::Fatal(anyhow!(err)),
        };

        let status = response.status();

        if status.is_success() {
            return RetryDecision::Success(());
        }

        let body = response.text().await.unwrap_or_default();

        if is_transient_status(status) {
            return RetryDecision::Transient(format!("HTTP {}: {}", status, body));
        }

        RetryDecision::Fatal(anyhow!(
            "Health check failed with status {}: {}",
            status,
            body
        ))
    })
    .await
}

async fn wait_for_all_workers_healthy(
    http: &reqwest::Client,
    workers: &[WorkerSpec],
    timeout: Duration,
    poll: Duration,
    max_parallelism: usize,
) -> Result<()> {
    let start = Instant::now();
    let mut remaining: Vec<usize> = (0..workers.len()).collect();
    let mut last_report = Instant::now();
    let max_parallelism = max_parallelism.max(1);

    while start.elapsed() < timeout {
        let mut still_unhealthy = Vec::new();

        for chunk in remaining.chunks(max_parallelism) {
            let probes = chunk.iter().map(|&idx| {
                let worker = &workers[idx];
                async move {
                    let url = format!("{}/health", worker.url.trim_end_matches('/'));
                    let healthy = matches!(
                        http.get(&url).send().await,
                        Ok(resp) if resp.status().is_success()
                    );
                    Ok::<_, anyhow::Error>((idx, healthy))
                }
            });

            for (idx, healthy) in try_join_all(probes).await? {
                if !healthy {
                    still_unhealthy.push(idx);
                }
            }
        }

        let healthy_count = workers.len().saturating_sub(still_unhealthy.len());

        if still_unhealthy.is_empty() {
            eprintln!(
                "[preflight] all {} workers are healthy after {}",
                workers.len(),
                format_hms(start.elapsed())
            );
            emit_network_metrics(NetworkPhaseMetrics {
                phase: "preflight".to_string(),
                group_size: workers.len(),
                operation: "worker_health".to_string(),
                request_count: workers.len(),
                recipient_count: workers.len(),
                max_parallelism,
                wall_ms: start.elapsed().as_millis(),
                retry_count: 0,
                retry_sleep_ms: 0,
                failures: 0,
                worker_latency_p50_ms: None,
                worker_latency_p95_ms: None,
                worker_latency_p99_ms: None,
                worker_latency_max_ms: None,
            });
            return Ok(());
        }

        if last_report.elapsed() >= Duration::from_secs(5) {
            let examples: Vec<String> = still_unhealthy
                .iter()
                .take(10)
                .map(|&idx| workers[idx].id.clone())
                .collect();

            eprintln!(
                "[preflight] {}/{} workers healthy; still waiting for {}. Examples: {:?}",
                healthy_count,
                workers.len(),
                still_unhealthy.len(),
                examples
            );

            last_report = Instant::now();
        }

        remaining = still_unhealthy;
        tokio::time::sleep(poll).await;
    }

    let examples: Vec<String> = remaining
        .iter()
        .take(25)
        .map(|&idx| {
            let worker = &workers[idx];
            format!("{}={}", worker.id, worker.url)
        })
        .collect();

    emit_network_metrics(NetworkPhaseMetrics {
        phase: "preflight".to_string(),
        group_size: workers.len(),
        operation: "worker_health".to_string(),
        request_count: workers.len(),
        recipient_count: workers.len(),
        max_parallelism,
        wall_ms: start.elapsed().as_millis(),
        retry_count: 0,
        retry_sleep_ms: 0,
        failures: remaining.len(),
        worker_latency_p50_ms: None,
        worker_latency_p95_ms: None,
        worker_latency_p99_ms: None,
        worker_latency_max_ms: None,
    });

    Err(anyhow!(
        "Timeout waiting for worker readiness after {}. {}/{} workers still unhealthy. Examples: {:?}",
        format_hms(timeout),
        remaining.len(),
        workers.len(),
        examples
    ))
}

#[derive(Debug, Clone)]
struct WorkerCommandContext {
    request_id: String,
    expected_epoch: Option<u64>,
    phase: Option<String>,
}

impl WorkerCommandContext {
    fn new(worker: &WorkerSpec, command: &Command) -> Self {
        Self::with_metadata(worker, command, None, None)
    }

    fn with_metadata(
        worker: &WorkerSpec,
        command: &Command,
        expected_epoch: Option<u64>,
        phase: Option<&str>,
    ) -> Self {
        let seq = WORKER_COMMAND_REQUEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let request_id = format!(
            "runner-{}-{}-{}-{}",
            std::process::id(),
            worker.id,
            command.kind(),
            seq
        );

        Self {
            request_id,
            expected_epoch,
            phase: phase.map(ToOwned::to_owned),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerCommandErrorClass {
    TransportRetryable,
    FatalHttpStatus,
    FatalDecode,
}

impl WorkerCommandErrorClass {
    fn as_str(self) -> &'static str {
        match self {
            WorkerCommandErrorClass::TransportRetryable => "transport-retryable",
            WorkerCommandErrorClass::FatalHttpStatus => "fatal-http-status",
            WorkerCommandErrorClass::FatalDecode => "fatal-decode",
        }
    }
}

#[derive(Debug)]
struct WorkerCommandError {
    worker_id: String,
    command: &'static str,
    url: String,
    request_id: String,
    attempts: usize,
    classification: WorkerCommandErrorClass,
    last_error: String,
    diagnostic: Option<String>,
}

impl WorkerCommandError {
    fn is_ambiguous_transport(&self) -> bool {
        self.classification == WorkerCommandErrorClass::TransportRetryable
    }
}

impl std::fmt::Display for WorkerCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "runner.worker_command failed: worker={} command={} url={} request_id={} attempts={} classification={} last_error={}",
            self.worker_id,
            self.command,
            self.url,
            self.request_id,
            self.attempts,
            self.classification.as_str(),
            self.last_error
        )?;

        if let Some(diagnostic) = &self.diagnostic {
            write!(f, " diagnostics={}", diagnostic)?;
        }

        Ok(())
    }
}

impl StdError for WorkerCommandError {}

fn reqwest_error_diagnostic(err: &reqwest::Error) -> String {
    let mut parts = Vec::new();
    parts.push(format!("top_level={}", err));
    parts.push(format!("is_connect={}", err.is_connect()));
    parts.push(format!("is_timeout={}", err.is_timeout()));
    parts.push(format!("is_request={}", err.is_request()));
    parts.push(format!("is_body={}", err.is_body()));
    parts.push(format!(
        "status={}",
        err.status()
            .map(|status| status.to_string())
            .unwrap_or_else(|| "-".to_string())
    ));

    let inferred_stage = if err.is_connect() {
        "connect"
    } else if err.is_timeout() {
        "timeout while connecting, writing request, or waiting for response"
    } else if err.is_body() {
        "reading response body"
    } else if err.is_request() {
        "writing request or waiting for response headers"
    } else {
        "unknown"
    };
    parts.push(format!("inferred_stage={}", inferred_stage));

    let mut source = err.source();
    let mut idx = 0usize;
    while let Some(err) = source {
        parts.push(format!("source[{}]={}", idx, err));
        source = err.source();
        idx += 1;
    }

    parts.join("; ")
}

async fn retry_worker_command_sleep(
    worker: &WorkerSpec,
    command_name: &str,
    attempt: usize,
    delay: &mut Duration,
    url: &str,
    err_text: &str,
) {
    let sleep_for = worker_command_with_jitter(*delay);
    eprintln!(
        "[retry] op=runner.worker_command worker={} command={} attempt={}/{} delay_ms={} url={} error={}",
        worker.id,
        command_name,
        attempt,
        WORKER_COMMAND_MAX_ATTEMPTS,
        sleep_for.as_millis(),
        url,
        err_text
    );
    tokio::time::sleep(sleep_for).await;
    *delay = worker_command_next_delay(*delay);
}

async fn send_command_with_context(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    context: &WorkerCommandContext,
) -> Result<CommandResponse> {
    let url = format!("{}/command", worker.url);
    let command_name = command.kind();
    let mut delay = WORKER_COMMAND_INITIAL_DELAY;
    let request = CommandRequestEnvelope {
        request_id: context.request_id.clone(),
        command: command.clone(),
        expected_epoch: context.expected_epoch,
        phase: context.phase.clone(),
    };

    for attempt in 1..=WORKER_COMMAND_MAX_ATTEMPTS {
        let response = match http.post(&url).json(&request).send().await {
            Ok(response) => response,
            Err(err)
                if is_transient_reqwest_error(&err) || is_connect_stage_reqwest_error(&err) =>
            {
                let err_text = err.to_string();
                let diagnostic = reqwest_error_diagnostic(&err);

                if attempt == WORKER_COMMAND_MAX_ATTEMPTS {
                    return Err(WorkerCommandError {
                        worker_id: worker.id.clone(),
                        command: command_name,
                        url: url.clone(),
                        request_id: context.request_id.clone(),
                        attempts: attempt,
                        classification: WorkerCommandErrorClass::TransportRetryable,
                        last_error: err_text,
                        diagnostic: Some(diagnostic),
                    }
                    .into());
                }

                retry_worker_command_sleep(
                    worker,
                    command_name,
                    attempt,
                    &mut delay,
                    &url,
                    &format!("{} ({})", err_text, diagnostic),
                )
                .await;
                continue;
            }
            Err(err) => {
                return Err(WorkerCommandError {
                    worker_id: worker.id.clone(),
                    command: command_name,
                    url,
                    request_id: context.request_id.clone(),
                    attempts: attempt,
                    classification: WorkerCommandErrorClass::FatalDecode,
                    last_error: err.to_string(),
                    diagnostic: Some(reqwest_error_diagnostic(&err)),
                }
                .into());
            }
        };

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let last_error = format!("HTTP {}: {}", status, body);

            if is_transient_status(status) && attempt < WORKER_COMMAND_MAX_ATTEMPTS {
                retry_worker_command_sleep(
                    worker,
                    command_name,
                    attempt,
                    &mut delay,
                    &url,
                    &last_error,
                )
                .await;
                continue;
            }

            return Err(WorkerCommandError {
                worker_id: worker.id.clone(),
                command: command_name,
                url,
                request_id: context.request_id.clone(),
                attempts: attempt,
                classification: WorkerCommandErrorClass::FatalHttpStatus,
                last_error,
                diagnostic: None,
            }
            .into());
        }

        match response.json::<CommandResponse>().await {
            Ok(parsed) => return Ok(parsed),
            Err(err) if is_transient_reqwest_error(&err) => {
                let err_text = err.to_string();
                let diagnostic = reqwest_error_diagnostic(&err);

                if attempt == WORKER_COMMAND_MAX_ATTEMPTS {
                    return Err(WorkerCommandError {
                        worker_id: worker.id.clone(),
                        command: command_name,
                        url,
                        request_id: context.request_id.clone(),
                        attempts: attempt,
                        classification: WorkerCommandErrorClass::TransportRetryable,
                        last_error: err_text,
                        diagnostic: Some(diagnostic),
                    }
                    .into());
                }

                retry_worker_command_sleep(
                    worker,
                    command_name,
                    attempt,
                    &mut delay,
                    &url,
                    &format!("{} ({})", err_text, diagnostic),
                )
                .await;
                continue;
            }
            Err(err) => {
                return Err(WorkerCommandError {
                    worker_id: worker.id.clone(),
                    command: command_name,
                    url,
                    request_id: context.request_id.clone(),
                    attempts: attempt,
                    classification: WorkerCommandErrorClass::FatalDecode,
                    last_error: err.to_string(),
                    diagnostic: Some(reqwest_error_diagnostic(&err)),
                }
                .into());
            }
        }
    }

    unreachable!("worker command retry loop always returns")
}

async fn send_command(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
) -> Result<CommandResponse> {
    let context = WorkerCommandContext::new(worker, command);
    send_command_with_context(http, worker, command, &context).await
}

fn worker_command_next_delay(delay: Duration) -> Duration {
    let doubled_ms = delay.as_millis().saturating_mul(2);
    let max_ms = WORKER_COMMAND_MAX_DELAY.as_millis();
    Duration::from_millis(doubled_ms.min(max_ms) as u64)
}

fn worker_command_with_jitter(delay: Duration) -> Duration {
    let base_ms = delay.as_millis() as u64;
    let jitter_cap_ms = (base_ms / 10).clamp(1, 100);
    let jitter_ms = thread_rng().gen_range(0..=jitter_cap_ms);
    Duration::from_millis(base_ms + jitter_ms)
}

async fn send_cmd_expect_ok_fragment(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
) -> Result<String> {
    let context = WorkerCommandContext::new(worker, command);
    send_cmd_expect_ok_fragment_with_context(http, worker, command, ok_fragment, &context).await
}

async fn send_cmd_expect_ok_fragment_with_context(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
    context: &WorkerCommandContext,
) -> Result<String> {
    let response = send_command_with_context(http, worker, command, context).await?;

    match response.status.as_str() {
        "ok" if response.message.contains(ok_fragment) => Ok(response.message),
        "ok" => Err(anyhow!(
            "Worker {} returned unexpected ok message: {}",
            worker.id,
            response.message
        )),
        "error" => Err(anyhow!("Worker {} error: {}", worker.id, response.message)),
        other => Err(anyhow!(
            "Worker {} returned unknown status '{}': {}",
            worker.id,
            other,
            response.message
        )),
    }
}

async fn send_cmd_until_ok(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
    retryable_error_fragment: &str,
    timeout: Duration,
) -> Result<String> {
    let start = Instant::now();

    while start.elapsed() < timeout {
        let response = send_command(http, worker, command).await?;

        match response.status.as_str() {
            "ok" if response.message.contains(ok_fragment) => return Ok(response.message),
            "ok" => {
                return Err(anyhow!(
                    "Worker {} returned unexpected ok message: {}",
                    worker.id,
                    response.message
                ));
            }
            "error" if response.message.contains(retryable_error_fragment) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            "error" => {
                return Err(anyhow!("Worker {} error: {}", worker.id, response.message));
            }
            other => {
                return Err(anyhow!(
                    "Worker {} returned unknown status '{}': {}",
                    worker.id,
                    other,
                    response.message
                ));
            }
        }
    }

    Err(anyhow!(
        "Timeout waiting for ok fragment '{}' from worker {}",
        ok_fragment,
        worker.id
    ))
}

fn parse_group_state_message(message: &str) -> Result<GroupStateSnapshot> {
    let msg = message
        .strip_prefix("group_id=")
        .ok_or_else(|| anyhow!("Unexpected show_group_state message: {}", message))?;

    let (group_id, rest) = msg
        .split_once(", epoch=")
        .ok_or_else(|| anyhow!("Missing epoch in show_group_state message: {}", message))?;

    let (epoch_str, members_str) = rest
        .split_once(", members=")
        .ok_or_else(|| anyhow!("Missing members in show_group_state message: {}", message))?;

    let epoch = epoch_str
        .parse::<u64>()
        .with_context(|| format!("Invalid epoch '{}' in '{}'", epoch_str, message))?;

    let mut members: Vec<String> = serde_json::from_str(members_str)
        .with_context(|| format!("Invalid members list '{}' in '{}'", members_str, message))?;

    members.sort();

    Ok(GroupStateSnapshot {
        group_id: group_id.to_string(),
        epoch,
        members,
    })
}

async fn show_group_state(
    http: &reqwest::Client,
    worker: &WorkerSpec,
) -> Result<GroupStateSnapshot> {
    let message =
        send_cmd_expect_ok_fragment(http, worker, &Command::ShowGroupState, "group_id=").await?;
    parse_group_state_message(&message)
}

fn expected_group_state(
    reference: &GroupStateSnapshot,
    epoch: u64,
    mut members: Vec<String>,
) -> ExpectedGroupState {
    members.sort();
    ExpectedGroupState {
        group_id: reference.group_id.clone(),
        epoch,
        members,
    }
}

fn expected_epoch(expected: &ExpectedReceiveCommitState) -> Option<u64> {
    match expected {
        ExpectedReceiveCommitState::Group(state) => Some(state.epoch),
        ExpectedReceiveCommitState::Removed { expected_epoch } => Some(*expected_epoch),
    }
}

async fn receive_commit_reconciled_by_state(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    expected: &ExpectedReceiveCommitState,
    original_error: &str,
) -> Result<bool> {
    match show_group_state(http, worker).await {
        Ok(snapshot) => match expected {
            ExpectedReceiveCommitState::Group(expected_state) if expected_state.matches(&snapshot) => {
                eprintln!(
                    "receive_commit ambiguous transport error reconciled by epoch check: worker={} expected_epoch={} original_error={}",
                    worker.id, expected_state.epoch, original_error
                );
                Ok(true)
            }
            ExpectedReceiveCommitState::Group(expected_state) if snapshot.epoch < expected_state.epoch => {
                eprintln!(
                    "[reconcile] receive_commit worker={} is behind after ambiguous transport error: current_epoch={} expected_epoch={}",
                    worker.id, snapshot.epoch, expected_state.epoch
                );
                Ok(false)
            }
            ExpectedReceiveCommitState::Group(expected_state) => Err(anyhow!(
                "receive_commit ambiguous transport error could not be reconciled for worker {}: expected group_id={} epoch={} members={:?}; got group_id={} epoch={} members={:?}; original_error={}",
                worker.id,
                expected_state.group_id,
                expected_state.epoch,
                expected_state.members,
                snapshot.group_id,
                snapshot.epoch,
                snapshot.members,
                original_error
            )),
            ExpectedReceiveCommitState::Removed { expected_epoch } => Err(anyhow!(
                "receive_commit ambiguous transport error could not be reconciled for removed worker {}: expected removed after epoch {}, but ShowGroupState still returned group_id={} epoch={} members={:?}; original_error={}",
                worker.id,
                expected_epoch,
                snapshot.group_id,
                snapshot.epoch,
                snapshot.members,
                original_error
            )),
        },
        Err(err) => {
            let text = format!("{:#}", err);
            if matches!(expected, ExpectedReceiveCommitState::Removed { .. })
                && text.contains("Client is not in a group")
            {
                eprintln!(
                    "receive_commit ambiguous transport error reconciled by removed-state check: worker={} original_error={}",
                    worker.id, original_error
                );
                return Ok(true);
            }

            Err(anyhow!(
                "receive_commit ambiguous transport error reconciliation could not query worker {} state: {}; original_error={}",
                worker.id,
                text,
                original_error
            ))
        }
    }
}

async fn receive_commit_expect(
    http: &reqwest::Client,
    worker: &WorkerSpec,
    ok_fragment: &str,
    expected: ExpectedReceiveCommitState,
    phase: &str,
) -> Result<()> {
    let command = Command::ReceiveCommit;
    let context = WorkerCommandContext::with_metadata(
        worker,
        &command,
        expected_epoch(&expected),
        Some(phase),
    );

    match send_cmd_expect_ok_fragment_with_context(http, worker, &command, ok_fragment, &context)
        .await
    {
        Ok(_) => return Ok(()),
        Err(err) => {
            let is_ambiguous = err
                .downcast_ref::<WorkerCommandError>()
                .map(WorkerCommandError::is_ambiguous_transport)
                .unwrap_or(false);

            if !is_ambiguous {
                return Err(err);
            }

            let original_error = format!("{:#}", err);
            if receive_commit_reconciled_by_state(http, worker, &expected, &original_error).await? {
                return Ok(());
            }

            eprintln!(
                "[reconcile] receive_commit retrying same request_id={} worker={} phase={} after behind-state check",
                context.request_id, worker.id, phase
            );

            match send_cmd_expect_ok_fragment_with_context(
                http,
                worker,
                &command,
                ok_fragment,
                &context,
            )
            .await
            {
                Ok(_) => Ok(()),
                Err(retry_err) => {
                    let retry_error = format!("{:#}", retry_err);
                    if receive_commit_reconciled_by_state(http, worker, &expected, &retry_error)
                        .await?
                    {
                        return Ok(());
                    }

                    Err(anyhow!(
                        "receive_commit failed after ambiguous transport reconciliation retry: worker={} request_id={} phase={} initial_error={} retry_error={}",
                        worker.id,
                        context.request_id,
                        phase,
                        original_error,
                        retry_error
                    ))
                }
            }
        }
    }
}

async fn ensure_converged(
    http: &reqwest::Client,
    active_workers: &[WorkerSpec],
    expected_active_ids: &[String],
    max_parallelism: usize,
) -> Result<GroupStateSnapshot> {
    if active_workers.is_empty() {
        return Err(anyhow!("No active workers to verify"));
    }

    let mut expected_members = expected_active_ids.to_vec();
    expected_members.sort();

    let started = Instant::now();
    let states = collect_worker_group_states(http, active_workers, max_parallelism).await?;
    let (reference_worker, reference) = states
        .first()
        .ok_or_else(|| anyhow!("No active workers to verify"))?;

    if reference.members != expected_members {
        return Err(anyhow!(
            "Reference worker {} member list mismatch. Expected {:?}, got {:?}",
            reference_worker.id,
            expected_members,
            reference.members
        ));
    }

    for (worker, state) in states.iter().skip(1) {
        if state.group_id != reference.group_id
            || state.epoch != reference.epoch
            || state.members != reference.members
        {
            return Err(anyhow!(
                "Convergence mismatch on worker {}. Expected group_id={}, epoch={}, members={:?}; got group_id={}, epoch={}, members={:?}",
                worker.id,
                reference.group_id,
                reference.epoch,
                reference.members,
                state.group_id,
                state.epoch,
                state.members
            ));
        }
    }

    emit_network_metrics(NetworkPhaseMetrics {
        phase: "convergence".to_string(),
        group_size: active_workers.len(),
        operation: "show_group_state".to_string(),
        request_count: active_workers.len(),
        recipient_count: active_workers.len(),
        max_parallelism: max_parallelism.max(1),
        wall_ms: started.elapsed().as_millis(),
        retry_count: 0,
        retry_sleep_ms: 0,
        failures: 0,
        worker_latency_p50_ms: None,
        worker_latency_p95_ms: None,
        worker_latency_p99_ms: None,
        worker_latency_max_ms: None,
    });

    Ok(reference.clone())
}

async fn collect_worker_group_states(
    http: &reqwest::Client,
    workers: &[WorkerSpec],
    max_parallelism: usize,
) -> Result<Vec<(WorkerSpec, GroupStateSnapshot)>> {
    let max_parallelism = max_parallelism.max(1);
    let mut states = Vec::with_capacity(workers.len());

    for chunk in workers.chunks(max_parallelism) {
        let futures = chunk.iter().cloned().map(|worker| async move {
            let state = show_group_state(http, &worker).await?;
            Ok::<_, anyhow::Error>((worker, state))
        });

        states.extend(try_join_all(futures).await?);
    }

    Ok(states)
}

fn latency_percentiles(
    mut latencies: Vec<u128>,
) -> (Option<u128>, Option<u128>, Option<u128>, Option<u128>) {
    if latencies.is_empty() {
        return (None, None, None, None);
    }

    latencies.sort_unstable();
    let max = latencies.last().copied();

    let percentile = |pct: usize| -> Option<u128> {
        let len = latencies.len();
        let idx = ((len.saturating_sub(1)) * pct).div_ceil(100);
        latencies.get(idx).copied()
    };

    (percentile(50), percentile(95), percentile(99), max)
}

async fn fanout_workers<F, Fut>(
    phase: &str,
    group_size: usize,
    operation: &str,
    workers: &[WorkerSpec],
    fanout: &mut FanoutController,
    op: F,
) -> Result<()>
where
    F: Fn(WorkerSpec) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let started = Instant::now();
    let max_parallelism = fanout.parallelism();
    let mut failures = Vec::new();
    let mut latencies = Vec::with_capacity(workers.len());

    for chunk in workers.chunks(max_parallelism) {
        let futures = chunk.iter().cloned().map(|worker| {
            let worker_for_result = worker.clone();
            let future = op(worker);
            async move {
                let command_started = Instant::now();
                let result = future.await;
                (
                    worker_for_result,
                    command_started.elapsed().as_millis(),
                    result,
                )
            }
        });

        for (worker, latency_ms, result) in join_all(futures).await {
            latencies.push(latency_ms);

            if let Err(err) = result {
                failures.push((worker, err));
            }
        }
    }

    let (p50, p95, p99, max) = latency_percentiles(latencies);
    let summary = FanoutSummary {
        request_count: workers.len(),
        failures: failures.len(),
        wall_ms: started.elapsed().as_millis(),
        latency_p50_ms: p50,
        latency_p95_ms: p95,
        latency_p99_ms: p99,
        latency_max_ms: max,
    };

    emit_network_metrics(NetworkPhaseMetrics {
        phase: phase.to_string(),
        group_size,
        operation: operation.to_string(),
        request_count: summary.request_count,
        recipient_count: workers.len(),
        max_parallelism,
        wall_ms: summary.wall_ms,
        retry_count: 0,
        retry_sleep_ms: 0,
        failures: summary.failures,
        worker_latency_p50_ms: summary.latency_p50_ms,
        worker_latency_p95_ms: summary.latency_p95_ms,
        worker_latency_p99_ms: summary.latency_p99_ms,
        worker_latency_max_ms: summary.latency_max_ms,
    });
    fanout.record(phase, operation, &summary);

    if failures.is_empty() {
        return Ok(());
    }

    let details = failures
        .iter()
        .map(|(worker, err)| format!("{}: {:#}", worker.id, err))
        .collect::<Vec<_>>()
        .join("; ");

    Err(anyhow!(
        "fanout phase={} operation={} failed_workers={} max_parallelism={} failures=[{}]",
        phase,
        operation,
        failures.len(),
        max_parallelism,
        details
    ))
}

fn emit_network_metrics(metrics: NetworkPhaseMetrics) {
    match serde_json::to_string(&metrics) {
        Ok(json) => eprintln!("[network-metrics] {}", json),
        Err(err) => eprintln!("[network-metrics] serialization_error={}", err),
    }
}

fn stepped_sizes(min_size: usize, max_size: usize, step_size: usize) -> Vec<usize> {
    let mut sizes = Vec::new();
    let mut current = min_size;

    sizes.push(current);
    while current < max_size {
        let next = current.saturating_add(step_size);
        current = next.min(max_size);
        if sizes.last().copied() != Some(current) {
            sizes.push(current);
        }
    }

    sizes
}

fn build_plateau_sequence(
    min_size: usize,
    max_size: usize,
    step_size: usize,
    roundtrips: usize,
) -> Vec<usize> {
    let ascent = stepped_sizes(min_size, max_size, step_size);
    let mut sequence = Vec::new();

    for _ in 0..roundtrips {
        for &size in &ascent {
            if sequence.last().copied() != Some(size) {
                sequence.push(size);
            }
        }
        for &size in ascent.iter().rev().skip(1) {
            if sequence.last().copied() != Some(size) {
                sequence.push(size);
            }
        }
    }

    sequence
}

fn cap_count(raw: usize, cap: usize) -> usize {
    if cap == 0 {
        0
    } else {
        raw.min(cap)
    }
}

fn update_ops_for_plateau(
    size: usize,
    update_rounds: usize,
    max_update_samples_per_plateau: usize,
) -> usize {
    cap_count(
        update_rounds.saturating_mul(size),
        max_update_samples_per_plateau,
    )
}

fn app_sends_per_payload_for_plateau(
    size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
) -> usize {
    if size < 2 {
        0
    } else {
        cap_count(app_rounds.saturating_mul(size), max_app_samples_per_payload)
    }
}

fn app_ops_for_plateau(
    size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
    payload_count: usize,
) -> usize {
    app_sends_per_payload_for_plateau(size, app_rounds, max_app_samples_per_payload)
        .saturating_mul(payload_count)
}

fn sampled_member_index(member_count: usize, sample_count: usize, seq_no: usize) -> usize {
    assert!(member_count > 0, "member_count must be greater than zero");
    assert!(sample_count > 0, "sample_count must be greater than zero");

    if sample_count >= member_count {
        return seq_no % member_count;
    }

    let sample_no = seq_no % sample_count;
    let one_based_index =
        ((sample_no + 1) as u128 * member_count as u128 / sample_count as u128) as usize;

    // Pick the right edge of each equal bucket: e.g. 20 / 4 => 5, 10, 15, 20.
    one_based_index.saturating_sub(1)
}

fn estimate_total_units(
    plateau_sequence: &[usize],
    update_rounds: usize,
    app_rounds: usize,
    max_update_samples_per_plateau: usize,
    max_app_samples_per_payload: usize,
    payload_count: usize,
) -> usize {
    let mut total = 1usize;
    let mut current_size = 1usize;

    for &target in plateau_sequence {
        total = total.saturating_add(target.abs_diff(current_size));
        total = total.saturating_add(update_ops_for_plateau(
            target,
            update_rounds,
            max_update_samples_per_plateau,
        ));
        total = total.saturating_add(app_ops_for_plateau(
            target,
            app_rounds,
            max_app_samples_per_payload,
            payload_count,
        ));
        current_size = target;
    }

    total
}

fn deterministic_payload(
    len: usize,
    plateau_size: usize,
    payload_size: usize,
    seq_no: usize,
    actor_id: &str,
) -> String {
    if len == 0 {
        return String::new();
    }

    let seed = format!(
        "plateau={};payload={};seq={};actor={};",
        plateau_size, payload_size, seq_no, actor_id
    );

    let mut out = String::with_capacity(len);
    while out.len() < len {
        out.push_str(&seed);
    }
    out.truncate(len);
    out
}

async fn create_group(
    http: &reqwest::Client,
    leader: &WorkerSpec,
    progress: &mut Progress,
) -> Result<()> {
    send_cmd_expect_ok_fragment(
        http,
        leader,
        &Command::CreateGroup,
        "group created and DS group state registered",
    )
    .await?;
    progress.tick("create_group");
    Ok(())
}

async fn add_one_member(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    progress: &mut Progress,
) -> Result<()> {
    let timeout = Duration::from_secs(30);

    if active.is_empty() {
        return Err(anyhow!(
            "No active group member available to add new member"
        ));
    }

    let actor_idx = thread_rng().gen_range(0..active.len());
    let actor = active[actor_idx].clone();

    let joiner = idle
        .pop_front()
        .ok_or_else(|| anyhow!("No idle worker available to add"))?;

    let actor_before = show_group_state(http, &actor).await?;
    let mut expected_members = actor_before.members.clone();
    expected_members.push(joiner.id.clone());
    let expected_after_commit =
        expected_group_state(&actor_before, actor_before.epoch + 1, expected_members);

    let fragment = format!("key package uploaded for {}", joiner.id);
    send_cmd_expect_ok_fragment(http, &joiner, &Command::GenerateKeyPackage, &fragment).await?;

    send_cmd_expect_ok_fragment(
        http,
        &actor,
        &Command::AddMembers {
            members: vec![joiner.id.clone()],
        },
        "added locally in one commit",
    )
    .await?;
    receive_commit_expect(
        http,
        &actor,
        "own commit accepted from DS",
        ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
        "add_member.actor_receive_commit",
    )
    .await?;

    let join_fragment = format!("{} joined from welcome", joiner.id);
    send_cmd_until_ok(
        http,
        &joiner,
        &Command::JoinFromWelcome,
        &join_fragment,
        "404 Not Found",
        timeout,
    )
    .await?;

    let recipients: Vec<WorkerSpec> = active
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != actor_idx)
        .map(|(_, worker)| worker.clone())
        .collect();
    fanout_workers(
        "add_member",
        active.len(),
        "receive_commit",
        &recipients,
        fanout,
        |worker| {
            let expected = ExpectedReceiveCommitState::Group(expected_after_commit.clone());
            async move {
                receive_commit_expect(
                    http,
                    &worker,
                    "external commit received and processed",
                    expected,
                    "add_member.fanout_receive_commit",
                )
                .await
            }
        },
    )
    .await?;

    active.push(joiner.clone());
    progress.tick(&format!("add {} actor={}", joiner.id, actor.id));
    Ok(())
}

async fn remove_one_member(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    fanout: &mut FanoutController,
    progress: &mut Progress,
) -> Result<()> {
    if active.len() <= 1 {
        return Err(anyhow!("Cannot remove the last remaining member"));
    }

    let mut rng = thread_rng();

    let actor_idx = rng.gen_range(0..active.len());
    let actor = active[actor_idx].clone();

    // Pick a random member to remove, but do not let the actor remove itself.
    // Self-removal is possible in MLS-style flows, but it complicates this benchmark's bookkeeping.
    let mut removed_idx = rng.gen_range(0..active.len() - 1);
    if removed_idx >= actor_idx {
        removed_idx += 1;
    }

    let removed = active[removed_idx].clone();
    let actor_before = show_group_state(http, &actor).await?;
    let expected_members = actor_before
        .members
        .iter()
        .filter(|member| *member != &removed.id)
        .cloned()
        .collect::<Vec<_>>();
    let expected_after_commit =
        expected_group_state(&actor_before, actor_before.epoch + 1, expected_members);

    send_cmd_expect_ok_fragment(
        http,
        &actor,
        &Command::RemoveMembers {
            members: vec![removed.id.clone()],
        },
        "removed locally; group commit published",
    )
    .await?;

    receive_commit_expect(
        http,
        &actor,
        "own commit accepted from DS",
        ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
        "remove_member.actor_receive_commit",
    )
    .await?;

    let recipients: Vec<WorkerSpec> = active
        .iter()
        .filter(|worker| worker.id != actor.id)
        .cloned()
        .collect();
    fanout_workers(
        "remove_member",
        active.len(),
        "receive_commit",
        &recipients,
        fanout,
        |worker| {
            let expected = if worker.id == removed.id {
                ExpectedReceiveCommitState::Removed {
                    expected_epoch: expected_after_commit.epoch,
                }
            } else {
                ExpectedReceiveCommitState::Group(expected_after_commit.clone())
            };

            async move {
                receive_commit_expect(
                    http,
                    &worker,
                    "external commit received and processed",
                    expected,
                    "remove_member.fanout_receive_commit",
                )
                .await
            }
        },
    )
    .await?;

    let actually_removed = active.remove(removed_idx);
    idle.push_front(actually_removed.clone());

    progress.tick(&format!(
        "remove {} actor={}",
        actually_removed.id, actor.id
    ));
    Ok(())
}

async fn transition_to_size(
    http: &reqwest::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    target_size: usize,
    fanout: &mut FanoutController,
    progress: &mut Progress,
) -> Result<()> {
    while active.len() < target_size {
        add_one_member(http, active, idle, fanout, progress).await?;
    }

    while active.len() > target_size {
        remove_one_member(http, active, idle, fanout, progress).await?;
    }

    Ok(())
}

async fn run_update_phase(
    http: &reqwest::Client,
    active: &[WorkerSpec],
    plateau_size: usize,
    update_rounds: usize,
    max_update_samples_per_plateau: usize,
    fanout: &mut FanoutController,
    progress: &mut Progress,
) -> Result<()> {
    let total_updates =
        update_ops_for_plateau(plateau_size, update_rounds, max_update_samples_per_plateau);
    if total_updates == 0 {
        return Ok(());
    }

    eprintln!(
        "\n[plateau {}] update phase: {} successful self-update cycles",
        plateau_size, total_updates
    );

    for seq_no in 0..total_updates {
        let actor_idx = sampled_member_index(active.len(), total_updates, seq_no);
        let actor = &active[actor_idx];
        let actor_before = show_group_state(http, actor).await?;
        let expected_after_commit = expected_group_state(
            &actor_before,
            actor_before.epoch + 1,
            actor_before.members.clone(),
        );

        send_cmd_expect_ok_fragment(
            http,
            actor,
            &Command::SelfUpdate,
            "self_update commit published to group",
        )
        .await?;

        receive_commit_expect(
            http,
            actor,
            "own commit accepted from DS",
            ExpectedReceiveCommitState::Group(expected_after_commit.clone()),
            "update.actor_receive_commit",
        )
        .await?;

        let recipients: Vec<WorkerSpec> = active
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != actor_idx)
            .map(|(_, worker)| worker.clone())
            .collect();
        fanout_workers(
            "update",
            plateau_size,
            "receive_commit",
            &recipients,
            fanout,
            |worker| {
                let expected = ExpectedReceiveCommitState::Group(expected_after_commit.clone());
                async move {
                    receive_commit_expect(
                        http,
                        &worker,
                        "external commit received and processed",
                        expected,
                        "update.fanout_receive_commit",
                    )
                    .await
                }
            },
        )
        .await?;

        progress.tick(&format!(
            "plateau {} update {}/{} actor={}",
            plateau_size,
            seq_no + 1,
            total_updates,
            actor.id
        ));
    }

    Ok(())
}

async fn run_application_phase(
    http: &reqwest::Client,
    active: &[WorkerSpec],
    plateau_size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
    payload_sizes: &[usize],
    fanout: &mut FanoutController,
    progress: &mut Progress,
) -> Result<()> {
    if active.len() < 2 {
        eprintln!(
            "\n[plateau {}] application phase skipped: fewer than 2 active members",
            plateau_size
        );
        return Ok(());
    }

    let per_payload_count =
        app_sends_per_payload_for_plateau(plateau_size, app_rounds, max_app_samples_per_payload);
    if per_payload_count == 0 {
        return Ok(());
    }

    for &payload_size in payload_sizes {
        eprintln!(
            "\n[plateau {}] application phase: {} successful sends at payload {} B",
            plateau_size, per_payload_count, payload_size
        );

        for seq_no in 0..per_payload_count {
            let actor_idx = sampled_member_index(active.len(), per_payload_count, seq_no);
            let actor = &active[actor_idx];
            let payload =
                deterministic_payload(payload_size, plateau_size, payload_size, seq_no, &actor.id);

            send_cmd_expect_ok_fragment(
                http,
                actor,
                &Command::SendApplicationMessage { message: payload },
                "application message broadcast to group",
            )
            .await?;

            let recipient_indices: Vec<usize> =
                (0..active.len()).filter(|&j| j != actor_idx).collect();

            let sampled_pos =
                sampled_member_index(recipient_indices.len(), per_payload_count, seq_no);

            let recipients: Vec<(usize, WorkerSpec)> = recipient_indices
                .iter()
                .enumerate()
                .map(|(pos, recipient_idx)| (pos, active[*recipient_idx].clone()))
                .collect();
            let sampled_worker_id = recipients[sampled_pos].1.id.clone();
            let recipient_workers: Vec<WorkerSpec> =
                recipients.into_iter().map(|(_, worker)| worker).collect();

            fanout_workers(
                "application",
                plateau_size,
                "receive_application_message",
                &recipient_workers,
                fanout,
                |worker| {
                    let sampled_worker_id = sampled_worker_id.clone();
                    async move {
                        let profile = worker.id == sampled_worker_id;
                        send_cmd_expect_ok_fragment(
                            http,
                            &worker,
                            &Command::ReceiveApplicationMessage { profile },
                            "application message received:",
                        )
                        .await
                        .map(|_| ())
                    }
                },
            )
            .await?;

            progress.tick(&format!(
                "plateau {} app payload={} {}/{} actor={}",
                plateau_size,
                payload_size,
                seq_no + 1,
                per_payload_count,
                actor.id
            ));
        }
    }

    Ok(())
}

fn aggregate_csv(run_dir: &Path, worker_ids: &[String]) -> Result<()> {
    let csv_path = run_dir.join("events.csv");
    let mut wtr = csv::Writer::from_path(&csv_path)?;

    #[derive(Serialize)]
    struct CsvRow<'a> {
        worker_id: &'a str,
        ts_unix_ns: u128,
        op: String,
        implementation: String,
        wall_ns: u128,
        cpu_thread_ns: Option<u128>,
        alloc_bytes: Option<u64>,
        alloc_count: Option<u64>,
        artifact_size_bytes: Option<usize>,
        encrypted_group_info_bytes: Option<usize>,
        encrypted_secrets_count: Option<usize>,
        group_epoch: Option<u64>,
        tree_size: Option<u32>,
        member_count: Option<usize>,
        invitee_count: Option<isize>,
        ciphersuite: Option<String>,
        app_msg_plaintext_bytes: Option<usize>,
        app_msg_padding_bytes: Option<usize>,
        app_msg_ciphertext_bytes: Option<usize>,
        aad_bytes: Option<usize>,
        pid: u32,
        thread_id: String,
        run_id: Option<String>,
        scenario: Option<String>,
        node_name: Option<String>,
        pod_name: Option<String>,
    }

    for worker_id in worker_ids {
        let path = run_dir.join(format!("client-{worker_id}.jsonl"));
        if !path.exists() {
            continue;
        }

        let file = File::open(&path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            let event: ProfileEvent = serde_json::from_str(&line)
                .with_context(|| format!("Invalid json in {}", path.display()))?;

            let row = CsvRow {
                worker_id,
                ts_unix_ns: event.ts_unix_ns,
                op: event.op,
                implementation: event.implementation,
                wall_ns: event.wall_ns,
                cpu_thread_ns: event.cpu_thread_ns,
                alloc_bytes: event.alloc_bytes,
                alloc_count: event.alloc_count,
                artifact_size_bytes: event.artifact_size_bytes,
                encrypted_group_info_bytes: event.encrypted_group_info_bytes,
                encrypted_secrets_count: event.encrypted_secrets_count,
                group_epoch: event.group_epoch,
                tree_size: event.tree_size,
                member_count: event.member_count,
                invitee_count: event.invitee_count,
                ciphersuite: event.ciphersuite,
                app_msg_plaintext_bytes: event.app_msg_plaintext_bytes,
                app_msg_padding_bytes: event.app_msg_padding_bytes,
                app_msg_ciphertext_bytes: event.app_msg_ciphertext_bytes,
                aad_bytes: event.aad_bytes,
                pid: event.pid,
                thread_id: event.thread_id,
                run_id: event.run_id,
                scenario: event.scenario,
                node_name: event.node_name,
                pod_name: event.pod_name,
            };

            wtr.serialize(row)?;
        }
    }

    wtr.flush()?;
    Ok(())
}

fn validate_config(config: &StaircaseConfig, worker_count: usize) -> Result<usize> {
    if config.min_size == 0 {
        return Err(anyhow!("--min-size must be at least 1"));
    }
    if config.step_size == 0 {
        return Err(anyhow!("--step-size must be at least 1"));
    }
    if config.roundtrips == 0 {
        return Err(anyhow!("--roundtrips must be at least 1"));
    }
    if config.payload_sizes.is_empty() {
        return Err(anyhow!("At least one payload size is required"));
    }

    let max_size = config.max_size.unwrap_or(worker_count);

    if max_size == 0 {
        return Err(anyhow!("--max-size must be at least 1"));
    }
    if max_size > worker_count {
        return Err(anyhow!(
            "--max-size {} exceeds number of supplied workers {}",
            max_size,
            worker_count
        ));
    }
    if config.min_size > max_size {
        return Err(anyhow!(
            "--min-size {} cannot exceed --max-size {}",
            config.min_size,
            max_size
        ));
    }

    Ok(max_size)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use anyhow::anyhow;

    use super::{fanout_workers, sampled_member_index, FanoutController, WorkerSpec};

    fn sampled_indices(member_count: usize, sample_count: usize) -> Vec<usize> {
        (0..sample_count)
            .map(|seq_no| sampled_member_index(member_count, sample_count, seq_no))
            .collect()
    }

    #[test]
    fn samples_every_member_when_sample_count_covers_group() {
        assert_eq!(sampled_indices(16, 16), (0..16).collect::<Vec<_>>());
        assert_eq!(
            sampled_indices(5, 16),
            vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0]
        );
    }

    #[test]
    fn samples_equal_bucket_right_edges_when_group_exceeds_sample_count() {
        assert_eq!(sampled_indices(20, 4), vec![4, 9, 14, 19]);
        assert_eq!(sampled_indices(100, 4), vec![24, 49, 74, 99]);
    }

    #[test]
    fn includes_last_member_when_sampling_large_group() {
        let indices = sampled_indices(100, 16);

        assert_eq!(indices.last(), Some(&99));
        assert!(indices.iter().all(|&idx| idx < 100));
        assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn fanout_attempts_all_workers_before_returning_failures() {
        let workers = (1..=4)
            .map(|idx| WorkerSpec {
                id: format!("{idx:05}"),
                url: format!("http://worker-{idx:05}:8080"),
            })
            .collect::<Vec<_>>();

        let attempts = Arc::new(AtomicUsize::new(0));
        let mut fanout = FanoutController::new(4, false);
        let attempts_for_op = attempts.clone();

        let result = fanout_workers(
            "test",
            workers.len(),
            "synthetic",
            &workers,
            &mut fanout,
            move |worker| {
                let attempts = attempts_for_op.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if worker.id == "00002" {
                        Err(anyhow!("synthetic transient failure"))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), workers.len());
    }
}
