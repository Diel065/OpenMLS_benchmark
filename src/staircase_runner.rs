use std::{
    collections::VecDeque,
    fs::{self, File},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

use crate::worker_api::{Command, CommandResponse};

#[derive(Debug, Clone)]
pub struct StaircaseConfig {
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
    let max_size = validate_config(&config, config.workers.len())?;

    let run_dir = run_dir_for(&config.output_dir, &config.run_id);
    fs::create_dir_all(&run_dir)?;

    let http = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("Failed to build HTTP client")?;

    wait_for_health(&http, &config.ds_url, Duration::from_secs(10))
        .with_context(|| format!("DS at {} is not healthy", config.ds_url))?;

    for worker in &config.workers {
        wait_for_health(&http, &worker.url, Duration::from_secs(10))
            .with_context(|| format!("Worker {} at {} is not healthy", worker.id, worker.url))?;
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

    create_group(&http, &leader, &mut progress)?;
    let active_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
    let initial_state = ensure_converged(&http, &active, &active_ids)?;
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

        transition_to_size(&http, &mut active, &mut idle, target_size, &mut progress)?;

        let active_ids: Vec<String> = active.iter().map(|w| w.id.clone()).collect();
        let state = ensure_converged(&http, &active, &active_ids)?;
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
            &mut progress,
        )?;

        let state_after_updates = ensure_converged(&http, &active, &active_ids)?;
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
            &mut progress,
        )?;

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

fn wait_for_health(
    http: &reqwest::blocking::Client,
    base_url: &str,
    timeout: Duration,
) -> Result<()> {
    let url = format!("{}/health", base_url.trim_end_matches('/'));
    let start = Instant::now();

    while start.elapsed() < timeout {
        match http.get(&url).send() {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            _ => thread::sleep(Duration::from_millis(250)),
        }
    }

    Err(anyhow!("Timeout waiting for health endpoint at {}", url))
}

fn send_command(
    http: &reqwest::blocking::Client,
    worker: &WorkerSpec,
    command: &Command,
) -> Result<CommandResponse> {
    let url = format!("{}/command", worker.url);

    let response = http
        .post(&url)
        .json(command)
        .send()
        .with_context(|| format!("Failed to POST command to worker {}", worker.id))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().unwrap_or_default();
        return Err(anyhow!(
            "Worker {} returned HTTP {}: {}",
            worker.id,
            status,
            body
        ));
    }

    let parsed: CommandResponse = response
        .json()
        .with_context(|| format!("Failed to decode JSON response from worker {}", worker.id))?;

    Ok(parsed)
}

fn send_cmd_expect_ok_fragment(
    http: &reqwest::blocking::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
) -> Result<String> {
    let response = send_command(http, worker, command)?;

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

fn send_cmd_until_ok(
    http: &reqwest::blocking::Client,
    worker: &WorkerSpec,
    command: &Command,
    ok_fragment: &str,
    retryable_error_fragment: &str,
    timeout: Duration,
) -> Result<String> {
    let start = Instant::now();

    while start.elapsed() < timeout {
        let response = send_command(http, worker, command)?;

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
                thread::sleep(Duration::from_millis(500));
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

fn show_group_state(
    http: &reqwest::blocking::Client,
    worker: &WorkerSpec,
) -> Result<GroupStateSnapshot> {
    let message = send_cmd_expect_ok_fragment(http, worker, &Command::ShowGroupState, "group_id=")?;
    parse_group_state_message(&message)
}

fn ensure_converged(
    http: &reqwest::blocking::Client,
    active_workers: &[WorkerSpec],
    expected_active_ids: &[String],
) -> Result<GroupStateSnapshot> {
    if active_workers.is_empty() {
        return Err(anyhow!("No active workers to verify"));
    }

    let mut expected_members = expected_active_ids.to_vec();
    expected_members.sort();

    let reference = show_group_state(http, &active_workers[0])?;

    if reference.members != expected_members {
        return Err(anyhow!(
            "Reference worker {} member list mismatch. Expected {:?}, got {:?}",
            active_workers[0].id,
            expected_members,
            reference.members
        ));
    }

    for worker in active_workers.iter().skip(1) {
        let state = show_group_state(http, worker)?;
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

    Ok(reference)
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

fn create_group(
    http: &reqwest::blocking::Client,
    leader: &WorkerSpec,
    progress: &mut Progress,
) -> Result<()> {
    send_cmd_expect_ok_fragment(
        http,
        leader,
        &Command::CreateGroup,
        "group created and DS group state registered",
    )?;
    progress.tick("create_group");
    Ok(())
}

fn add_one_member(
    http: &reqwest::blocking::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
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

    let fragment = format!("key package uploaded for {}", joiner.id);
    send_cmd_expect_ok_fragment(http, &joiner, &Command::GenerateKeyPackage, &fragment)?;

    send_cmd_expect_ok_fragment(
        http,
        &actor,
        &Command::AddMembers {
            members: vec![joiner.id.clone()],
        },
        "added locally in one commit",
    )?;
    send_cmd_expect_ok_fragment(
        http,
        &actor,
        &Command::ReceiveCommit,
        "own commit accepted from DS",
    )?;

    let join_fragment = format!("{} joined from welcome", joiner.id);
    send_cmd_until_ok(
        http,
        &joiner,
        &Command::JoinFromWelcome,
        &join_fragment,
        "404 Not Found",
        timeout,
    )?;

    for (idx, other) in active.iter().enumerate() {
        if idx == actor_idx {
            continue;
        }

        send_cmd_expect_ok_fragment(
            http,
            other,
            &Command::ReceiveCommit,
            "external commit received and processed",
        )?;
    }

    active.push(joiner.clone());
    progress.tick(&format!("add {} actor={}", joiner.id, actor.id));
    Ok(())
}

fn remove_one_member(
    http: &reqwest::blocking::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
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

    send_cmd_expect_ok_fragment(
        http,
        &actor,
        &Command::RemoveMembers {
            members: vec![removed.id.clone()],
        },
        "removed locally; group commit published",
    )?;

    send_cmd_expect_ok_fragment(
        http,
        &actor,
        &Command::ReceiveCommit,
        "own commit accepted from DS",
    )?;

    for other in active.iter() {
        if other.id == actor.id {
            continue;
        }

        send_cmd_expect_ok_fragment(
            http,
            other,
            &Command::ReceiveCommit,
            "external commit received and processed",
        )?;
    }

    let actually_removed = active.remove(removed_idx);
    idle.push_front(actually_removed.clone());

    progress.tick(&format!(
        "remove {} actor={}",
        actually_removed.id, actor.id
    ));
    Ok(())
}

fn transition_to_size(
    http: &reqwest::blocking::Client,
    active: &mut Vec<WorkerSpec>,
    idle: &mut VecDeque<WorkerSpec>,
    target_size: usize,
    progress: &mut Progress,
) -> Result<()> {
    while active.len() < target_size {
        add_one_member(http, active, idle, progress)?;
    }

    while active.len() > target_size {
        remove_one_member(http, active, idle, progress)?;
    }

    Ok(())
}

fn run_update_phase(
    http: &reqwest::blocking::Client,
    active: &[WorkerSpec],
    plateau_size: usize,
    update_rounds: usize,
    max_update_samples_per_plateau: usize,
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

        send_cmd_expect_ok_fragment(
            http,
            actor,
            &Command::SelfUpdate,
            "self_update commit published to group",
        )?;

        send_cmd_expect_ok_fragment(
            http,
            actor,
            &Command::ReceiveCommit,
            "own commit accepted from DS",
        )?;

        for (j, worker) in active.iter().enumerate() {
            if j == actor_idx {
                continue;
            }

            send_cmd_expect_ok_fragment(
                http,
                worker,
                &Command::ReceiveCommit,
                "external commit received and processed",
            )?;
        }

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

fn run_application_phase(
    http: &reqwest::blocking::Client,
    active: &[WorkerSpec],
    plateau_size: usize,
    app_rounds: usize,
    max_app_samples_per_payload: usize,
    payload_sizes: &[usize],
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
            )?;

            let recipient_indices: Vec<usize> =
                (0..active.len()).filter(|&j| j != actor_idx).collect();

            let sampled_pos =
                sampled_member_index(recipient_indices.len(), per_payload_count, seq_no);

            for (pos, recipient_idx) in recipient_indices.iter().enumerate() {
                let worker = &active[*recipient_idx];
                let profile = pos == sampled_pos;

                send_cmd_expect_ok_fragment(
                    http,
                    worker,
                    &Command::ReceiveApplicationMessage { profile },
                    "application message received:",
                )?;
            }

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
    use super::sampled_member_index;

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
}
