//ADDED THIS ENTIRE FILE FOR THE MASTERS THESIS PROJECT!!!
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use cpu_time::ThreadTime;
use serde::Serialize;

static PROFILE_WRITER: OnceLock<Option<Mutex<BufWriter<File>>>> = OnceLock::new();

fn profile_path() -> Option<PathBuf> {
    std::env::var_os("OPENMLS_PROFILE_PATH").map(PathBuf::from)
}

fn writer() -> &'static Option<Mutex<BufWriter<File>>> {
    PROFILE_WRITER.get_or_init(|| {
        let path = match profile_path() {
            Some(p) => p,
            None => return None,
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;

        Some(Mutex::new(BufWriter::new(file)))
    })
}

fn unix_timestamp_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

fn current_pid() -> u32 {
    std::process::id()
}

fn current_thread_id() -> String {
    format!("{:?}", std::thread::current().id())
}

fn env_or_none(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

pub(crate) fn profiling_enabled() -> bool {
    writer().is_some()
}

#[derive(Serialize, Debug)]
pub(crate) struct ProfileEvent {
    pub ts_unix_ns: u128,
    pub op: String,
    pub implementation: String,

    pub wall_ns: u128,
    pub cpu_thread_ns: Option<u128>,

    pub alloc_bytes: Option<u64>,
    pub alloc_count: Option<u64>,

    pub artifact_size_bytes: Option<usize>,
    pub encrypted_group_info_bytes: Option<usize>,
    pub encrypted_secrets_count: Option<usize>,

    pub group_epoch: Option<u64>,
    pub tree_size: Option<u32>,
    pub member_count: Option<usize>,
    pub invitee_count: Option<isize>,
    pub ciphersuite: Option<String>,

    pub app_msg_plaintext_bytes: Option<usize>,
    pub app_msg_padding_bytes: Option<usize>,
    pub app_msg_ciphertext_bytes: Option<usize>,
    pub aad_bytes: Option<usize>,

    pub pid: u32,
    pub thread_id: String,

    pub run_id: Option<String>,
    pub scenario: Option<String>,
    pub node_name: Option<String>,
    pub pod_name: Option<String>,
}

pub(crate) fn emit_event(event: &ProfileEvent) {
    let Some(lock) = writer().as_ref() else {
        return;
    };

    let Ok(mut guard) = lock.lock() else {
        return;
    };

    if let Ok(line) = serde_json::to_string(event) {
        let _ = guard.write_all(line.as_bytes());
        let _ = guard.write_all(b"\n");
        let _ = guard.flush();
    }
}

pub(crate) struct ProfileScope {
    op: String,
    implementation: String,
    wall_start: Instant,
    cpu_start: Option<ThreadTime>,
}

impl ProfileScope {
    pub(crate) fn start(
        op: impl Into<String>,
        implementation: impl Into<String>,
    ) -> Option<Self> {
        if !profiling_enabled() {
            return None;
        }

        Some(Self {
            op: op.into(),
            implementation: implementation.into(),
            wall_start: Instant::now(),
            cpu_start: Some(ThreadTime::now()),
        })
    }

    pub(crate) fn finish(self) -> ProfileEvent {
        let wall_ns = self.wall_start.elapsed().as_nanos();
        let cpu_thread_ns = self.cpu_start.map(|start| start.elapsed().as_nanos());

        ProfileEvent {
            ts_unix_ns: unix_timestamp_ns(),
            op: self.op,
            implementation: self.implementation,

            wall_ns,
            cpu_thread_ns,

            alloc_bytes: None,
            alloc_count: None,

            artifact_size_bytes: None,
            encrypted_group_info_bytes: None,
            encrypted_secrets_count: None,

            group_epoch: None,
            tree_size: None,
            member_count: None,
            invitee_count: None,
            ciphersuite: None,

            app_msg_plaintext_bytes: None,
            app_msg_padding_bytes: None,
            app_msg_ciphertext_bytes: None,
            aad_bytes: None,

            pid: current_pid(),
            thread_id: current_thread_id(),

            run_id: env_or_none("OPENMLS_PROFILE_RUN_ID"),
            scenario: env_or_none("OPENMLS_PROFILE_SCENARIO"),
            node_name: env_or_none("OPENMLS_PROFILE_NODE"),
            pod_name: env_or_none("OPENMLS_PROFILE_POD"),
        }
    }
}
/*
pub(crate) fn finish_and_emit(
    scope: Option<ProfileScope>,
    fill: impl FnOnce(&mut ProfileEvent),
) {
    let Some(scope) = scope else {
        return;
    };

    let mut event = scope.finish();
    fill(&mut event);
    emit_event(&event);
}
*/