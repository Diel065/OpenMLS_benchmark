#!/usr/bin/env python3
from __future__ import annotations

import os
import argparse
import datetime as dt
import json
import math
import re
import shutil
import shlex
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="One-command local containerized Signal benchmark runner."
    )

    p.add_argument("--workers", type=int, required=True, help="Number of logical worker clients")
    p.add_argument("--run-id", default=None, help="Optional explicit run id")
    p.add_argument("--scenario", default="http-staircase-compose", help="Scenario label")
    p.add_argument("--output-dir", default="benchmark_output", help="Base output directory")

    p.add_argument("--min-size", type=int, default=2)
    p.add_argument("--max-size", type=int, default=None)
    p.add_argument("--step-size", type=int, default=1)
    p.add_argument("--roundtrips", type=int, default=1)

    p.add_argument("--update-rounds", type=int, default=2)
    p.add_argument("--max-update-samples-per-plateau", type=int, default=16)

    p.add_argument("--app-rounds", type=int, default=2)
    p.add_argument("--max-app-samples-per-payload", type=int, default=16)

    p.add_argument("--payload-sizes", default="32,256,1024", help="Comma-separated payload sizes")

    p.add_argument("--base-worker-port", type=int, default=8081)
    p.add_argument("--kr-port", type=int, default=3000)
    p.add_argument("--relay-port", type=int, default=4000)

    p.add_argument(
        "--bridge-count",
        type=int,
        default=1,
        help=(
            "Number of Docker bridge networks to distribute workers across. "
            "Passed through to scripts/generate_compose.py."
        ),
    )

    p.add_argument("--health-timeout-seconds", type=int, default=90)
    p.add_argument("--health-poll-seconds", type=float, default=0.5)

    p.add_argument(
        "--post-startup-settle-seconds",
        type=float,
        default=0.0,
        help=(
            "Sleep this many seconds after all containers are started, "
            "before starting health checks / runner. Useful for large Docker stacks."
        ),
    )

    p.add_argument(
        "--worker-health-timeout-seconds",
        type=int,
        default=300,
        help=(
            "How long the in-network benchmark runner should wait for all workers "
            "to become healthy before starting Signal logic."
        ),
    )

    p.add_argument(
        "--worker-health-poll-ms",
        type=int,
        default=250,
        help="Polling interval in milliseconds for in-network worker health checks.",
    )
    p.add_argument(
        "--max-fanout-parallelism",
        type=int,
        default=0,
        help=(
            "Maximum bounded parallelism for runner-to-worker fan-out. "
            "0 lets the Rust runner choose its conservative Docker-network default."
        ),
    )
    p.add_argument(
        "--fanout-adaptive",
        action="store_true",
        help=(
            "Enable adaptive runner fan-out throttling. Starts at 32 and reduces "
            "parallelism on latency/error spikes."
        ),
    )
    p.add_argument(
        "--no-fanout-adaptive",
        action="store_true",
        help="Disable adaptive fan-out even for large Docker worker counts.",
    )
    p.add_argument(
        "--min-fanout-parallelism",
        type=int,
        default=0,
        help="Minimum adaptive runner fan-out parallelism. 0 uses the Rust runner default.",
    )
    p.add_argument(
        "--fanout-error-rate-threshold",
        type=float,
        default=0.0,
        help="Adaptive fan-out error-rate threshold. 0 uses the Rust runner default.",
    )
    p.add_argument(
        "--fanout-p95-threshold-ms",
        type=int,
        default=0,
        help="Adaptive fan-out p95 latency threshold in milliseconds. 0 uses the Rust runner default.",
    )
    p.add_argument(
        "--http-pool-max-idle-per-host",
        type=int,
        default=0,
        help="Maximum idle pooled HTTP connections per host for the Rust runner.",
    )
    p.add_argument(
        "--runner-http-connect-timeout-ms",
        type=int,
        default=2000,
        help="SIGNAL_RUNNER_HTTP_CONNECT_TIMEOUT_MS for runner->worker clients.",
    )
    p.add_argument(
        "--runner-http-request-timeout-ms",
        type=int,
        default=60000,
        help="SIGNAL_RUNNER_HTTP_REQUEST_TIMEOUT_MS for runner->worker clients.",
    )
    p.add_argument(
        "--process-pending-fanout",
        action="store_true",
        help="Accepted for compatibility; Signal pairwise receives are already batched by worker.",
    )
    p.add_argument(
        "--worker-http-pool-max-idle-per-host",
        type=int,
        default=32,
        help="SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST for worker->KR/relay clients.",
    )
    p.add_argument(
        "--worker-http-connect-timeout-ms",
        type=int,
        default=5000,
        help="SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS for worker->KR/relay clients.",
    )
    p.add_argument(
        "--worker-http-request-timeout-ms",
        type=int,
        default=30000,
        help="SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS for worker->KR/relay clients.",
    )
    p.add_argument(
        "--worker-outbound-http-permits",
        type=int,
        default=32,
        help="SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS per worker process.",
    )
    p.add_argument(
        "--host-health-parallelism",
        type=int,
        default=64,
        help="Maximum parallel host-side worker health probes when not using --runner-in-docker.",
    )

    p.add_argument(
        "--preflight-only",
        action="store_true",
        help=(
            "Only check KR/relay/worker reachability from inside the Docker network, "
            "then exit without running the Signal benchmark. No events.csv is expected."
        ),
    )

    p.add_argument(
        "--startup-batch-size",
        type=int,
        default=0,
        help=(
            "Start worker containers in batches of this size. "
            "0 means use one normal docker compose up for all services."
        ),
    )

    p.add_argument(
        "--startup-batch-sleep-seconds",
        type=float,
        default=0.25,
        help="Sleep this many seconds between worker startup batches.",
    )

    p.add_argument(
        "--compose-parallel-limit",
        type=int,
        default=None,
        help=(
            "Set COMPOSE_PARALLEL_LIMIT for docker compose operations. "
            "Useful for avoiding Docker daemon overload with many containers."
        ),
    )

    p.add_argument(
        "--compose-down-timeout-seconds",
        type=int,
        default=1,
        help=(
            "Shutdown timeout for docker compose down. "
            "Use a small value for benchmark containers to avoid long teardown waits."
        ),
    )

    p.add_argument(
        "--teardown-batch-size",
        type=int,
        default=0,
        help=(
            "Stop/remove worker containers in batches of this size before final compose down. "
            "0 means use normal docker compose down for the whole stack."
        ),
    )

    p.add_argument(
        "--teardown-batch-sleep-seconds",
        type=float,
        default=0.25,
        help="Sleep this many seconds between teardown batches.",
    )

    p.add_argument(
        "--runner-in-docker",
        action="store_true",
        help=(
            "Run benchmark_runner_http_staircase inside the Docker network. "
            "This allows workers to avoid publishing host ports."
        ),
    )
    p.add_argument(
        "--include-netcheck",
        action="store_true",
        help=(
            "Include the continuous diagnostic netcheck service in the generated "
            "Compose stack. Default: disabled."
        ),
    )

    p.add_argument(
        "--build-images",
        action="store_true",
        help="Build Docker images before running the benchmark",
    )
    p.add_argument(
        "--keep-stack-up",
        action="store_true",
        help="Do not run docker compose down at the end",
    )
    p.add_argument(
        "--keep-stack-up-on-failure",
        action="store_true",
        help="Do not run docker compose down when startup or runner execution fails",
    )
    p.add_argument(
        "--keep-generated-files",
        action="store_true",
        help="Keep temporary generated compose/worker files at repo root",
    )
    p.add_argument(
        "--force-cleanup-signal-ports",
        action="store_true",
        help="Before starting, forcibly remove existing Docker containers with names beginning with 'signal-'",
    )

    # Hybrid layout flags
    p.add_argument(
        "--worker-layout-mode",
        choices=["one-container-per-client", "hybrid"],
        default="one-container-per-client",
        help="Worker layout mode: one-container-per-client (legacy) or hybrid",
    )
    p.add_argument(
        "--singleton-min-count",
        type=int,
        default=16,
        help="Minimum number of singleton measured clients in hybrid mode",
    )
    p.add_argument(
        "--singleton-fraction",
        type=float,
        default=0.125,
        help="Fraction of logical workers to use as singletons in hybrid mode",
    )
    p.add_argument(
        "--packed-clients-per-container",
        type=int,
        default=16,
        help="Number of packed virtual clients per packed container",
    )
    p.add_argument(
        "--singleton-selection-seed",
        type=int,
        default=1,
        help="Seed for deterministic singleton selection",
    )
    p.add_argument(
        "--singleton-selection-strategy",
        choices=["stratified-random", "evenly-spaced"],
        default="stratified-random",
        help="Strategy for selecting singleton client IDs",
    )
    p.add_argument(
        "--profile-only-singletons",
        action="store_true",
        default=True,
        help="Only profile singleton measured clients (default: true)",
    )
    p.add_argument(
        "--packed-worker-internal-parallelism",
        type=int,
        default=4,
        help="Internal parallelism for packed worker containers",
    )

    # External device flags
    p.add_argument(
        "--devices-file",
        default=None,
        help="Path to YAML config file for external real devices",
    )
    p.add_argument(
        "--enable-external-devices",
        action="store_true",
        help="Enable external device orchestration (requires --devices-file)",
    )
    p.add_argument(
        "--external-device",
        action="append",
        default=[],
        dest="external_device_ids",
        help="Specific external device ID(s) to enable (repeatable). Default: all enabled devices.",
    )
    p.add_argument(
        "--no-aggregate",
        action="store_true",
        help="Pass --no-aggregate to the Rust benchmark runner (for post-run aggregation with external devices)",
    )
    p.add_argument(
        "--wipe-run-dir",
        action="store_true",
        help="Wipe local benchmark_output/<run_id> before starting",
    )
    p.add_argument(
        "--wipe-device-run-dirs",
        action="store_true",
        help="Wipe remote device run directories before starting",
    )
    p.add_argument(
        "--no-device-stop-after-run",
        action="store_true",
        help="Do not stop external device workers after the benchmark run",
    )

    return p


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def timestamped_run_id(worker_count: int) -> str:
    now = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"compose-{worker_count}w-{now}"


def sanitize_project_name(run_id: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", run_id).strip("-_").lower()
    if not cleaned:
        cleaned = "signal-benchmark"
    return f"signal-{cleaned}"[:63]


def validate_run_id(run_id: str) -> None:
    if not run_id:
        raise ValueError("Run ID must not be empty")
    if run_id in ("/", ".", ".."):
        raise ValueError(f"Run ID must not be '{run_id}'")
    if "/" in run_id:
        raise ValueError("Run ID must not contain '/'")
    if not RUN_ID_RE.match(run_id):
        raise ValueError(
            f"Run ID must only contain [A-Za-z0-9._-], got '{run_id}'"
        )


def output_root_for(root: Path, output_dir_name: str) -> Path:
    output_root = Path(output_dir_name)
    if not output_root.is_absolute():
        output_root = root / output_root
    return output_root


def safe_wipe_run_dir(run_dir: Path, output_root: Path, run_id: str) -> None:
    validate_run_id(run_id)
    resolved_output_root = output_root.resolve()
    resolved_run_dir = run_dir.resolve(strict=False)
    if resolved_run_dir.parent != resolved_output_root or resolved_run_dir.name != run_id:
        raise RuntimeError(f"Refusing to wipe unsafe run directory: {run_dir}")
    if run_dir.exists():
        print(f"[cleanup] wiping local run directory: {run_dir}", flush=True)
        shutil.rmtree(run_dir)


def insert_after_leader(existing: list, additions: list) -> list:
    if not additions:
        return existing
    if not existing:
        return list(additions)
    return existing[:1] + list(additions) + existing[1:]


def run_cmd(
        cmd: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        check: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(cwd), env=env, check=check)


def tee_subprocess_output(
        cmd: list[str],
        *,
        cwd: Path,
        output_path: Path,
        env: dict[str, str] | None = None,
) -> int:
    with output_path.open("w", encoding="utf-8") as out_file:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
            out_file.write(line)

        return proc.wait()


def wait_for_health(url: str, timeout_seconds: int, poll_seconds: float) -> None:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                body = resp.read().decode("utf-8", errors="replace").strip()
                if 200 <= resp.status < 300 and body == "ok":
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass

        time.sleep(poll_seconds)

    raise RuntimeError(f"Timed out waiting for health endpoint: {url}")


def wait_for_workers_health_parallel(
        worker_lines: list[str],
        timeout_seconds: int,
        poll_seconds: float,
        max_parallelism: int,
) -> None:
    if not worker_lines:
        return

    parsed_workers: list[tuple[str, str]] = []
    for line in worker_lines:
        worker_id, worker_url = line.split("=", 1)
        parsed_workers.append((worker_id, worker_url))

    parallelism = max(1, min(max_parallelism, len(parsed_workers)))
    print(
        f"[health] waiting for {len(parsed_workers)} workers with host parallelism={parallelism}",
        flush=True,
    )

    def probe(worker: tuple[str, str]) -> str:
        worker_id, worker_url = worker
        wait_for_health(f"{worker_url}/health", timeout_seconds, poll_seconds)
        return worker_id

    completed = 0
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        futures = [executor.submit(probe, worker) for worker in parsed_workers]
        for future in as_completed(futures):
            worker_id = future.result()
            completed += 1
            if completed <= 10 or completed == len(parsed_workers) or completed % 100 == 0:
                print(
                    f"[health] worker {worker_id} ok ({completed}/{len(parsed_workers)})",
                    flush=True,
                )


def read_worker_lines(path: Path) -> list[str]:
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        lines.append(line)
    return lines


def validate_artifacts(run_dir: Path, layout_mode: str) -> None:
    csv_path = run_dir / "events.csv"
    layout_path = run_dir / "worker_layout.json"

    if layout_mode == "hybrid":
        if not layout_path.exists():
            raise RuntimeError(f"Missing worker_layout.json in hybrid mode: {layout_path}")

    if not csv_path.exists():
        raise RuntimeError(f"Missing aggregated CSV: {csv_path}")
    if csv_path.stat().st_size == 0:
        raise RuntimeError(f"Aggregated CSV is empty: {csv_path}")

    jsonl_files = sorted(run_dir.glob("participant-*.jsonl"))
    if not jsonl_files:
        raise RuntimeError(f"No per-worker JSONL files found in {run_dir}")

    non_empty_jsonl = [p for p in jsonl_files if p.stat().st_size > 0]
    if not non_empty_jsonl:
        raise RuntimeError(f"All per-worker JSONL files are empty in {run_dir}")


def copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        shutil.copy2(src, dst)


def port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def required_host_ports(args: argparse.Namespace) -> list[int]:
    ports = [args.kr_port, args.relay_port]

    if not args.runner_in_docker:
        if args.worker_layout_mode == "hybrid":
            import math
            singleton_count = min(
                args.workers,
                max(args.singleton_min_count, math.ceil(args.workers * args.singleton_fraction)),
            )
            packed_client_count = args.workers - singleton_count
            packed_container_count = math.ceil(packed_client_count / args.packed_clients_per_container) if packed_client_count > 0 else 0
            physical_count = singleton_count + packed_container_count
        else:
            physical_count = args.workers
        ports.extend(args.base_worker_port + i for i in range(physical_count))

    return ports


def check_required_ports(args: argparse.Namespace) -> None:
    busy = [p for p in required_host_ports(args) if not port_is_free(p)]
    if not busy:
        return

    busy_text = ", ".join(str(p) for p in busy)
    raise RuntimeError(
        "One or more required host ports are already in use: "
        f"{busy_text}\n"
        "Stop the previous benchmark stack, or choose different ports.\n"
        "You can also rerun with --force-cleanup-signal-ports to remove old signal-* Docker containers."
    )


def docker_cleanup_signal_containers(root: Path) -> None:
    result = subprocess.run(
        ["docker", "ps", "-aq", "--filter", "name=signal-"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if container_ids:
        print(f"[cleanup] removing {len(container_ids)} old signal-* containers")

        for batch in chunks(container_ids, 64):
            subprocess.run(
                ["docker", "rm", "-f", *batch],
                cwd=str(root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )

    network_result = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    network_names = [
        line.strip()
        for line in network_result.stdout.splitlines()
        if line.strip().startswith("signal-")
    ]

    if network_names:
        print(f"[cleanup] removing {len(network_names)} old signal-* networks")

        for batch in chunks(network_names, 32):
            subprocess.run(
                ["docker", "network", "rm", *batch],
                cwd=str(root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )


def write_compose_logs(root: Path, compose_file: Path, dest: Path, append: bool = False) -> None:
    mode = "a" if append else "w"
    with dest.open(mode, encoding="utf-8") as f:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "logs", "--no-color"],
            cwd=str(root),
            stdout=f,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )


def run_capture(
        root: Path,
        dest: Path,
        cmd: list[str],
        *,
        env: dict[str, str] | None = None,
) -> None:
    with dest.open("w", encoding="utf-8") as f:
        f.write("$ " + " ".join(cmd) + "\n\n")
        subprocess.run(
            cmd,
            cwd=str(root),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )


def write_url_capture(dest: Path, url: str, timeout_seconds: float = 3.0) -> None:
    with dest.open("w", encoding="utf-8") as f:
        f.write(f"$ GET {url}\n\n")
        try:
            with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
                f.write(response.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, TimeoutError, OSError) as err:
            f.write(f"ERROR: {err}\n")


def extract_failed_worker_ids(terminal_output_path: Path) -> list[str]:
    if not terminal_output_path.exists():
        return []

    text = terminal_output_path.read_text(encoding="utf-8", errors="replace")
    ids: list[str] = []
    seen: set[str] = set()

    def add(worker_id: str) -> None:
        if worker_id not in seen:
            seen.add(worker_id)
            ids.append(worker_id)

    tail = text[-250_000:]
    priority_patterns = [
        r"failures=\[([^\]]+)\]",
        r"runner\.worker_command failed: worker=([0-9]{5})",
        r"Worker ([0-9]{5}) error:",
        r"client=([0-9]{5}) url=http://(?:ds|relay)",
    ]

    for pattern in priority_patterns:
        for match in re.finditer(pattern, tail, flags=re.DOTALL):
            if match.lastindex == 1 and "[" not in match.group(1) and ":" not in match.group(1):
                add(match.group(1))
                continue

            for worker_id in re.findall(r"\b([0-9]{5})\b", match.group(0)):
                add(worker_id)

    fallback_patterns = [
        r"worker=([0-9]{5})",
        r"Worker ([0-9]{5})",
        r"worker-([0-9]{5})",
        r"client=([0-9]{5})",
    ]
    for pattern in fallback_patterns:
        for match in re.finditer(pattern, text):
            worker_id = match.group(1)
            add(worker_id)

    return ids[:50]


def compose_service_logs(
        root: Path,
        compose_file: Path,
        dest: Path,
        services: list[str],
        env: dict[str, str] | None,
) -> None:
    with dest.open("w", encoding="utf-8") as f:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "logs",
                "--no-color",
                *services,
            ],
            cwd=str(root),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
        )


def compose_container_id(
        root: Path,
        compose_file: Path,
        service: str,
        env: dict[str, str] | None,
) -> str | None:
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "ps", "-q", service],
        cwd=str(root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )

    container_id = result.stdout.strip()
    return container_id or None


def project_network_names(root: Path, project_name: str) -> list[str]:
    result = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )

    prefixes = (f"{project_name}_", f"{project_name}-")
    return [
        line.strip()
        for line in result.stdout.splitlines()
        if line.strip().startswith(prefixes)
    ]


def write_netcheck_targets(run_dir: Path, worker_ids: list[str]) -> None:
    if not worker_ids:
        return

    target_path = run_dir / "netcheck_targets.txt"
    target_path.write_text("\n".join(worker_ids) + "\n", encoding="utf-8")


def read_int_file(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def warn_if_neighbor_cache_tight(layout_path: Path, service_count: int = 3) -> None:
    try:
        layout = json.loads(layout_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return

    physical_workers = int(layout.get("physical_worker_count") or 0)
    gc_thresh3 = read_int_file(Path("/proc/sys/net/ipv4/neigh/default/gc_thresh3"))
    if physical_workers <= 0 or not gc_thresh3:
        return

    estimated_neighbors = physical_workers + service_count
    if estimated_neighbors >= int(gc_thresh3 * 0.8):
        print(
            "[preflight-warning] physical containers plus services are close to "
            f"neighbor-cache gc_thresh3: estimated={estimated_neighbors} "
            f"gc_thresh3={gc_thresh3}. Hybrid runs usually stay below this, but "
            "singleton or high-physical-container runs may need net.ipv4.neigh.* "
            "and nf_conntrack tuning.",
            flush=True,
        )


def collect_failure_diagnostics(
        *,
        root: Path,
        compose_file: Path,
        run_dir: Path,
        args: argparse.Namespace,
        compose_env: dict[str, str] | None,
        project_name: str,
        terminal_output_path: Path,
) -> None:
    diag_dir = run_dir / "failure_diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    failed_worker_ids = extract_failed_worker_ids(terminal_output_path)
    write_netcheck_targets(run_dir, failed_worker_ids)

    print(
        f"[diagnostics] collecting failure evidence in {diag_dir} "
        f"failed_workers={failed_worker_ids or '-'}",
        flush=True,
    )

    run_capture(
        root,
        diag_dir / "docker-compose-ps.txt",
        ["docker", "compose", "-f", str(compose_file), "ps"],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-ip-neigh.txt",
        [
            "sh",
            "-lc",
            "ip neigh show 2>&1; printf '\\n## state counts\\n'; "
            "ip neigh show 2>/dev/null | awk '{count[$NF]++} END {for (state in count) print state, count[state]}' || true",
        ],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-conntrack.txt",
        [
            "sh",
            "-lc",
            "printf 'nf_conntrack_count='; cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || true; "
            "printf '\\nnf_conntrack_max='; cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null || true; printf '\\n'",
        ],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-sockets.txt",
        ["sh", "-lc", "ss -s 2>&1 || true"],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-cpu-io.txt",
        ["sh", "-lc", "top -b -n1 | head -40 2>&1; printf '\\n## vmstat\\n'; vmstat 1 3 2>&1 || true"],
        env=compose_env,
    )
    run_capture(
        root,
        diag_dir / "host-neighbor-sysctls.txt",
        [
            "sh",
            "-lc",
            "for f in /proc/sys/net/ipv4/neigh/default/gc_thresh1 "
            "/proc/sys/net/ipv4/neigh/default/gc_thresh2 "
            "/proc/sys/net/ipv4/neigh/default/gc_thresh3; do "
            "printf '%s=' \"$f\"; cat \"$f\" 2>/dev/null || true; done",
        ],
        env=compose_env,
    )
    write_url_capture(diag_dir / "kr-metrics.json", f"http://127.0.0.1:{args.kr_port}/metrics")
    write_url_capture(
        diag_dir / "relay-metrics.json",
        f"http://127.0.0.1:{args.relay_port}/metrics",
    )

    services = ["runner", "kr", "relay"]
    if args.include_netcheck:
        services.append("netcheck")
    services.extend(f"worker-{worker_id}" for worker_id in failed_worker_ids)
    services.extend(f"worker-pack-{i:03d}" for i in range(100))
    compose_service_logs(
        root,
        compose_file,
        diag_dir / "focused-compose-logs.txt",
        services,
        compose_env,
    )

    for worker_id in failed_worker_ids:
        service = f"worker-{worker_id}"
        container_id = compose_container_id(root, compose_file, service, compose_env)
        if container_id:
            run_capture(
                root,
                diag_dir / f"{service}-inspect.json",
                ["docker", "inspect", container_id],
                env=compose_env,
            )
            run_capture(
                root,
                diag_dir / f"{service}-docker-logs.txt",
                ["docker", "logs", "--timestamps", container_id],
                env=compose_env,
            )

    networks = project_network_names(root, project_name)
    for network in networks:
        run_capture(
            root,
            diag_dir / f"network-{network}-inspect.json",
            ["docker", "network", "inspect", network],
            env=compose_env,
        )

    if failed_worker_ids:
        probe_lines = []
        for worker_id in failed_worker_ids[:10]:
            probe_lines.append(f'echo "## worker-{worker_id}"')
            probe_lines.append(
                f'curl -fsS --connect-timeout 2 --max-time 5 '
                f'http://worker-{worker_id}:8080/health || true'
            )
            probe_lines.append("")
        probe_script = "\n".join(probe_lines)

        if args.include_netcheck:
            run_capture(
                root,
                diag_dir / "runner-network-health-probes.txt",
                [
                    "docker",
                    "compose",
                    "-f",
                    str(compose_file),
                    "run",
                    "--rm",
                    "--no-deps",
                    "netcheck",
                    "sh",
                    "-lc",
                    probe_script,
                ],
                env=compose_env,
            )

        for worker_id in failed_worker_ids[:10]:
            worker_index = int(worker_id)
            bridge_index = ((worker_index - 1) * args.bridge_count) // args.workers
            bridge_suffix = f"bench-net-{bridge_index:03d}"
            matching_networks = [name for name in networks if name.endswith(bridge_suffix)]
            for network in matching_networks[:1]:
                run_capture(
                    root,
                    diag_dir / f"failed-worker-{worker_id}-own-network-health.txt",
                    [
                        "docker",
                        "run",
                        "--rm",
                        "--network",
                        network,
                        "nicolaka/netshoot:latest",
                        "sh",
                        "-lc",
                        (
                            f"curl -fsS --connect-timeout 2 --max-time 5 "
                            f"http://worker-{worker_id}:8080/health || true"
                        ),
                    ],
                    env=compose_env,
                )

        for network in networks:
            run_capture(
                root,
                diag_dir / f"diagnostic-container-health-probe-{network}.txt",
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    network,
                    "nicolaka/netshoot:latest",
                    "sh",
                    "-lc",
                    probe_script,
                ],
                env=compose_env,
            )

    if args.include_netcheck:
        netcheck_log = run_dir / "netcheck.log"
        if netcheck_log.exists():
            shutil.copy2(netcheck_log, diag_dir / "netcheck.log")


def physical_worker_services(
    args: argparse.Namespace,
    compose_file: Path,
    root: Path,
) -> list[str]:
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "config", "--services"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return []

    services = [s.strip() for s in result.stdout.splitlines() if s.strip()]
    return [s for s in services if s.startswith("worker-") and s not in ("worker-00000",)]


def chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def build_compose_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    if args.compose_parallel_limit is not None:
        env["COMPOSE_PARALLEL_LIMIT"] = str(args.compose_parallel_limit)

    env["SIGNAL_RUNNER_HTTP_CONNECT_TIMEOUT_MS"] = str(args.runner_http_connect_timeout_ms)
    env["SIGNAL_RUNNER_HTTP_REQUEST_TIMEOUT_MS"] = str(args.runner_http_request_timeout_ms)
    env["SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST"] = str(args.worker_http_pool_max_idle_per_host)
    env["SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS"] = str(args.worker_http_connect_timeout_ms)
    env["SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS"] = str(args.worker_http_request_timeout_ms)
    env["SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS"] = str(args.worker_outbound_http_permits)
    return env


def compose_down(
        *,
        root: Path,
        compose_file: Path,
        args: argparse.Namespace,
        env: dict[str, str] | None,
) -> None:
    timeout = str(args.compose_down_timeout_seconds)

    if args.teardown_batch_size <= 0:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "down",
                "--timeout",
                timeout,
            ],
            cwd=str(root),
            env=env,
            check=False,
        )
        return

    print("[compose] stopping/removing runner service if present")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "stop",
            "-t",
            timeout,
            "runner",
        ],
        cwd=str(root),
        env=env,
        check=False,
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "rm",
            "-f",
            "runner",
        ],
        cwd=str(root),
        env=env,
        check=False,
    )

    workers = physical_worker_services(args, compose_file, root)
    workers.reverse()

    for batch in chunks(workers, args.teardown_batch_size):
        print(
            f"[compose] stopping/removing workers {batch[-1]} .. {batch[0]} "
            f"({len(batch)} workers)"
        )

        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "stop",
                "-t",
                timeout,
                *batch,
            ],
            cwd=str(root),
            env=env,
            check=False,
        )

        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_file),
                "rm",
                "-f",
                *batch,
            ],
            cwd=str(root),
            env=env,
            check=False,
        )

        if args.teardown_batch_sleep_seconds > 0:
            time.sleep(args.teardown_batch_sleep_seconds)

    print("[compose] final down for kr/relay/network")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "down",
            "--timeout",
            timeout,
        ],
        cwd=str(root),
        env=env,
        check=False,
    )


def launch_external_devices(
    args: argparse.Namespace,
    root: Path,
    kr_port: int,
    relay_port: int,
    run_id: str,
) -> tuple[list[dict], list[dict], list[str]]:
    """Start external devices and return (layout_clients, layout_workers, worker_lines)."""
    from external_devices import (
        create_backend,
        load_devices_config,
        validate_run_id as py_validate_run_id,
        WorkerLaunch,
        build_external_device_layout_entry,
    )

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / devices_file
    if not devices_file.exists():
        raise RuntimeError(f"Devices file not found: {devices_file}")

    py_validate_run_id(run_id)

    configs = load_devices_config(devices_file)
    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    if not enabled:
        print("[device] no enabled external devices found")
        return [], [], []

    layout_clients: list[dict] = []
    layout_workers: list[dict] = []
    worker_lines: list[str] = []

    for dev_config in enabled:
        dev_id = dev_config.id
        print(f"[device] launching external device: {dev_id}", flush=True)

        backend = create_backend(dev_config)

        # Check reachable
        print(f"[device] {dev_id}: checking reachability...", flush=True)
        backend.check_reachable()

        expected_arch = dev_config.target.get("arch")
        if expected_arch:
            arch = backend.shell("uname -m", check=False).stdout.strip()
            if arch != expected_arch:
                raise RuntimeError(
                    f"Device {dev_id} architecture mismatch: "
                    f"expected {expected_arch}, got {arch or '<unknown>'}"
                )
            print(f"[device] {dev_id}: architecture ok ({arch})", flush=True)

        print(f"[device] {dev_id}: checking clock...", flush=True)
        backend.ensure_clock_synchronized()

        transport = dev_config.transport
        device_ip = transport.get("device_ip", "172.32.0.93")
        host_ip = transport.get("host_ip", "172.32.0.98")
        worker_port = transport.get("worker_port", 8080)
        listen_addr = f"0.0.0.0:{worker_port}"

        # Stop existing worker
        print(f"[device] {dev_id}: stopping existing worker...", flush=True)
        backend.stop_worker()

        # Wipe remote directories
        if args.wipe_device_run_dirs:
            print(f"[device] {dev_id}: wiping remote directories...", flush=True)
            backend.wipe_for_run(run_id)

        # Push worker binary
        configured_binary = Path(dev_config.target.get(
            "binary",
            "target/armv7-unknown-linux-musleabihf/minsize/worker",
        ))
        local_binary = configured_binary if configured_binary.is_absolute() else root / configured_binary
        remote_binary = dev_config.worker.get("remote_binary", "/worker")
        if local_binary.exists():
            print(f"[device] {dev_id}: pushing worker binary...", flush=True)
            backend.install_worker(local_binary, remote_binary)
        else:
            print(f"[device] {dev_id}: worker binary not found at {local_binary}; checking remote {remote_binary}", flush=True)
            remote_check = backend.shell(
                f"test -x {shlex.quote(remote_binary)}",
                check=False,
            )
            if remote_check.returncode != 0:
                raise RuntimeError(
                    f"External worker binary is missing locally and remotely for {dev_id}. "
                    f"Expected local binary at {local_binary} or executable remote binary at {remote_binary}."
                )
            print(f"[device] {dev_id}: using existing remote worker binary {remote_binary}", flush=True)

        # Build DS/relay URLs for device
        device_kr_url = f"http://{host_ip}:{kr_port}"
        device_relay_url = f"http://{host_ip}:{relay_port}"

        worker_id = dev_config.worker.get("id", dev_id)
        remote_results_root = dev_config.worker.get("remote_results_root", "/results/signal")
        remote_tmp = dev_config.worker.get("remote_tmp", "/tmp/signal-benchmark")
        profile_template = f"{remote_results_root}/{run_id}/participant-{{participant_id}}.jsonl"

        launch = WorkerLaunch(
            worker_id=worker_id,
            binary_path=remote_binary,
            kr_url=device_kr_url,
            relay_url=device_relay_url,
            listen_addr=listen_addr,
            run_id=run_id,
            scenario=args.scenario,
            profile_path_template=profile_template,
            remote_results_root=remote_results_root,
            remote_tmp=remote_tmp,
            node_name=dev_id,
        )

        print(f"[device] {dev_id}: starting worker (KR={device_kr_url}, Relay={device_relay_url})...", flush=True)
        backend.start_worker(launch)

        # Health check
        worker_url = f"http://{device_ip}:{worker_port}"
        print(f"[device] {dev_id}: waiting for health on {worker_url}/health...", flush=True)
        try:
            backend.wait_health(f"{worker_url}/health", timeout_s=60)
        except Exception as e:
            log = backend.shell(
                f"cat {shlex.quote(remote_tmp)}/worker.log 2>/dev/null || true",
                check=False,
            ).stdout.strip()
            ps = backend.shell("ps | grep '[w]orker' || true", check=False).stdout.strip()
            backend.stop_worker()
            details = [
                f"External worker {dev_id} did not become healthy at {worker_url}/health.",
                f"Remote binary: {remote_binary}",
            ]
            if ps:
                details.append(f"Worker process:\n{ps}")
            if log:
                details.append(f"Worker log:\n{log}")
            raise RuntimeError("\n".join(details)) from e

        # Build layout entries
        client_entry, phys_entry, worker_line = build_external_device_layout_entry(
            dev_config,
            transport_ip=device_ip,
            host_ip=host_ip,
            worker_port=worker_port,
            kr_port=kr_port,
            relay_port=relay_port,
            run_id=run_id,
        )

        layout_clients.append(client_entry)
        layout_workers.append(phys_entry)
        worker_lines.append(worker_line)

        print(f"[device] {dev_id}: worker {worker_id} ready at {worker_url}", flush=True)

    return layout_clients, layout_workers, worker_lines


def stop_external_device_workers(args: argparse.Namespace, root: Path) -> None:
    if not args.enable_external_devices or not args.devices_file or args.no_device_stop_after_run:
        return

    from external_devices import create_backend, load_devices_config

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / args.devices_file
    if not devices_file.exists():
        return

    configs = load_devices_config(devices_file)
    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    for dev_config in enabled:
        try:
            backend = create_backend(dev_config)
            print(f"[device] {dev_config.id}: stopping worker...", flush=True)
            backend.stop_worker()
        except Exception as e:
            print(f"[device] {dev_config.id}: stop failed (non-fatal): {e}", flush=True)


def pull_external_device_profiles(
    args: argparse.Namespace,
    root: Path,
    run_id: str,
    run_dir: Path,
) -> None:
    """Pull profile files from external devices into the local run directory."""
    from external_devices import create_backend, load_devices_config, validate_run_id as py_validate_run_id

    devices_file = Path(args.devices_file)
    if not devices_file.is_absolute():
        devices_file = root / devices_file
    if not devices_file.exists():
        return

    py_validate_run_id(run_id)

    configs = load_devices_config(devices_file)
    enabled = [c for c in configs if c.enabled]
    if args.external_device_ids:
        enabled = [c for c in enabled if c.id in args.external_device_ids]

    for dev_config in enabled:
        dev_id = dev_config.id
        backend = create_backend(dev_config)

        remote_results_root = dev_config.worker.get("remote_results_root", "/results/signal")
        remote_path = f"{remote_results_root}/{run_id}"

        device_local_dir = run_dir / "external" / dev_id
        device_local_dir.mkdir(parents=True, exist_ok=True)

        print(f"[device] {dev_id}: pulling profiles from {remote_path} to {device_local_dir}", flush=True)
        try:
            backend.pull(remote_path, device_local_dir)
        except Exception as e:
            print(f"[device] {dev_id}: pull failed (non-fatal): {e}", flush=True)

        # Also try pulling individual JSONL files to the top-level run dir.
        # Retry with backoff in case the device worker is still flushing data.
        worker_id = dev_config.worker.get("id", dev_id)
        remote_jsonl = f"{remote_path}/participant-{worker_id}.jsonl"
        local_jsonl = run_dir / f"participant-{worker_id}.jsonl"

        pulled = False
        for attempt in range(1, 4):
            try:
                backend.pull(remote_jsonl, local_jsonl)
                size = local_jsonl.stat().st_size if local_jsonl.exists() else 0
                print(f"[device] {dev_id}: pulled profile ({size} bytes) to {local_jsonl}", flush=True)
                pulled = True
                break
            except Exception as e:
                if attempt < 3:
                    wait = attempt * 2.0
                    print(f"[device] {dev_id}: pull attempt {attempt} failed, retrying in {wait}s: {e}", flush=True)
                    time.sleep(wait)
                else:
                    print(f"[device] {dev_id}: pulling individual JSONL failed after 3 attempts (non-fatal): {e}", flush=True)

        if not pulled:
            # Check what files exist on the device
            print(f"[device] {dev_id}: checking remote directory contents for debugging...", flush=True)
            try:
                ls_result = backend.shell(f"ls -la {shlex.quote(remote_path)}/ 2>/dev/null || echo 'NO_DIR'", check=False)
                print(f"[device] {dev_id}: remote {remote_path}/ contents:\n{ls_result.stdout.strip()}", flush=True)
            except Exception as ls_err:
                print(f"[device] {dev_id}: could not list remote dir: {ls_err}", flush=True)


def run_standalone_aggregation(
    run_dir: Path,
    layout_file: Optional[Path],
    workers_file: Optional[Path],
) -> int:
    """Run the Rust aggregate_profiles binary."""
    root = repo_root()
    # Prefer compiled binary if available; fall back to cargo run
    binary = root / "target" / "debug" / "aggregate_profiles"
    release_binary = root / "target" / "release" / "aggregate_profiles"
    if release_binary.exists():
        binary = release_binary

    if binary.exists():
        cmd = [str(binary)]
    else:
        cmd = ["cargo", "run", "--bin", "aggregate_profiles", "--"]

    cmd += ["--run-dir", str(run_dir)]
    if layout_file:
        cmd += ["--layout-file", str(layout_file)]
    if workers_file:
        cmd += ["--workers-file", str(workers_file)]

    print(f"[aggregate] running standalone aggregation", flush=True)
    result = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[aggregate] aggregation failed (exit={result.returncode}): {result.stderr}", flush=True)
    else:
        print(f"[aggregate] aggregation complete: {result.stdout.strip()}", flush=True)
    return result.returncode


def main() -> int:
    args = build_parser().parse_args()
    root = repo_root()

    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    if args.bridge_count < 1:
        raise SystemExit("--bridge-count must be at least 1")

    if args.bridge_count > args.workers:
        raise SystemExit("--bridge-count must not exceed --workers")

    if args.startup_batch_size < 0:
        raise SystemExit("--startup-batch-size must be >= 0")

    if args.startup_batch_sleep_seconds < 0:
        raise SystemExit("--startup-batch-sleep-seconds must be >= 0")

    if args.compose_parallel_limit is not None and args.compose_parallel_limit < 1:
        raise SystemExit("--compose-parallel-limit must be >= 1")

    if args.compose_down_timeout_seconds < 0:
        raise SystemExit("--compose-down-timeout-seconds must be >= 0")

    if args.teardown_batch_size < 0:
        raise SystemExit("--teardown-batch-size must be >= 0")

    if args.teardown_batch_sleep_seconds < 0:
        raise SystemExit("--teardown-batch-sleep-seconds must be >= 0")

    if args.post_startup_settle_seconds < 0:
        raise SystemExit("--post-startup-settle-seconds must be >= 0")

    if args.worker_health_timeout_seconds < 1:
        raise SystemExit("--worker-health-timeout-seconds must be >= 1")

    if args.worker_health_poll_ms < 1:
        raise SystemExit("--worker-health-poll-ms must be >= 1")

    if args.max_fanout_parallelism < 0:
        raise SystemExit("--max-fanout-parallelism must be >= 0")

    if args.min_fanout_parallelism < 0:
        raise SystemExit("--min-fanout-parallelism must be >= 0")

    if args.fanout_adaptive and args.no_fanout_adaptive:
        raise SystemExit("--fanout-adaptive and --no-fanout-adaptive cannot both be set")

    if args.fanout_error_rate_threshold < 0:
        raise SystemExit("--fanout-error-rate-threshold must be >= 0")

    if args.fanout_p95_threshold_ms < 0:
        raise SystemExit("--fanout-p95-threshold-ms must be >= 0")

    if args.http_pool_max_idle_per_host < 0:
        raise SystemExit("--http-pool-max-idle-per-host must be >= 0")

    if args.worker_http_pool_max_idle_per_host < 1:
        raise SystemExit("--worker-http-pool-max-idle-per-host must be >= 1")

    if args.worker_http_connect_timeout_ms < 1:
        raise SystemExit("--worker-http-connect-timeout-ms must be >= 1")

    if args.worker_http_request_timeout_ms < 1:
        raise SystemExit("--worker-http-request-timeout-ms must be >= 1")

    if args.worker_outbound_http_permits < 1:
        raise SystemExit("--worker-outbound-http-permits must be >= 1")

    if args.host_health_parallelism < 1:
        raise SystemExit("--host-health-parallelism must be >= 1")

    if args.singleton_min_count < 1:
        raise SystemExit("--singleton-min-count must be >= 1")

    if not (0 < args.singleton_fraction <= 1):
        raise SystemExit("--singleton-fraction must be between 0 and 1")

    if args.packed_clients_per_container < 1:
        raise SystemExit("--packed-clients-per-container must be >= 1")

    if args.packed_worker_internal_parallelism < 1:
        raise SystemExit("--packed-worker-internal-parallelism must be >= 1")

    if args.enable_external_devices and not args.devices_file:
        raise SystemExit("--enable-external-devices requires --devices-file")

    run_id = args.run_id or timestamped_run_id(args.workers)
    try:
        validate_run_id(run_id)
    except ValueError as e:
        raise SystemExit(str(e)) from e

    scenario = args.scenario
    output_dir_name = args.output_dir
    output_root = output_root_for(root, output_dir_name)
    run_dir = output_root / run_id
    if args.wipe_run_dir:
        safe_wipe_run_dir(run_dir, output_root, run_id)
    run_dir.mkdir(parents=True, exist_ok=True)

    project_name = sanitize_project_name(run_id)

    compose_tmp = root / f"docker-compose.{run_id}.generated.yml"
    workers_internal_tmp = root / f"workers.{run_id}.txt"
    workers_host_tmp = root / f"workers.{run_id}.host.txt"
    layout_tmp = root / f"worker_layout.{run_id}.json"

    terminal_output_path = run_dir / "terminal_output.txt"
    compose_logs_path = run_dir / "compose_services.log"

    generator = root / "scripts" / "generate_compose.py"
    if not generator.exists():
        raise SystemExit(f"Missing generator script: {generator}")

    compose_up = False
    failure_seen = False
    external_device_stop_required = False

    compose_env = build_compose_env(args)

    try:
        if args.force_cleanup_signal_ports:
            docker_cleanup_signal_containers(root)

        check_required_ports(args)

        if args.build_images:
            run_cmd(
                ["docker", "build", "--target", "kr-runtime", "-t", "signal-kr", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "relay-runtime", "-t", "signal-relay", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "worker-runtime", "-t", "signal-worker", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "runner-runtime", "-t", "signal-runner", "."],
                cwd=root,
            )

        generator_cmd = [
            sys.executable,
            str(generator),
            "--workers",
            str(args.workers),
            "--run-id",
            run_id,
            "--scenario",
            scenario,
            "--output-dir",
            output_dir_name,
            "--compose-out",
            str(compose_tmp),
            "--workers-out",
            str(workers_internal_tmp),
            "--workers-host-out",
            str(workers_host_tmp),
            "--project-name",
            project_name,
            "--base-worker-port",
            str(args.base_worker_port),
            "--kr-port",
            str(args.kr_port),
            "--relay-port",
            str(args.relay_port),
            "--bridge-count",
            str(args.bridge_count),
            "--worker-layout-mode",
            args.worker_layout_mode,
            "--singleton-min-count",
            str(args.singleton_min_count),
            "--singleton-fraction",
            str(args.singleton_fraction),
            "--packed-clients-per-container",
            str(args.packed_clients_per_container),
            "--singleton-selection-seed",
            str(args.singleton_selection_seed),
            "--singleton-selection-strategy",
            args.singleton_selection_strategy,
            "--worker-layout-out",
            str(layout_tmp),
            "--packed-worker-internal-parallelism",
            str(args.packed_worker_internal_parallelism),
        ]

        if args.runner_in_docker:
            generator_cmd.append("--include-runner")
        else:
            generator_cmd.append("--publish-workers")

        if args.include_netcheck:
            generator_cmd.append("--include-netcheck")

        run_cmd(generator_cmd, cwd=root)

        copy_if_exists(compose_tmp, run_dir / "docker-compose.generated.yml")
        copy_if_exists(workers_internal_tmp, run_dir / "workers.txt")
        copy_if_exists(workers_host_tmp, run_dir / "workers.host.txt")
        copy_if_exists(layout_tmp, run_dir / "worker_layout.json")
        warn_if_neighbor_cache_tight(layout_tmp)

        try:
            if args.startup_batch_size > 0:
                print("[compose] starting kr and relay")
                run_cmd(
                    ["docker", "compose", "-f", str(compose_tmp), "up", "-d", "kr", "relay"],
                    cwd=root,
                    env=compose_env,
                )
                compose_up = True

                if args.startup_batch_sleep_seconds > 0:
                    time.sleep(args.startup_batch_sleep_seconds)

                worker_services_result = subprocess.run(
                    ["docker", "compose", "-f", str(compose_tmp), "config", "--services"],
                    cwd=str(root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                all_services = [
                    s.strip()
                    for s in worker_services_result.stdout.splitlines()
                    if s.strip().startswith("worker-")
                ]

                for start in range(0, len(all_services), args.startup_batch_size):
                    batch = all_services[start:start + args.startup_batch_size]
                    print(
                        f"[compose] starting workers {batch[0]} .. {batch[-1]} "
                        f"({start + len(batch)}/{len(all_services)})"
                    )

                    run_cmd(
                        ["docker", "compose", "-f", str(compose_tmp), "up", "-d", *batch],
                        cwd=root,
                        env=compose_env,
                    )

                    if args.startup_batch_sleep_seconds > 0:
                        time.sleep(args.startup_batch_sleep_seconds)
            else:
                run_cmd(
                    ["docker", "compose", "-f", str(compose_tmp), "up", "-d"],
                    cwd=root,
                    env=compose_env,
                )
                compose_up = True

        except subprocess.CalledProcessError as e:
            failure_seen = True
            compose_up = True
            write_compose_logs(root, compose_tmp, compose_logs_path, append=False)
            collect_failure_diagnostics(
                root=root,
                compose_file=compose_tmp,
                run_dir=run_dir,
                args=args,
                compose_env=compose_env,
                project_name=project_name,
                terminal_output_path=terminal_output_path,
            )

            raise RuntimeError(
                "docker compose up failed.\n"
                f"See compose logs in: {compose_logs_path}\n"
                f"Original error: {e}"
            ) from e

        if args.post_startup_settle_seconds > 0:
            print(
                f"[compose] settling for {args.post_startup_settle_seconds:.1f}s "
                "before health checks",
                flush=True,
            )
            time.sleep(args.post_startup_settle_seconds)

        print(f"[health] waiting for kr on http://127.0.0.1:{args.kr_port}/health", flush=True)
        wait_for_health(
            f"http://127.0.0.1:{args.kr_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )
        print("[health] kr ok", flush=True)

        print(f"[health] waiting for relay on http://127.0.0.1:{args.relay_port}/health", flush=True)
        wait_for_health(
            f"http://127.0.0.1:{args.relay_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )
        print("[health] relay ok", flush=True)

        if args.include_netcheck:
            print("[netcheck] starting continuous network monitor", flush=True)
            run_cmd(
                ["docker", "compose", "-f", str(compose_tmp), "up", "-d", "netcheck"],
                cwd=root,
                env=compose_env,
            )
            print(f"[netcheck] writing continuous log to {run_dir / 'netcheck.log'}", flush=True)

        if args.runner_in_docker:
            print("[health] skipping host worker health checks; runner will check workers inside Docker network")
        else:
            wait_for_workers_health_parallel(
                read_worker_lines(workers_host_tmp),
                args.health_timeout_seconds,
                args.health_poll_seconds,
                args.host_health_parallelism,
            )

        # ---- External device orchestration ---------------------------------
        external_layout_clients: list[dict] = []
        external_layout_workers: list[dict] = []
        external_worker_lines: list[str] = []

        if args.enable_external_devices and args.devices_file:
            external_device_stop_required = not args.no_device_stop_after_run
            ext_clients, ext_workers, ext_lines = launch_external_devices(
                args, root, args.kr_port, args.relay_port, run_id,
            )
            external_layout_clients = ext_clients
            external_layout_workers = ext_workers
            external_worker_lines = ext_lines

            # Merge external devices into worker layout
            layout_path = run_dir / "worker_layout.json"
            if layout_path.exists():
                layout_data = json.loads(layout_path.read_text(encoding="utf-8"))
                layout_data["clients"] = insert_after_leader(
                    layout_data.get("clients", []),
                    external_layout_clients,
                )
                layout_data["physical_workers"].extend(external_layout_workers)
                layout_data["logical_worker_count"] = len(layout_data["clients"])
                layout_data["physical_worker_count"] = len(layout_data["physical_workers"])
                layout_path.write_text(json.dumps(layout_data, indent=2), encoding="utf-8")
                # Also update the temp layout file for the runner
                if layout_tmp.exists():
                    layout_tmp.write_text(json.dumps(layout_data, indent=2), encoding="utf-8")

            # Merge external devices into workers files
            combined_workers = run_dir / "workers.combined.txt"
            combined_host_workers = run_dir / "workers.combined.host.txt"

            # Start with Docker internal workers
            internal_lines = read_worker_lines(workers_internal_tmp) if workers_internal_tmp.exists() else []
            internal_lines = insert_after_leader(internal_lines, external_worker_lines)
            combined_workers.write_text("\n".join(internal_lines) + "\n", encoding="utf-8")

            # Host workers
            host_lines = read_worker_lines(workers_host_tmp) if workers_host_tmp.exists() else []
            host_lines = insert_after_leader(host_lines, external_worker_lines)
            combined_host_workers.write_text("\n".join(host_lines) + "\n", encoding="utf-8")

            print(f"[device] merged {len(external_layout_clients)} external device(s) into layout/workers files", flush=True)

        # ---- Decide which layout / workers files to use ----
        effective_layout_tmp = layout_tmp
        effective_workers_host_tmp = workers_host_tmp
        effective_workers_internal_tmp = workers_internal_tmp

        if args.enable_external_devices and external_worker_lines:
            combined_host = run_dir / "workers.combined.host.txt"
            combined_internal = run_dir / "workers.combined.txt"
            if combined_host.exists():
                effective_workers_host_tmp = combined_host
            if combined_internal.exists():
                effective_workers_internal_tmp = combined_internal

        layout_path_for_runner = f"/results/{run_id}/worker_layout.json" if args.runner_in_docker else str(effective_layout_tmp)
        workers_file_for_docker_runner = (
            f"/results/{run_id}/workers.combined.txt"
            if args.enable_external_devices and external_worker_lines
            else f"/results/{run_id}/workers.txt"
        )

        use_no_aggregate = args.no_aggregate or args.enable_external_devices

        if args.runner_in_docker:
            benchmark_cmd = [
                "docker",
                "compose",
                "-f",
                str(compose_tmp),
                "run",
                "--rm",
                "runner",
                "--kr-url",
                f"http://kr:{args.kr_port}",
                "--relay-url",
                f"http://relay:{args.relay_port}",
                "--workers-file",
                workers_file_for_docker_runner,
                "--worker-layout",
                layout_path_for_runner,
                "--min-size",
                str(args.min_size),
                "--max-size",
                str(args.max_size if args.max_size is not None else args.workers),
                "--step-size",
                str(args.step_size),
                "--roundtrips",
                str(args.roundtrips),
                "--app-rounds",
                str(args.app_rounds),
                "--max-app-samples-per-payload",
                str(args.max_app_samples_per_payload),
                "--payload-sizes",
                args.payload_sizes,
                "--worker-health-timeout-seconds",
                str(args.worker_health_timeout_seconds),
                "--worker-health-poll-ms",
                str(args.worker_health_poll_ms),
                "--max-fanout-parallelism",
                str(args.max_fanout_parallelism),
                "--min-fanout-parallelism",
                str(args.min_fanout_parallelism),
                *(["--fanout-adaptive"] if args.fanout_adaptive else []),
                *(["--no-fanout-adaptive"] if args.no_fanout_adaptive else []),
                "--fanout-error-rate-threshold",
                str(args.fanout_error_rate_threshold),
                "--fanout-p95-threshold-ms",
                str(args.fanout_p95_threshold_ms),
                "--http-pool-max-idle-per-host",
                str(args.http_pool_max_idle_per_host),
                *(["--preflight-only"] if args.preflight_only else []),
                *(["--profile-only-singletons"] if args.profile_only_singletons else []),
                *(["--no-aggregate"] if use_no_aggregate else []),
                "--run-id",
                run_id,
                "--scenario",
                scenario,
                "--output-dir",
                "/results",
            ]
        else:
            benchmark_cmd = [
                "cargo",
                "run",
                "--bin",
                "benchmark_runner_http_staircase",
                "--",
                "--kr-url",
                f"http://127.0.0.1:{args.kr_port}",
                "--relay-url",
                f"http://127.0.0.1:{args.relay_port}",
                "--workers-file",
                str(effective_workers_host_tmp),
                "--worker-layout",
                layout_path_for_runner,
                "--min-size",
                str(args.min_size),
                "--max-size",
                str(args.max_size if args.max_size is not None else args.workers),
                "--step-size",
                str(args.step_size),
                "--roundtrips",
                str(args.roundtrips),
                "--app-rounds",
                str(args.app_rounds),
                "--max-app-samples-per-payload",
                str(args.max_app_samples_per_payload),
                "--payload-sizes",
                args.payload_sizes,
                "--worker-health-timeout-seconds",
                str(args.worker_health_timeout_seconds),
                "--worker-health-poll-ms",
                str(args.worker_health_poll_ms),
                "--max-fanout-parallelism",
                str(args.max_fanout_parallelism),
                "--min-fanout-parallelism",
                str(args.min_fanout_parallelism),
                *(["--fanout-adaptive"] if args.fanout_adaptive else []),
                *(["--no-fanout-adaptive"] if args.no_fanout_adaptive else []),
                "--fanout-error-rate-threshold",
                str(args.fanout_error_rate_threshold),
                "--fanout-p95-threshold-ms",
                str(args.fanout_p95_threshold_ms),
                "--http-pool-max-idle-per-host",
                str(args.http_pool_max_idle_per_host),
                *(["--preflight-only"] if args.preflight_only else []),
                *(["--profile-only-singletons"] if args.profile_only_singletons else []),
                *(["--no-aggregate"] if use_no_aggregate else []),
                "--run-id",
                run_id,
                "--scenario",
                scenario,
                "--output-dir",
                output_dir_name,
            ]

        print("[runner] starting benchmark runner", flush=True)
        print("[runner] " + " ".join(benchmark_cmd), flush=True)

        exit_code = tee_subprocess_output(
            benchmark_cmd,
            cwd=root,
            output_path=terminal_output_path,
            env=compose_env,
        )

        if exit_code != 0:
            failure_seen = True
            collect_failure_diagnostics(
                root=root,
                compose_file=compose_tmp,
                run_dir=run_dir,
                args=args,
                compose_env=compose_env,
                project_name=project_name,
                terminal_output_path=terminal_output_path,
            )
            raise RuntimeError(f"Benchmark runner exited with code {exit_code}")

        # ---- Post-run: pull external device profiles -----------------------
        if args.enable_external_devices and args.devices_file:
            settle = 3.0
            print(f"[device] settling {settle}s for external device workers to finish writing profiles...", flush=True)
            time.sleep(settle)
            print("[device] pulling external device profiles...", flush=True)
            pull_external_device_profiles(args, root, run_id, run_dir)

            # Run standalone aggregation if --no-aggregate was used
            if use_no_aggregate and not args.preflight_only:
                layout_file = run_dir / "worker_layout.json"
                workers_file = run_dir / "workers.combined.txt"
                agg_exit = run_standalone_aggregation(run_dir, layout_file if layout_file.exists() else None, workers_file if workers_file.exists() else None)
                if agg_exit != 0:
                    print(f"[aggregate] standalone aggregation had non-zero exit, but continuing", flush=True)

        if not args.preflight_only:
            validate_artifacts(run_dir, args.worker_layout_mode)
        else:
            print("[preflight] skipping artifact validation because --preflight-only was used")

        # Stop external device workers
        if external_device_stop_required:
            stop_external_device_workers(args, root)
            external_device_stop_required = False

        write_compose_logs(root, compose_tmp, compose_logs_path, append=False)

        print("")
        print(f"Run complete: {run_id}")
        print(f"Results: {run_dir}")
        return 0

    except Exception as e:
        failure_seen = True
        print(
            f"[error] benchmark orchestration failed before cleanup: "
            f"{type(e).__name__}: {e}",
            file=sys.stderr,
            flush=True,
        )
        raise

    finally:
        if external_device_stop_required:
            stop_external_device_workers(args, root)

        if compose_up:
            try:
                write_compose_logs(root, compose_tmp, compose_logs_path, append=True)
            except Exception:
                pass

        keep_stack = args.keep_stack_up or (failure_seen and args.keep_stack_up_on_failure)
        if not keep_stack:
            compose_down(
                root=root,
                compose_file=compose_tmp,
                args=args,
                env=compose_env,
            )
        elif compose_up:
            print("[compose] keeping stack up", flush=True)

        if not args.keep_generated_files:
            for path in (compose_tmp, workers_internal_tmp, workers_host_tmp, layout_tmp):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
