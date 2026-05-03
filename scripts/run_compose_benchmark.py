#!/usr/bin/env python3
from __future__ import annotations

import os
import argparse
import datetime as dt
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="One-command local containerized MLS benchmark runner."
    )

    p.add_argument("--workers", type=int, required=True, help="Number of worker containers")
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
    p.add_argument("--ds-port", type=int, default=3000)
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
            "to become healthy before starting MLS logic."
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
            "0 lets the Rust runner choose a CPU-scaled default."
        ),
    )
    p.add_argument(
        "--http-pool-max-idle-per-host",
        type=int,
        default=32,
        help="Maximum idle pooled HTTP connections per host for the Rust runner.",
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
            "Only check DS/relay/worker reachability from inside the Docker network, "
            "then exit without running the MLS benchmark. No events.csv is expected."
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
        "--keep-generated-files",
        action="store_true",
        help="Keep temporary generated compose/worker files at repo root",
    )
    p.add_argument(
        "--force-cleanup-mls-ports",
        action="store_true",
        help="Before starting, forcibly remove existing Docker containers with names beginning with 'mls-'",
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
        cleaned = "mls-benchmark"
    return f"mls-{cleaned}"[:63]


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


def validate_artifacts(run_dir: Path) -> None:
    csv_path = run_dir / "events.csv"
    jsonl_files = sorted(run_dir.glob("client-*.jsonl"))

    if not csv_path.exists():
        raise RuntimeError(f"Missing aggregated CSV: {csv_path}")
    if csv_path.stat().st_size == 0:
        raise RuntimeError(f"Aggregated CSV is empty: {csv_path}")

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
    ports = [args.ds_port, args.relay_port]

    if not args.runner_in_docker:
        ports.extend(args.base_worker_port + i for i in range(args.workers))

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
        "You can also rerun with --force-cleanup-mls-ports to remove old mls-* Docker containers."
    )


def docker_cleanup_mls_containers(root: Path) -> None:
    result = subprocess.run(
        ["docker", "ps", "-aq", "--filter", "name=mls-"],
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if container_ids:
        print(f"[cleanup] removing {len(container_ids)} old mls-* containers")

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
        if line.strip().startswith("mls-")
    ]

    if network_names:
        print(f"[cleanup] removing {len(network_names)} old mls-* networks")

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


def worker_service_names(worker_count: int) -> list[str]:
    return [f"worker-{i:05d}" for i in range(1, worker_count + 1)]


def chunks(items: list[str], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


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

    # If the benchmark runner container is still alive because of an interrupt/error,
    # stop it first so it does not keep sending requests while workers are removed.
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

    workers = worker_service_names(args.workers)
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

    print("[compose] final down for ds/relay/network")
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

    if args.http_pool_max_idle_per_host < 1:
        raise SystemExit("--http-pool-max-idle-per-host must be >= 1")

    if args.host_health_parallelism < 1:
        raise SystemExit("--host-health-parallelism must be >= 1")

    run_id = args.run_id or timestamped_run_id(args.workers)
    scenario = args.scenario
    output_dir_name = args.output_dir
    run_dir = root / output_dir_name / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    project_name = sanitize_project_name(run_id)

    compose_tmp = root / f"docker-compose.{run_id}.generated.yml"
    workers_internal_tmp = root / f"workers.{run_id}.txt"
    workers_host_tmp = root / f"workers.{run_id}.host.txt"

    terminal_output_path = run_dir / "terminal_output.txt"
    compose_logs_path = run_dir / "compose_services.log"

    generator = root / "scripts" / "generate_compose.py"
    if not generator.exists():
        raise SystemExit(f"Missing generator script: {generator}")

    compose_up = False

    compose_env = None
    if args.compose_parallel_limit is not None:
        compose_env = dict(os.environ)
        compose_env["COMPOSE_PARALLEL_LIMIT"] = str(args.compose_parallel_limit)

    try:
        if args.force_cleanup_mls_ports:
            docker_cleanup_mls_containers(root)

        check_required_ports(args)

        if args.build_images:
            run_cmd(
                ["docker", "build", "--target", "ds-runtime", "-t", "mls-ds", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "relay-runtime", "-t", "mls-relay", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "worker-runtime", "-t", "mls-worker", "."],
                cwd=root,
            )
            run_cmd(
                ["docker", "build", "--target", "runner-runtime", "-t", "mls-runner", "."],
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
            "--ds-port",
            str(args.ds_port),
            "--relay-port",
            str(args.relay_port),
            "--bridge-count",
            str(args.bridge_count),
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

        compose_env = None
        if args.compose_parallel_limit is not None:
            compose_env = os.environ.copy()
            compose_env["COMPOSE_PARALLEL_LIMIT"] = str(args.compose_parallel_limit)

        try:
            if args.startup_batch_size > 0:
                print("[compose] starting ds and relay")
                run_cmd(
                    ["docker", "compose", "-f", str(compose_tmp), "up", "-d", "ds", "relay"],
                    cwd=root,
                    env=compose_env,
                )
                compose_up = True

                if args.startup_batch_sleep_seconds > 0:
                    time.sleep(args.startup_batch_sleep_seconds)

                worker_services = [
                    f"worker-{i:05d}"
                    for i in range(1, args.workers + 1)
                ]

                for start in range(0, len(worker_services), args.startup_batch_size):
                    batch = worker_services[start:start + args.startup_batch_size]
                    print(
                        f"[compose] starting workers {batch[0]} .. {batch[-1]} "
                        f"({start + len(batch)}/{len(worker_services)})"
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
            write_compose_logs(root, compose_tmp, compose_logs_path, append=False)
            compose_down(
                root=root,
                compose_file=compose_tmp,
                args=args,
                env=compose_env,
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

        print(f"[health] waiting for ds on http://127.0.0.1:{args.ds_port}/health", flush=True)
        wait_for_health(
            f"http://127.0.0.1:{args.ds_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )
        print("[health] ds ok", flush=True)

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

        if args.runner_in_docker:
            benchmark_cmd = [
                "docker",
                "compose",
                "-f",
                str(compose_tmp),
                "run",
                "--rm",
                "runner",
                "--ds-url",
                f"http://ds:{args.ds_port}",
                "--workers-file",
                f"/results/{run_id}/workers.txt",
                "--min-size",
                str(args.min_size),
                "--max-size",
                str(args.max_size if args.max_size is not None else args.workers),
                "--step-size",
                str(args.step_size),
                "--roundtrips",
                str(args.roundtrips),
                "--update-rounds",
                str(args.update_rounds),
                "--max-update-samples-per-plateau",
                str(args.max_update_samples_per_plateau),
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
                "--http-pool-max-idle-per-host",
                str(args.http_pool_max_idle_per_host),
                *(["--preflight-only"] if args.preflight_only else []),
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
                "--ds-url",
                f"http://127.0.0.1:{args.ds_port}",
                "--workers-file",
                str(workers_host_tmp),
                "--min-size",
                str(args.min_size),
                "--max-size",
                str(args.max_size if args.max_size is not None else args.workers),
                "--step-size",
                str(args.step_size),
                "--roundtrips",
                str(args.roundtrips),
                "--update-rounds",
                str(args.update_rounds),
                "--max-update-samples-per-plateau",
                str(args.max_update_samples_per_plateau),
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
                "--http-pool-max-idle-per-host",
                str(args.http_pool_max_idle_per_host),
                *(["--preflight-only"] if args.preflight_only else []),
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
            env=compose_env if args.runner_in_docker else None,
        )

        if exit_code != 0:
            raise RuntimeError(f"Benchmark runner exited with code {exit_code}")

        if not args.preflight_only:
            validate_artifacts(run_dir)
        else:
            print("[preflight] skipping artifact validation because --preflight-only was used")

        write_compose_logs(root, compose_tmp, compose_logs_path, append=False)

        print("")
        print(f"Run complete: {run_id}")
        print(f"Results: {run_dir}")
        return 0

    except Exception as e:
        print(
            f"[error] benchmark orchestration failed before cleanup: "
            f"{type(e).__name__}: {e}",
            file=sys.stderr,
            flush=True,
        )
        raise

    finally:
        if compose_up:
            try:
                write_compose_logs(root, compose_tmp, compose_logs_path, append=True)
            except Exception:
                pass

        if not args.keep_stack_up:
            compose_down(
                root=root,
                compose_file=compose_tmp,
                args=args,
                env=compose_env,
            )

        if not args.keep_generated_files:
            for path in (compose_tmp, workers_internal_tmp, workers_host_tmp):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
