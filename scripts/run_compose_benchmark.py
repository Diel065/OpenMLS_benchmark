#!/usr/bin/env python3
from __future__ import annotations

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

    p.add_argument("--health-timeout-seconds", type=int, default=90)
    p.add_argument("--health-poll-seconds", type=float, default=0.5)

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
        subprocess.run(
            ["docker", "rm", "-f", *container_ids],
            cwd=str(root),
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
        subprocess.run(
            ["docker", "network", "rm", *network_names],
            cwd=str(root),
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


def main() -> int:
    args = build_parser().parse_args()
    root = repo_root()

    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

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
        ]
        run_cmd(generator_cmd, cwd=root)

        copy_if_exists(compose_tmp, run_dir / "docker-compose.generated.yml")
        copy_if_exists(workers_internal_tmp, run_dir / "workers.txt")
        copy_if_exists(workers_host_tmp, run_dir / "workers.host.txt")

        try:
            run_cmd(
                ["docker", "compose", "-f", str(compose_tmp), "up", "-d"],
                cwd=root,
            )
            compose_up = True
        except subprocess.CalledProcessError as e:
            write_compose_logs(root, compose_tmp, compose_logs_path, append=False)
            subprocess.run(
                ["docker", "compose", "-f", str(compose_tmp), "down"],
                cwd=str(root),
                check=False,
            )
            raise RuntimeError(
                "docker compose up failed.\n"
                f"See compose logs in: {compose_logs_path}\n"
                f"Original error: {e}"
            ) from e

        wait_for_health(
            f"http://127.0.0.1:{args.ds_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )
        wait_for_health(
            f"http://127.0.0.1:{args.relay_port}/health",
            args.health_timeout_seconds,
            args.health_poll_seconds,
        )

        for line in read_worker_lines(workers_host_tmp):
            worker_id, worker_url = line.split("=", 1)
            wait_for_health(
                f"{worker_url}/health",
                args.health_timeout_seconds,
                args.health_poll_seconds,
            )
            print(f"[health] worker {worker_id} ok")

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
            "--run-id",
            run_id,
            "--scenario",
            scenario,
            "--output-dir",
            output_dir_name,
        ]

        exit_code = tee_subprocess_output(
            benchmark_cmd,
            cwd=root,
            output_path=terminal_output_path,
        )
        if exit_code != 0:
            raise RuntimeError(f"Benchmark runner exited with code {exit_code}")

        validate_artifacts(run_dir)
        write_compose_logs(root, compose_tmp, compose_logs_path, append=False)

        print("")
        print(f"Run complete: {run_id}")
        print(f"Results: {run_dir}")
        return 0

    finally:
        if compose_up:
            try:
                write_compose_logs(root, compose_tmp, compose_logs_path, append=True)
            except Exception:
                pass

            if not args.keep_stack_up:
                subprocess.run(
                    ["docker", "compose", "-f", str(compose_tmp), "down"],
                    cwd=str(root),
                    check=False,
                )

        if not args.keep_generated_files:
            for path in (compose_tmp, workers_internal_tmp, workers_host_tmp):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())