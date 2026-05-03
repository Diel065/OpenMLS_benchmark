#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Generate a docker-compose file plus worker lists for many MLS workers."
        )
    )

    p.add_argument("--workers", type=int, required=True, help="Number of worker services to generate")
    p.add_argument("--run-id", default="compose-generated-001", help="Default run id baked into the compose env")
    p.add_argument("--scenario", default="http-staircase-compose", help="Default scenario baked into the compose env")
    p.add_argument("--output-dir", default="benchmark_output", help="Host results directory")
    p.add_argument("--compose-out", default="docker-compose.generated.yml", help="Generated compose file path")
    p.add_argument("--workers-out", default="workers.txt", help="Generated internal worker list path")
    p.add_argument("--workers-host-out", default="workers.host.txt", help="Generated host worker list path")
    p.add_argument("--project-name", default="mls-benchmark", help="Compose project name")
    p.add_argument("--base-worker-port", type=int, default=8081, help="First published host port for workers")
    p.add_argument("--ds-port", type=int, default=3000, help="Published DS port")
    p.add_argument("--relay-port", type=int, default=4000, help="Published relay port")

    p.add_argument(
        "--bridge-count",
        type=int,
        default=1,
        help=(
            "Number of Docker bridge networks to distribute workers across. "
            "Workers are assigned as evenly as possible across these bridges."
        ),
    )

    p.add_argument(
        "--publish-workers",
        action="store_true",
        help="Publish every worker on a host port. Disable this for large runs.",
    )

    p.add_argument(
        "--include-runner",
        action="store_true",
        help="Include a runner service that can run inside the Docker network.",
    )

    p.add_argument(
        "--include-netcheck",
        action="store_true",
        help=(
            "Include a debug netcheck service attached to all benchmark networks. "
            "Useful for in-network DNS/HTTP diagnostics."
        ),
    )

    return p


def worker_id(i: int) -> str:
    return f"{i:05d}"


def service_name(i: int) -> str:
    return f"worker-{worker_id(i)}"


def bridge_name(i: int) -> str:
    return f"bench-net-{i:03d}"


def bridge_names(args: argparse.Namespace) -> list[str]:
    return [bridge_name(i) for i in range(args.bridge_count)]


def worker_bridge_index(args: argparse.Namespace, worker_index: int) -> int:
    return ((worker_index - 1) * args.bridge_count) // args.workers


def worker_bridge_name(args: argparse.Namespace, worker_index: int) -> str:
    return bridge_name(worker_bridge_index(args, worker_index))


def validate_args(args: argparse.Namespace) -> None:
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")
    if args.bridge_count < 1:
        raise SystemExit("--bridge-count must be at least 1")
    if args.bridge_count > args.workers:
        raise SystemExit("--bridge-count must not exceed --workers")
    if not (1 <= args.base_worker_port <= 65535):
        raise SystemExit("--base-worker-port must be between 1 and 65535")
    if not (1 <= args.ds_port <= 65535):
        raise SystemExit("--ds-port must be between 1 and 65535")
    if not (1 <= args.relay_port <= 65535):
        raise SystemExit("--relay-port must be between 1 and 65535")

    if args.publish_workers:
        last_port = args.base_worker_port + args.workers - 1
        if last_port > 65535:
            raise SystemExit(
                f"Worker host ports would exceed 65535: last port would be {last_port}"
            )

def append_netcheck_service(
        lines: list[str],
        *,
        args: argparse.Namespace,
        bridges: list[str],
) -> None:
    monitor_script = f"""\
set +e

RUN_ID="{args.run_id}"
OUT="/results/$RUN_ID/netcheck.log"
WORKERS="/results/$RUN_ID/workers.txt"

INTERVAL_SECONDS=5
FULL_SCAN_EVERY_SECONDS=60

mkdir -p "/results/$RUN_ID"
: > "$OUT"
exec >> "$OUT" 2>&1

ts() {{ date -u +"%Y-%m-%dT%H:%M:%SZ"; }}
log() {{ echo "[$(ts)] $*"; }}

check_url() {{
  label="$1"
  url="$2"

  if curl -fsS --connect-timeout 1 --max-time 3 "$url" >/dev/null; then
    log "OK $label $url"
  else
    rc=$?
    log "FAIL $label $url curl_exit=$rc"
  fi
}}

snapshot() {{
  log "======================================================================"
  log "NETCHECK MONITOR START"
  log "======================================================================"

  log "hostname=$(hostname)"

  log "--- /etc/resolv.conf ---"
  cat /etc/resolv.conf || true

  log "--- ip addr ---"
  ip addr || true

  log "--- ip route ---"
  ip route || true

  log "--- ss -tulpen ---"
  ss -tulpen || true

  log "--- workers.txt ---"
  if [ -f "$WORKERS" ]; then
    wc -l "$WORKERS" || true
    head -n 20 "$WORKERS" || true
    tail -n 20 "$WORKERS" || true
  else
    log "MISSING workers file: $WORKERS"
  fi
}}

worker_scan() {{
  mode="$1"

  if [ ! -f "$WORKERS" ]; then
    log "WORKER_SCAN mode=$mode missing_workers_file=$WORKERS"
    return 0
  fi

  total=0
  checked=0
  failed=0

  while IFS="=" read -r id url; do
    [ -z "$id" ] && continue

    total=$((total + 1))
    do_check=0

    if [ "$mode" = "full" ]; then
      do_check=1
    else
      if [ "$total" -le 10 ]; then
        do_check=1
      fi

      if [ $((total % 250)) -eq 0 ]; then
        do_check=1
      fi
    fi

    if [ "$do_check" -eq 1 ]; then
      checked=$((checked + 1))

      if ! curl -fsS --connect-timeout 1 --max-time 3 "$url/health" >/dev/null; then
        rc=$?
        host=$(printf "%s" "$url" | cut -d/ -f3 | cut -d: -f1)
        dns=$(getent hosts "$host" 2>/dev/null || true)

        log "WORKER_FAIL mode=$mode id=$id url=$url host=$host curl_exit=$rc dns=$dns"

        failed=$((failed + 1))
      fi
    fi
  done < "$WORKERS"

  healthy=$((checked - failed))

  log "WORKER_SCAN mode=$mode total_workers=$total checked=$checked healthy_checked=$healthy failed_checked=$failed"
}}

trap 'log "NETCHECK MONITOR RECEIVED STOP SIGNAL"; exit 0' TERM INT

snapshot

last_full=0
iteration=0

while true; do
  iteration=$((iteration + 1))
  now=$(date +%s)

  log "HEARTBEAT iteration=$iteration"

  check_url "ds" "http://ds:{args.ds_port}/health"
  check_url "relay" "http://relay:{args.relay_port}/health"

  if [ "$last_full" -eq 0 ] || [ $((now - last_full)) -ge "$FULL_SCAN_EVERY_SECONDS" ]; then
    worker_scan full
    last_full="$now"
  else
    worker_scan sample
  fi

  sleep "$INTERVAL_SECONDS" &
  wait $!
done
"""

    # Docker Compose interpolates $VAR inside YAML values before the shell sees it.
    # Escape shell dollars so the netcheck container receives the script unchanged.
    monitor_script = monitor_script.replace("$", "$$")

    lines.append("")
    lines.append("  netcheck:")
    lines.append("    image: nicolaka/netshoot:latest")
    lines.append("    depends_on:")
    lines.append("      - ds")
    lines.append("      - relay")
    lines.append("    volumes:")
    lines.append(f"      - ./{args.output_dir}:/results")
    lines.append("    command:")
    lines.append("      - sh")
    lines.append("      - -lc")
    lines.append("      - |")
    for raw_line in monitor_script.strip("\n").splitlines():
        lines.append(f"        {raw_line}")
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")


def append_bounded_logging(lines: list[str], indent: str = "    ") -> None:
    lines.append(f"{indent}logging:")
    lines.append(f'{indent}  driver: "local"')
    lines.append(f"{indent}  options:")
    lines.append(f'{indent}    max-size: "10m"')
    lines.append(f'{indent}    max-file: "3"')


def generate_compose_text(args: argparse.Namespace) -> str:
    lines: list[str] = []
    bridges = bridge_names(args)

    lines.append(f"name: {args.project_name}")
    lines.append("")
    lines.append("x-worker-common: &worker-common")
    lines.append("  image: mls-worker")
    lines.append("  environment:")
    lines.append(f"    OPENMLS_PROFILE_RUN_ID: {args.run_id}")
    lines.append(f"    OPENMLS_PROFILE_SCENARIO: {args.scenario}")
    lines.append("  depends_on:")
    lines.append("    - ds")
    lines.append("    - relay")
    lines.append("  volumes:")
    lines.append(f"    - ./{args.output_dir}:/results")
    append_bounded_logging(lines, indent="  ")
    lines.append("")
    lines.append("services:")

    lines.append("  ds:")
    lines.append("    image: mls-ds")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.ds_port}"]')
    lines.append("    ports:")
    lines.append(f'      - "{args.ds_port}:{args.ds_port}"')
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")
    append_bounded_logging(lines)
    lines.append("")

    lines.append("  relay:")
    lines.append("    image: mls-relay")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.relay_port}"]')
    lines.append("    ports:")
    lines.append(f'      - "{args.relay_port}:{args.relay_port}"')
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")
    append_bounded_logging(lines)

    for i in range(1, args.workers + 1):
        wid = worker_id(i)
        svc = service_name(i)
        host_port = args.base_worker_port + i - 1

        lines.append("")
        lines.append(f"  {svc}:")
        lines.append("    <<: *worker-common")
        lines.append("    command:")
        lines.append('      - "--name"')
        lines.append(f'      - "{wid}"')
        lines.append('      - "--ds-url"')
        lines.append(f'      - "http://ds:{args.ds_port}"')
        lines.append('      - "--relay-url"')
        lines.append(f'      - "http://relay:{args.relay_port}"')
        lines.append('      - "--listen-addr"')
        lines.append('      - "0.0.0.0:8080"')
        lines.append("    environment:")
        lines.append(f"      OPENMLS_PROFILE_RUN_ID: {args.run_id}")
        lines.append(f"      OPENMLS_PROFILE_SCENARIO: {args.scenario}")
        lines.append(f"      OPENMLS_PROFILE_PATH: /results/{args.run_id}/client-{wid}.jsonl")

        if args.publish_workers:
            lines.append("    ports:")
            lines.append(f'      - "{host_port}:8080"')

        lines.append("    networks:")
        lines.append(f"      - {worker_bridge_name(args, i)}")

    # Important: this block must be OUTSIDE the worker loop.
    # Otherwise the generated compose file gets one duplicate runner service per worker.
    if args.include_runner:
        lines.append("")
        lines.append("  runner:")
        lines.append("    image: mls-runner")
        lines.append("    depends_on:")
        lines.append("      - ds")
        lines.append("      - relay")
        lines.append("    volumes:")
        lines.append(f"      - ./{args.output_dir}:/results")
        lines.append("    networks:")
        for bridge in bridges:
            lines.append(f"      - {bridge}")
        append_bounded_logging(lines)

    if args.include_netcheck:
        append_netcheck_service(lines, args=args, bridges=bridges)
        append_bounded_logging(lines)

    lines.append("")
    lines.append("networks:")
    for bridge in bridges:
        lines.append(f"  {bridge}:")
        lines.append("    driver: bridge")

    return "\n".join(lines) + "\n"


def generate_workers_internal(args: argparse.Namespace) -> str:
    lines = []
    for i in range(1, args.workers + 1):
        wid = worker_id(i)
        svc = service_name(i)
        lines.append(f"{wid}=http://{svc}:8080")
    return "\n".join(lines) + "\n"


def generate_workers_host(args: argparse.Namespace) -> str:
    lines = []
    for i in range(1, args.workers + 1):
        wid = worker_id(i)
        host_port = args.base_worker_port + i - 1
        lines.append(f"{wid}=http://127.0.0.1:{host_port}")
    return "\n".join(lines) + "\n"


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args)

    compose_out = Path(args.compose_out)
    workers_out = Path(args.workers_out)
    workers_host_out = Path(args.workers_host_out)

    write_text(compose_out, generate_compose_text(args))
    write_text(workers_out, generate_workers_internal(args))
    write_text(workers_host_out, generate_workers_host(args))

    print(f"Wrote {compose_out}")
    print(f"Wrote {workers_out}")
    print(f"Wrote {workers_host_out}")
    print("")
    print("What you generated:")
    print(f"- Compose file with {args.workers} workers")
    print(f"- workers distributed across {args.bridge_count} Docker bridge network(s)")
    print("- workers.txt for an in-network runner")
    print("- workers.host.txt for the host-runner workflow")

    if args.publish_workers:
        print("- worker ports are published on the host")
    else:
        print("- worker ports are internal-only")

    if args.include_runner:
        print("- runner service included")

    if args.include_netcheck:
        print("- netcheck monitor service included")


if __name__ == "__main__":
    main()
