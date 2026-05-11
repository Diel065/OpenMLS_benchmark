#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import random
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Generate a docker-compose file plus worker lists for many Signal benchmark workers."
        )
    )

    p.add_argument("--workers", type=int, required=True, help="Number of logical worker clients")
    p.add_argument("--run-id", default="compose-generated-001", help="Default run id baked into the compose env")
    p.add_argument("--scenario", default="http-staircase-compose", help="Default scenario label baked into the compose env")
    p.add_argument("--output-dir", default="benchmark_output", help="Host results directory")
    p.add_argument("--compose-out", default="docker-compose.generated.yml", help="Generated compose file path")
    p.add_argument("--workers-out", default="workers.txt", help="Generated internal worker list path")
    p.add_argument("--workers-host-out", default="workers.host.txt", help="Generated host worker list path")
    p.add_argument("--project-name", default="signal-benchmark", help="Compose project name")
    p.add_argument("--base-worker-port", type=int, default=8081, help="First published host port for workers")
    p.add_argument("--kr-port", type=int, default=3000, help="Published KR port")
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
        "--worker-layout-out",
        default="worker_layout.json",
        help="Output path for the worker layout JSON artifact",
    )
    p.add_argument(
        "--packed-worker-internal-parallelism",
        type=int,
        default=4,
        help="Internal parallelism for packed worker containers",
    )

    return p


def worker_id(i: int) -> str:
    return f"{i:05d}"


def service_name(i: int) -> str:
    return f"worker-{worker_id(i)}"


def bridge_name(i: int) -> str:
    return f"bench-net-{i:03d}"


def bridges_from_args(args: argparse.Namespace) -> list[str]:
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
    if not (1 <= args.kr_port <= 65535):
        raise SystemExit("--kr-port must be between 1 and 65535")
    if not (1 <= args.relay_port <= 65535):
        raise SystemExit("--relay-port must be between 1 and 65535")

    if args.publish_workers:
        last_port = args.base_worker_port + args.workers - 1
        if last_port > 65535:
            raise SystemExit(
                f"Worker host ports would exceed 65535: last port would be {last_port}"
            )

    if args.worker_layout_mode == "hybrid":
        if args.singleton_min_count < 1:
            raise SystemExit("--singleton-min-count must be at least 1")
        if not (0 < args.singleton_fraction <= 1):
            raise SystemExit("--singleton-fraction must be between 0 and 1")
        if args.packed_clients_per_container < 1:
            raise SystemExit("--packed-clients-per-container must be at least 1")
        if args.packed_worker_internal_parallelism < 1:
            raise SystemExit("--packed-worker-internal-parallelism must be at least 1")


# ── Hybrid layout calculation ──────────────────────────────────────────

def compute_hybrid_layout(
    total_logical_workers: int,
    singleton_min_count: int,
    singleton_fraction: float,
    packed_clients_per_container: int,
) -> dict:
    singleton_count = min(
        total_logical_workers,
        max(singleton_min_count, math.ceil(total_logical_workers * singleton_fraction)),
    )
    packed_client_count = total_logical_workers - singleton_count
    packed_container_count = math.ceil(packed_client_count / packed_clients_per_container) if packed_client_count > 0 else 0
    physical_worker_count = singleton_count + packed_container_count

    return {
        "singleton_count": singleton_count,
        "packed_client_count": packed_client_count,
        "packed_container_count": packed_container_count,
        "physical_worker_count": physical_worker_count,
    }


# ── Singleton selection ────────────────────────────────────────────────

def select_singleton_ids(
    total_logical_workers: int,
    singleton_count: int,
    seed: int,
    strategy: str,
) -> list[str]:
    if singleton_count >= total_logical_workers:
        return [worker_id(i) for i in range(1, total_logical_workers + 1)]

    singletons = {worker_id(1)}

    if singleton_count == 1:
        return sorted(singletons)

    remaining_slots = singleton_count - 1
    candidate_ids = list(range(2, total_logical_workers + 1))

    if strategy == "evenly-spaced":
        strata_size = len(candidate_ids) / remaining_slots
        for i in range(remaining_slots):
            idx = int(i * strata_size + strata_size / 2)
            idx = min(idx, len(candidate_ids) - 1)
            singletons.add(worker_id(candidate_ids[idx]))
    else:
        rng = random.Random(seed)
        strata_size = len(candidate_ids) / remaining_slots
        for i in range(remaining_slots):
            start = int(i * strata_size)
            end = int((i + 1) * strata_size) if i < remaining_slots - 1 else len(candidate_ids)
            chosen = rng.choice(candidate_ids[start:end])
            singletons.add(worker_id(chosen))

    return sorted(singletons)


# ── Layout builder ─────────────────────────────────────────────────────

class ClientLayoutEntry:
    def __init__(
        self,
        client_id: str,
        physical_worker_id: str,
        container_mode: str,
        profile_enabled: bool,
        command_url: str,
        health_url: str,
    ):
        self.client_id = client_id
        self.physical_worker_id = physical_worker_id
        self.container_mode = container_mode
        self.profile_enabled = profile_enabled
        self.command_url = command_url
        self.health_url = health_url

    def to_dict(self) -> dict:
        return {
            "client_id": self.client_id,
            "physical_worker_id": self.physical_worker_id,
            "container_mode": self.container_mode,
            "profile_enabled": self.profile_enabled,
            "command_url": self.command_url,
            "health_url": self.health_url,
        }


class PhysicalWorkerEntry:
    def __init__(
        self,
        physical_worker_id: str,
        container_mode: str,
        client_ids: list[str],
        base_url: str,
        profile_enabled_client_ids: list[str],
    ):
        self.physical_worker_id = physical_worker_id
        self.container_mode = container_mode
        self.client_ids = client_ids
        self.base_url = base_url
        self.profile_enabled_client_ids = profile_enabled_client_ids

    def to_dict(self) -> dict:
        return {
            "physical_worker_id": self.physical_worker_id,
            "container_mode": self.container_mode,
            "client_ids": self.client_ids,
            "base_url": self.base_url,
            "profile_enabled_client_ids": self.profile_enabled_client_ids,
        }


def build_hybrid_layout(
    args: argparse.Namespace,
    singleton_ids: list[str],
    packed_client_ids: list[str],
    layout_info: dict,
) -> tuple[list[ClientLayoutEntry], list[PhysicalWorkerEntry]]:
    singleton_set = set(singleton_ids)
    clients: list[ClientLayoutEntry] = []
    physical_workers: list[PhysicalWorkerEntry] = []

    singleton_counter = 0
    for cid in singleton_ids:
        sid = f"worker-{cid}"
        base_url = f"http://{sid}:8080"
        clients.append(ClientLayoutEntry(
            client_id=cid,
            physical_worker_id=sid,
            container_mode="singleton",
            profile_enabled=True,
            command_url=f"{base_url}/participant/{cid}",
            health_url=f"{base_url}/participant/{cid}/health",
        ))
        physical_workers.append(PhysicalWorkerEntry(
            physical_worker_id=sid,
            container_mode="singleton",
            client_ids=[cid],
            base_url=base_url,
            profile_enabled_client_ids=[cid],
        ))
        singleton_counter += 1

    pack_idx = 0
    clients_per_pack = args.packed_clients_per_container
    for start in range(0, len(packed_client_ids), clients_per_pack):
        pack_clients = packed_client_ids[start:start + clients_per_pack]
        pack_id = f"worker-pack-{pack_idx:03d}"
        base_url = f"http://{pack_id}:8080"

        for cid in pack_clients:
            clients.append(ClientLayoutEntry(
                client_id=cid,
                physical_worker_id=pack_id,
                container_mode="packed",
                profile_enabled=False,
                command_url=f"{base_url}/participant/{cid}",
                health_url=f"{base_url}/participant/{cid}/health",
            ))

        physical_workers.append(PhysicalWorkerEntry(
            physical_worker_id=pack_id,
            container_mode="packed",
            client_ids=pack_clients,
            base_url=base_url,
            profile_enabled_client_ids=[],
        ))
        pack_idx += 1

    return clients, physical_workers


def build_legacy_layout(
    args: argparse.Namespace,
) -> tuple[list[ClientLayoutEntry], list[PhysicalWorkerEntry]]:
    clients: list[ClientLayoutEntry] = []
    physical_workers: list[PhysicalWorkerEntry] = []

    for i in range(1, args.workers + 1):
        cid = worker_id(i)
        sid = f"worker-{cid}"
        base_url = f"http://{sid}:8080"
        clients.append(ClientLayoutEntry(
            client_id=cid,
            physical_worker_id=sid,
            container_mode="singleton",
            profile_enabled=True,
            command_url=f"{base_url}/participant/{cid}",
            health_url=f"{base_url}/participant/{cid}/health",
        ))
        physical_workers.append(PhysicalWorkerEntry(
            physical_worker_id=sid,
            container_mode="singleton",
            client_ids=[cid],
            base_url=base_url,
            profile_enabled_client_ids=[cid],
        ))

    return clients, physical_workers


def generate_worker_layout_json(
    args: argparse.Namespace,
    clients: list[ClientLayoutEntry],
    physical_workers: list[PhysicalWorkerEntry],
) -> dict:
    return {
        "version": 1,
        "logical_worker_count": args.workers,
        "physical_worker_count": len(physical_workers),
        "layout_mode": args.worker_layout_mode,
        "singleton_min_count": args.singleton_min_count,
        "singleton_fraction": args.singleton_fraction,
        "packed_clients_per_container": args.packed_clients_per_container,
        "singleton_selection_seed": args.singleton_selection_seed,
        "profile_policy": "singletons_only" if args.worker_layout_mode == "hybrid" else "all",
        "clients": [c.to_dict() for c in clients],
        "physical_workers": [pw.to_dict() for pw in physical_workers],
    }


# ── Compose generation ─────────────────────────────────────────────────

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
TARGETS="/results/$RUN_ID/netcheck_targets.txt"

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

      if [ $(((total + iteration) % 250)) -eq 0 ]; then
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

target_scan() {{
  if [ ! -f "$TARGETS" ] || [ ! -f "$WORKERS" ]; then
    return 0
  fi

  while read -r target_id; do
    [ -z "$target_id" ] && continue
    line=$(grep -E "^$target_id=" "$WORKERS" 2>/dev/null | head -n 1 || true)
    if [ -z "$line" ]; then
      log "TARGET_FAIL id=$target_id missing_from_workers_file=$WORKERS"
      continue
    fi

    id=$(printf "%s" "$line" | cut -d= -f1)
    url=$(printf "%s" "$line" | cut -d= -f2-)

    if curl -fsS --connect-timeout 1 --max-time 3 "$url/health" >/dev/null; then
      log "TARGET_OK id=$id url=$url"
    else
      rc=$?
      host=$(printf "%s" "$url" | cut -d/ -f3 | cut -d: -f1)
      dns=$(getent hosts "$host" 2>/dev/null || true)
      log "TARGET_FAIL id=$id url=$url host=$host curl_exit=$rc dns=$dns"
    fi
  done < "$TARGETS"
}}

trap 'log "NETCHECK MONITOR RECEIVED STOP SIGNAL"; exit 0' TERM INT

snapshot

last_full=0
iteration=0

while true; do
  iteration=$((iteration + 1))
  now=$(date +%s)

  log "HEARTBEAT iteration=$iteration"

  check_url "kr" "http://kr:{args.kr_port}/health"
  check_url "relay" "http://relay:{args.relay_port}/health"

  if [ "$last_full" -eq 0 ] || [ $((now - last_full)) -ge "$FULL_SCAN_EVERY_SECONDS" ]; then
    worker_scan full
    last_full="$now"
  else
    worker_scan sample
  fi

  target_scan

  sleep "$INTERVAL_SECONDS" &
  wait $!
done
"""

    monitor_script = monitor_script.replace("$", "$$")

    lines.append("")
    lines.append("  netcheck:")
    lines.append("    image: nicolaka/netshoot:latest")
    lines.append("    depends_on:")
    lines.append("      - kr")
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


def physical_worker_bridge_index(
    args: argparse.Namespace,
    physical_idx: int,
    total_physical: int,
) -> int:
    if total_physical <= 1:
        return 0
    return (physical_idx * args.bridge_count) // total_physical


def generate_compose_text(
    args: argparse.Namespace,
    physical_workers: list[PhysicalWorkerEntry],
) -> str:
    lines: list[str] = []
    bridges = bridges_from_args(args)

    lines.append(f"name: {args.project_name}")
    lines.append("")
    lines.append("x-worker-common: &worker-common")
    lines.append("  image: signal-worker")
    lines.append("  environment:")
    lines.append(f"    SIGNAL_PROFILE_RUN_ID: {args.run_id}")
    lines.append(f"    SIGNAL_PROFILE_SCENARIO: {args.scenario}")
    lines.append("    SIGNAL_DEBUG_LOGS: ${SIGNAL_DEBUG_LOGS:-}")
    lines.append("    SIGNAL_WORKER_DEBUG_IDS: ${SIGNAL_WORKER_DEBUG_IDS:-}")
    lines.append("    SIGNAL_WORKER_COMMAND_QUEUE_CAPACITY: ${SIGNAL_WORKER_COMMAND_QUEUE_CAPACITY:-}")
    lines.append("    SIGNAL_WORKER_IDEMPOTENCY_CACHE_SIZE: ${SIGNAL_WORKER_IDEMPOTENCY_CACHE_SIZE:-}")
    lines.append("    SIGNAL_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS: ${SIGNAL_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS:-}")
    lines.append("    SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST: ${SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST:-32}")
    lines.append("    SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS: ${SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS:-5000}")
    lines.append("    SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS: ${SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS:-30000}")
    lines.append("    SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS: ${SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS:-32}")
    lines.append("  depends_on:")
    lines.append("    - kr")
    lines.append("    - relay")
    lines.append("  volumes:")
    lines.append(f"    - ./{args.output_dir}:/results")
    append_bounded_logging(lines, indent="  ")
    lines.append("")
    lines.append("services:")

    lines.append("  kr:")
    lines.append("    image: signal-kr")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.kr_port}"]')
    lines.append("    environment:")
    lines.append("      SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT: ${SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT:-128}")
    lines.append("    ports:")
    lines.append(f'      - "{args.kr_port}:{args.kr_port}"')
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")
    append_bounded_logging(lines)
    lines.append("")

    lines.append("  relay:")
    lines.append("    image: signal-relay")
    lines.append(f'    command: ["--listen-addr", "0.0.0.0:{args.relay_port}"]')
    lines.append("    environment:")
    lines.append("      SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT: ${SIGNAL_SERVICE_METRICS_WARN_IN_FLIGHT:-128}")
    lines.append("    ports:")
    lines.append(f'      - "{args.relay_port}:{args.relay_port}"')
    lines.append("    networks:")
    for bridge in bridges:
        lines.append(f"      - {bridge}")
    append_bounded_logging(lines)

    total_physical = len(physical_workers)
    next_host_port = args.base_worker_port

    for pw_idx, pw in enumerate(physical_workers):
        lines.append("")
        lines.append(f"  {pw.physical_worker_id}:")
        lines.append("    <<: *worker-common")
        lines.append("    command:")
        lines.append('      - "--name"')
        lines.append(f'      - "{pw.physical_worker_id}"')

        participant_ids_csv = ",".join(pw.client_ids)
        lines.append('      - "--participants"')
        lines.append(f'      - "{participant_ids_csv}"')

        if pw.container_mode == "singleton" and pw.profile_enabled_client_ids:
            profile_csv = ",".join(pw.profile_enabled_client_ids)
            lines.append('      - "--profile-enabled-participant-ids"')
            lines.append(f'      - "{profile_csv}"')
            lines.append('      - "--profile-path-template"')
            lines.append(f'      - "/results/{args.run_id}/participant-{{participant_id}}.jsonl"')

        lines.append('      - "--kr-url"')
        lines.append(f'      - "http://kr:{args.kr_port}"')
        lines.append('      - "--relay-url"')
        lines.append(f'      - "http://relay:{args.relay_port}"')
        lines.append('      - "--listen-addr"')
        lines.append('      - "0.0.0.0:8080"')
        if pw.container_mode == "packed":
            lines.append('      - "--packed-worker-internal-parallelism"')
            lines.append(f'      - "{args.packed_worker_internal_parallelism}"')

        lines.append("    environment:")
        lines.append(f"      SIGNAL_PROFILE_RUN_ID: {args.run_id}")
        lines.append(f"      SIGNAL_PROFILE_SCENARIO: {args.scenario}")

        if pw.container_mode == "singleton" and pw.profile_enabled_client_ids:
            profile_csv = ",".join(pw.profile_enabled_client_ids)
            lines.append(f'      SIGNAL_PROFILE_ENABLED: "true"')
            lines.append(f'      SIGNAL_PROFILE_PARTICIPANT_IDS: "{profile_csv}"')
            lines.append(f'      SIGNAL_PROFILE_PATH_TEMPLATE: "/results/{args.run_id}/participant-{{participant_id}}.jsonl"')
            first_cid = pw.profile_enabled_client_ids[0]
            lines.append(f'      SIGNAL_PROFILE_PATH: "/results/{args.run_id}/participant-{first_cid}.jsonl"')
        else:
            lines.append('      SIGNAL_PROFILE_ENABLED: "false"')
            lines.append('      SIGNAL_PROFILE_PARTICIPANT_IDS: ""')
            lines.append('      SIGNAL_PROFILE_PATH_TEMPLATE: ""')

        if pw.container_mode == "packed":
            lines.append(f'      SIGNAL_PACKED_WORKER_INTERNAL_PARALLELISM: "{args.packed_worker_internal_parallelism}"')

        lines.append("      SIGNAL_DEBUG_LOGS: ${SIGNAL_DEBUG_LOGS:-}")
        lines.append("      SIGNAL_WORKER_DEBUG_IDS: ${SIGNAL_WORKER_DEBUG_IDS:-}")
        lines.append("      SIGNAL_WORKER_COMMAND_QUEUE_CAPACITY: ${SIGNAL_WORKER_COMMAND_QUEUE_CAPACITY:-}")
        lines.append("      SIGNAL_WORKER_IDEMPOTENCY_CACHE_SIZE: ${SIGNAL_WORKER_IDEMPOTENCY_CACHE_SIZE:-}")
        lines.append("      SIGNAL_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS: ${SIGNAL_WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS:-}")
        lines.append("      SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST: ${SIGNAL_WORKER_HTTP_POOL_MAX_IDLE_PER_HOST:-32}")
        lines.append("      SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS: ${SIGNAL_WORKER_HTTP_CONNECT_TIMEOUT_MS:-5000}")
        lines.append("      SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS: ${SIGNAL_WORKER_HTTP_REQUEST_TIMEOUT_MS:-30000}")
        lines.append("      SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS: ${SIGNAL_WORKER_OUTBOUND_HTTP_PERMITS:-32}")

        if args.publish_workers:
            lines.append("    ports:")
            lines.append(f'      - "{next_host_port}:8080"')
            next_host_port += 1

        bridge_idx = physical_worker_bridge_index(args, pw_idx, total_physical)
        lines.append("    networks:")
        lines.append(f"      - {bridge_name(bridge_idx)}")

    if args.include_runner:
        lines.append("")
        lines.append("  runner:")
        lines.append("    image: signal-runner")
        lines.append("    profiles:")
        lines.append("      - runner")
        lines.append("    depends_on:")
        lines.append("      - kr")
        lines.append("      - relay")
        lines.append("    environment:")
        lines.append("      SIGNAL_RUNNER_HTTP_CONNECT_TIMEOUT_MS: ${SIGNAL_RUNNER_HTTP_CONNECT_TIMEOUT_MS:-2000}")
        lines.append("      SIGNAL_RUNNER_HTTP_REQUEST_TIMEOUT_MS: ${SIGNAL_RUNNER_HTTP_REQUEST_TIMEOUT_MS:-60000}")
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


def generate_workers_internal(args: argparse.Namespace, clients: list[ClientLayoutEntry]) -> str:
    lines = []
    for c in clients:
        svc = c.physical_worker_id
        lines.append(f"{c.client_id}=http://{svc}:8080")
    return "\n".join(lines) + "\n"


def generate_workers_host(
    args: argparse.Namespace,
    physical_workers: list[PhysicalWorkerEntry],
) -> str:
    lines = []
    next_port = args.base_worker_port
    port_map: dict[str, int] = {}
    for pw in physical_workers:
        port_map[pw.physical_worker_id] = next_port
        next_port += 1

    for pw in physical_workers:
        port = port_map[pw.physical_worker_id]
        for cid in pw.client_ids:
            lines.append(f"{cid}=http://127.0.0.1:{port}")
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
    layout_out = Path(args.worker_layout_out)

    if args.worker_layout_mode == "hybrid":
        layout_info = compute_hybrid_layout(
            args.workers,
            args.singleton_min_count,
            args.singleton_fraction,
            args.packed_clients_per_container,
        )

        singleton_ids = select_singleton_ids(
            args.workers,
            layout_info["singleton_count"],
            args.singleton_selection_seed,
            args.singleton_selection_strategy,
        )

        all_ids = [worker_id(i) for i in range(1, args.workers + 1)]
        singleton_set = set(singleton_ids)
        packed_client_ids = [cid for cid in all_ids if cid not in singleton_set]

        clients, physical_workers = build_hybrid_layout(
            args, singleton_ids, packed_client_ids, layout_info,
        )

        print(f"[layout] logical_workers={args.workers} "
              f"singleton_measured_clients={layout_info['singleton_count']} "
              f"packed_virtual_clients={layout_info['packed_client_count']} "
              f"packed_containers={layout_info['packed_container_count']} "
              f"physical_worker_containers={layout_info['physical_worker_count']} "
              f"packed_clients_per_container={args.packed_clients_per_container} "
              f"profile_policy=singletons_only")
    else:
        clients, physical_workers = build_legacy_layout(args)
        layout_info = {
            "singleton_count": args.workers,
            "packed_client_count": 0,
            "packed_container_count": 0,
            "physical_worker_count": args.workers,
        }

    layout_json = generate_worker_layout_json(args, clients, physical_workers)
    write_text(layout_out, json.dumps(layout_json, indent=2) + "\n")

    write_text(compose_out, generate_compose_text(args, physical_workers))
    write_text(workers_out, generate_workers_internal(args, clients))
    write_text(workers_host_out, generate_workers_host(args, physical_workers))

    print(f"Wrote {compose_out}")
    print(f"Wrote {workers_out}")
    print(f"Wrote {workers_host_out}")
    print(f"Wrote {layout_out}")
    print("")
    print("What you generated:")
    print(f"- Compose file with {len(physical_workers)} physical worker services "
          f"({args.workers} logical clients)")
    print(f"- Layout mode: {args.worker_layout_mode}")
    if args.worker_layout_mode == "hybrid":
        print(f"- Singleton measured clients: {layout_info['singleton_count']}")
        print(f"- Packed virtual clients: {layout_info['packed_client_count']}")
        print(f"- Packed containers: {layout_info['packed_container_count']}")
    print(f"- Workers distributed across {args.bridge_count} Docker bridge network(s)")
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
