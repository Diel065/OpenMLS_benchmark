#!/usr/bin/env bash
set -uo pipefail

STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT="diagnosis_${STAMP}.txt"

exec > >(tee "$REPORT") 2>&1

PY=".venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="python3"
fi

SECTION_LINE="======================================================================"

section() {
  echo
  echo "$SECTION_LINE"
  echo "$1"
  echo "$SECTION_LINE"
}

run_cmd() {
  echo
  echo "\$ $*"
  "$@"
  local rc=$?
  echo "[exit=$rc]"
  return "$rc"
}

run_shell() {
  echo
  echo "\$ $*"
  bash -lc "$*"
  local rc=$?
  echo "[exit=$rc]"
  return "$rc"
}

have_sudo() {
  sudo -n true >/dev/null 2>&1
}

safe_journalctl_docker() {
  local since="$1"

  if have_sudo; then
    sudo journalctl -u docker --since "$since" -n 300 --no-pager || true
  else
    journalctl -u docker --since "$since" -n 300 --no-pager || true
  fi
}

safe_dmesg() {
  if have_sudo; then
    sudo dmesg -T | tail -250 || true
  else
    dmesg -T | tail -250 || true
  fi
}

worker_service_names_reverse() {
  local workers="$1"
  local i
  for ((i=workers; i>=1; i--)); do
    printf 'worker-%05d\n' "$i"
  done
}

cleanup_stack_batched() {
  local compose_file="$1"
  local workers="$2"
  local batch_size="$3"

  if [ ! -f "$compose_file" ]; then
    echo "[cleanup] no compose file found: $compose_file"
    return 0
  fi

  if [ "$batch_size" -le 0 ]; then
    batch_size=100
  fi

  section "CLEANUP STACK: $compose_file"

  echo "[cleanup] stopping/removing runner if present"
  docker compose -f "$compose_file" stop -t 1 runner || true
  docker compose -f "$compose_file" rm -f runner || true

  mapfile -t services < <(worker_service_names_reverse "$workers")

  local batch=()
  local count=0
  local total=0
  local svc

  for svc in "${services[@]}"; do
    batch+=("$svc")
    count=$((count + 1))
    total=$((total + 1))

    if [ "$count" -ge "$batch_size" ]; then
      echo "[cleanup] stopping/removing worker batch ending at $svc ($total/$workers)"
      docker compose -f "$compose_file" stop -t 1 "${batch[@]}" || true
      docker compose -f "$compose_file" rm -f "${batch[@]}" || true
      batch=()
      count=0
    fi
  done

  if [ "${#batch[@]}" -gt 0 ]; then
    echo "[cleanup] stopping/removing final worker batch ($total/$workers)"
    docker compose -f "$compose_file" stop -t 1 "${batch[@]}" || true
    docker compose -f "$compose_file" rm -f "${batch[@]}" || true
  fi

  echo "[cleanup] final docker compose down"
  docker compose -f "$compose_file" down --timeout 1 || true
}

global_preflight() {
  section "GLOBAL PREFLIGHT"

  echo "diagnosis report: $REPORT"
  echo "date: $(date -Is)"
  echo "pwd: $(pwd)"
  echo "python: $PY"

  section "GIT STATE"
  run_shell "git rev-parse --show-toplevel 2>/dev/null || true"
  run_shell "git branch --show-current 2>/dev/null || true"
  run_shell "git log -1 --oneline 2>/dev/null || true"
  run_shell "git status --short 2>/dev/null || true"

  section "SYSTEM INFO"
  run_shell "uname -a"
  run_shell "cat /etc/os-release 2>/dev/null || true"
  run_shell "uptime"
  run_shell "nproc"
  run_shell "lscpu | sed -n '1,80p' || true"
  run_shell "free -h"
  run_shell "df -h"
  run_shell "ulimit -a"

  section "IMPORTANT SYSCTL VALUES"
  run_shell "sysctl kernel.pid_max kernel.threads-max fs.file-max fs.nr_open vm.max_map_count net.core.somaxconn net.core.netdev_max_backlog net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse net.netfilter.nf_conntrack_max net.netfilter.nf_conntrack_count 2>/dev/null || true"

  section "DOCKER VERSION / INFO"
  run_shell "docker version || true"
  run_shell "docker compose version || true"
  run_shell "docker info || true"

  section "DOCKER CURRENT STATE BEFORE TESTS"
  run_shell "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | head -120 || true"
  run_shell "docker network ls || true"
  run_shell "docker system df || true"
}

diagnose_run() {
  local workers="$1"
  local bridges="$2"
  local run_id="$3"
  local compose_file="$4"
  local run_dir="$5"
  local run_start="$6"
  local bench_rc="$7"

  section "DIAGNOSIS FOR $run_id"

  echo "workers=$workers"
  echo "bridges=$bridges"
  echo "benchmark_exit_code=$bench_rc"
  echo "compose_file=$compose_file"
  echo "run_dir=$run_dir"
  echo "run_start=$run_start"
  echo "now=$(date -Is)"

  section "$run_id: GENERATED FILES"
  run_shell "ls -lh '$compose_file' 'workers.$run_id.txt' 'workers.$run_id.host.txt' '$run_dir' 2>/dev/null || true"

  section "$run_id: COMPOSE CONFIG VALIDATION"
  if [ -f "$compose_file" ]; then
    run_shell "docker compose -f '$compose_file' config >/tmp/resolved-$run_id.yml && echo 'compose config ok' || echo 'compose config failed'"
    run_shell "docker compose -f '$compose_file' config --services 2>/dev/null | wc -l || true"
    run_shell "docker compose -f '$compose_file' config --services 2>/dev/null | sed -n '1,40p' || true"
    run_shell "docker compose -f '$compose_file' config --services 2>/dev/null | tail -40 || true"
  else
    echo "compose file missing"
  fi

  section "$run_id: GENERATED COMPOSE NETWORK SUMMARY"
  if [ -f "$compose_file" ]; then
    run_shell "grep -n 'bench-net' '$compose_file' | head -80 || true"
    run_shell "grep -n '^networks:' -A80 '$compose_file' || true"
    run_shell "grep -n '^  runner:' -A40 '$compose_file' || true"
    run_shell "grep -n '^  ds:' -A30 '$compose_file' || true"
    run_shell "grep -n '^  relay:' -A30 '$compose_file' || true"
  fi

  section "$run_id: WORKERS FILES"
  run_shell "wc -l 'workers.$run_id.txt' 'workers.$run_id.host.txt' 2>/dev/null || true"
  run_shell "head -10 'workers.$run_id.txt' 2>/dev/null || true"
  run_shell "tail -10 'workers.$run_id.txt' 2>/dev/null || true"

  section "$run_id: RUN DIRECTORY ARTIFACTS"
  run_shell "find '$run_dir' -maxdepth 1 -type f -printf '%f\t%k KB\n' 2>/dev/null | sort || true"
  run_shell "ls -lh '$run_dir/events.csv' 2>/dev/null || echo 'events.csv missing'"
  run_shell "test -f '$run_dir/events.csv' && { echo 'events.csv line count:'; wc -l '$run_dir/events.csv'; echo 'events.csv head:'; head -5 '$run_dir/events.csv'; echo 'events.csv tail:'; tail -5 '$run_dir/events.csv'; } || true"
  run_shell "echo 'jsonl count:'; find '$run_dir' -maxdepth 1 -name 'client-*.jsonl' 2>/dev/null | wc -l"
  run_shell "echo 'non-empty jsonl count:'; find '$run_dir' -maxdepth 1 -name 'client-*.jsonl' -size +0c 2>/dev/null | wc -l"
  run_shell "find '$run_dir' -maxdepth 1 -name 'client-*.jsonl' -size +0c 2>/dev/null | head -10 | xargs -r ls -lh"

  section "$run_id: TERMINAL OUTPUT"
  run_shell "test -f '$run_dir/terminal_output.txt' && sed -n '1,160p' '$run_dir/terminal_output.txt' || echo 'terminal_output.txt missing'"
  run_shell "test -f '$run_dir/terminal_output.txt' && { echo '--- tail terminal_output.txt ---'; tail -250 '$run_dir/terminal_output.txt'; } || true"

  section "$run_id: COMPOSE SERVICE LOG FILE"
  run_shell "test -f '$run_dir/compose_services.log' && sed -n '1,160p' '$run_dir/compose_services.log' || echo 'compose_services.log missing'"
  run_shell "test -f '$run_dir/compose_services.log' && { echo '--- tail compose_services.log ---'; tail -300 '$run_dir/compose_services.log'; } || true"

  section "$run_id: DOCKER COMPOSE PS"
  if [ -f "$compose_file" ]; then
    run_shell "docker compose -f '$compose_file' ps || true"
    run_shell "docker compose -f '$compose_file' ps -a || true"
  fi

  section "$run_id: DOCKER CONTAINERS MATCHING RUN"
  run_shell "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}' | grep 'mls-$run_id' | head -120 || true"
  run_shell "echo 'container count:'; docker ps -a --format '{{.Names}}' | grep 'mls-$run_id' | wc -l || true"
  run_shell "echo 'running count:'; docker ps --format '{{.Names}}' | grep 'mls-$run_id' | wc -l || true"

  section "$run_id: DOCKER NETWORKS FOR RUN"
  run_shell "docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}' | grep 'mls-$run_id' || true"
  run_shell "for net in \$(docker network ls --format '{{.Name}}' | grep 'mls-$run_id' || true); do echo; echo \"=== \$net ===\"; docker network inspect \"\$net\" --format 'driver={{.Driver}} containers={{len .Containers}} subnet={{range .IPAM.Config}}{{.Subnet}}{{end}} gateway={{range .IPAM.Config}}{{.Gateway}}{{end}}' || true; done"

  section "$run_id: SELECTED COMPOSE LOGS"
  if [ -f "$compose_file" ]; then
    run_shell "docker compose -f '$compose_file' logs --no-color ds 2>/dev/null | tail -120 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color relay 2>/dev/null | tail -120 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color runner 2>/dev/null | tail -120 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color worker-00001 2>/dev/null | tail -80 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color worker-$(printf '%05d' "$workers") 2>/dev/null | tail -80 || true"
  fi

  section "$run_id: HOST HEALTH CHECKS"
  run_shell "curl -v --max-time 5 http://127.0.0.1:3000/health || true"
  run_shell "curl -v --max-time 5 http://127.0.0.1:4400/health || true"

  section "$run_id: DOCKER STATS SAMPLE"
  run_shell "ids=\$(docker ps -q --filter 'name=mls-$run_id' | head -50 | tr '\n' ' '); if [ -n \"\$ids\" ]; then timeout 30 docker stats --no-stream \$ids || true; else echo 'no running containers for stats'; fi"

  section "$run_id: PROCESS / RESOURCE STATE AFTER RUN"
  run_shell "uptime"
  run_shell "free -h"
  run_shell "df -h"
  run_shell "ps -eo pid,ppid,stat,comm,%cpu,%mem --sort=-%cpu | head -40"
  run_shell "echo 'process count:'; ps -e --no-headers | wc -l"
  run_shell "echo 'docker/containerd processes:'; ps -eo pid,ppid,stat,comm,args | grep -E 'dockerd|containerd|runc|docker compose' | grep -v grep | head -80 || true"

  section "$run_id: NETWORK / SOCKET STATE"
  run_shell "ss -s || true"
  run_shell "ss -tan state established | wc -l || true"
  run_shell "ss -tan state time-wait | wc -l || true"
  run_shell "ip addr show | sed -n '1,180p' || true"
  run_shell "ip route show || true"

  section "$run_id: SYSCTL SNAPSHOT AFTER RUN"
  run_shell "sysctl kernel.pid_max kernel.threads-max fs.file-max fs.nr_open vm.max_map_count net.core.somaxconn net.core.netdev_max_backlog net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse net.netfilter.nf_conntrack_max net.netfilter.nf_conntrack_count 2>/dev/null || true"

  section "$run_id: DOCKER DAEMON LOGS SINCE RUN START"
  safe_journalctl_docker "$run_start"

  section "$run_id: DMESG TAIL"
  safe_dmesg

  section "$run_id: OOM / KILL / DOCKER ERROR HINTS"
  if have_sudo; then
    sudo journalctl --since "$run_start" --no-pager 2>/dev/null | grep -Ei 'oom|killed process|out of memory|dockerd|containerd|runc|veth|bridge|iptables|nft|conntrack|segfault|panic' | tail -200 || true
  else
    journalctl --since "$run_start" --no-pager 2>/dev/null | grep -Ei 'oom|killed process|out of memory|dockerd|containerd|runc|veth|bridge|iptables|nft|conntrack|segfault|panic' | tail -200 || true
  fi
}

run_benchmark_case() {
  local workers="$1"
  local bridges="$2"
  local startup_batch="$3"
  local teardown_batch="$4"

  local run_id="diag-${workers}w-${STAMP}"
  local compose_file="docker-compose.${run_id}.generated.yml"
  local run_dir="benchmark_output/${run_id}"
  local step_size=$((workers / 4))

  if [ "$step_size" -lt 1 ]; then
    step_size=1
  fi

  section "START BENCHMARK CASE: workers=$workers bridges=$bridges run_id=$run_id"

  local run_start
  run_start="$(date '+%Y-%m-%d %H:%M:%S')"

  local cmd=(
    "$PY" scripts/run_compose_benchmark.py
    --workers "$workers"
    --bridge-count "$bridges"
    --runner-in-docker
    --startup-batch-size "$startup_batch"
    --startup-batch-sleep-seconds 0
    --compose-parallel-limit 16
    --teardown-batch-size "$teardown_batch"
    --teardown-batch-sleep-seconds 0
    --compose-down-timeout-seconds 1
    --keep-stack-up
    --keep-generated-files
    --force-cleanup-mls-ports
    --ds-port 3000
    --relay-port 4400
    --min-size 2
    --max-size "$workers"
    --step-size "$step_size"
    --roundtrips 1
    --update-rounds 1
    --max-update-samples-per-plateau 1
    --app-rounds 1
    --max-app-samples-per-payload 1
    --payload-sizes 32
    --run-id "$run_id"
  )

  echo "command:"
  printf '%q ' "${cmd[@]}"
  echo

  "${cmd[@]}"
  local bench_rc=$?

  echo
  echo "[benchmark case finished] run_id=$run_id exit_code=$bench_rc"

  diagnose_run "$workers" "$bridges" "$run_id" "$compose_file" "$run_dir" "$run_start" "$bench_rc"

  cleanup_stack_batched "$compose_file" "$workers" "$teardown_batch"

  section "END BENCHMARK CASE: workers=$workers bridges=$bridges run_id=$run_id exit_code=$bench_rc"
}

global_preflight

section "BENCHMARK MATRIX"
echo "The following cases will run:"
echo "75 workers, 1 bridge"
echo "256 workers, 4 bridges"
echo "1000 workers, 10 bridges"
echo "2000 workers, 10 bridges"
echo
echo "All output is being saved to: $REPORT"

run_benchmark_case 75 1 25 25
run_benchmark_case 256 4 50 50
run_benchmark_case 1000 10 100 100
run_benchmark_case 2000 10 100 100

section "FINAL DOCKER STATE"
run_shell "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep 'mls-diag' || true"
run_shell "docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}' | grep 'mls-diag' || true"
run_shell "docker system df || true"

section "DONE"
echo "Diagnosis report written to: $REPORT"