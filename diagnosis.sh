#!/usr/bin/env bash

<<Build
docker build --no-cache --target ds-runtime -t mls-ds .
docker build --no-cache --target relay-runtime -t mls-relay .
docker build --no-cache --target worker-runtime -t mls-worker .
docker build --no-cache --target runner-runtime -t mls-runner .
Build

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

  echo
  echo "\$ journalctl -u docker --since '$since' -n 300 --no-pager"

  if have_sudo; then
    sudo journalctl -u docker --since "$since" -n 300 --no-pager || true
  else
    journalctl -u docker --since "$since" -n 300 --no-pager || true
  fi
}

safe_dmesg() {
  echo
  echo "\$ dmesg -T | tail -250"

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

diagnose_netcheck() {
  local workers="$1"
  local run_id="$2"
  local compose_file="$3"
  local run_dir="$4"
  local run_start="$5"
  local last_worker

  last_worker="$(printf 'worker-%05d' "$workers")"

  section "$run_id: NETCHECK GENERATED COMPOSE CONFIG"

  if [ -f "$compose_file" ]; then
    run_shell "grep -n '^  netcheck:' -A240 '$compose_file' || true"

    echo
    echo "[check] escaped shell variables in generated compose file"
    run_shell "grep -n '\\\$\\\$RUN_ID\\|\\\$\\\$OUT\\|\\\$\\\$WORKERS\\|\\\$\\\$url\\|\\\$\\\$label\\|\\\$\\\$rc\\|\\\$\\\$mode\\|\\\$\\\$id\\|\\\$\\\$host\\|\\\$\\\$dns' '$compose_file' | head -160 || true"

    echo
    echo "[check] possible unescaped Compose variables in generated compose file"
    run_shell "grep -nE '\\\$[A-Za-z_][A-Za-z0-9_]*' '$compose_file' | grep -v '\\\$\\\$' | head -160 || true"

    echo
    echo "[check] resolved compose config around netcheck"
    run_shell "docker compose -f '$compose_file' config 2>/dev/null | grep -n '^  netcheck:' -A240 | head -280 || true"
  else
    echo "compose file missing: $compose_file"
  fi

  section "$run_id: NETCHECK RUN DIRECTORY LOG"

  run_shell "ls -lh '$run_dir/netcheck.log' 2>/dev/null || echo 'netcheck.log missing'"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'netcheck.log line count:'; wc -l '$run_dir/netcheck.log'; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo '--- netcheck.log head ---'; sed -n '1,220p' '$run_dir/netcheck.log'; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo '--- netcheck.log tail ---'; tail -400 '$run_dir/netcheck.log'; } || true"

  section "$run_id: NETCHECK FAILURE SUMMARY"

  run_shell "test -f '$run_dir/netcheck.log' && { echo 'first explicit failures:'; grep -nE 'FAIL|WORKER_FAIL|MISSING|curl_exit|missing_workers_file' '$run_dir/netcheck.log' | head -120; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'last explicit failures:'; grep -nE 'FAIL|WORKER_FAIL|MISSING|curl_exit|missing_workers_file' '$run_dir/netcheck.log' | tail -300; } || echo 'no explicit netcheck failures found or netcheck.log missing'"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'curl exit code counts:'; grep -oE 'curl_exit=[0-9]+' '$run_dir/netcheck.log' | sort | uniq -c | sort -nr; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'worker failure counts:'; grep 'WORKER_FAIL' '$run_dir/netcheck.log' | sed -n 's/.*id=\\([^ ]*\\).*/\\1/p' | sort | uniq -c | sort -nr | head -120; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'host failure counts:'; grep 'WORKER_FAIL' '$run_dir/netcheck.log' | sed -n 's/.*host=\\([^ ]*\\).*/\\1/p' | sort | uniq -c | sort -nr | head -120; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'DNS result samples for failures:'; grep 'WORKER_FAIL' '$run_dir/netcheck.log' | sed -n 's/.*dns=\\(.*\\)$/\\1/p' | sort | uniq -c | sort -nr | head -120; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'worker scan summaries:'; grep 'WORKER_SCAN' '$run_dir/netcheck.log' | tail -120; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'heartbeats:'; grep 'HEARTBEAT' '$run_dir/netcheck.log' | tail -80; } || true"
  run_shell "test -f '$run_dir/netcheck.log' && { echo 'DS/relay checks:'; grep -E 'OK ds|FAIL ds|OK relay|FAIL relay' '$run_dir/netcheck.log' | tail -120; } || true"

  section "$run_id: NETCHECK CONTAINER STATE"

  if [ -f "$compose_file" ]; then
    run_shell "docker compose -f '$compose_file' ps netcheck || true"
    run_shell "docker compose -f '$compose_file' logs --no-color netcheck 2>/dev/null | tail -300 || true"

    echo
    echo "\$ inspect netcheck container"
    local cid
    cid="$(docker compose -f "$compose_file" ps -q netcheck 2>/dev/null || true)"

    if [ -z "$cid" ]; then
      echo "netcheck container not found"
    else
      echo "netcheck container id: $cid"

      echo
      echo "--- docker inspect state ---"
      docker inspect "$cid" --format '{{json .State}}' 2>/dev/null | python3 -m json.tool || docker inspect "$cid" --format '{{json .State}}' || true

      echo
      echo "--- docker inspect pid/status/oom ---"
      docker inspect "$cid" --format 'name={{.Name}} pid={{.State.Pid}} status={{.State.Status}} running={{.State.Running}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}' || true

      echo
      echo "--- mounted volumes ---"
      docker inspect "$cid" --format '{{json .Mounts}}' 2>/dev/null | python3 -m json.tool || true

      echo
      echo "--- attached networks ---"
      docker inspect "$cid" --format '{{json .NetworkSettings.Networks}}' 2>/dev/null | python3 -m json.tool || true
    fi
  fi

  section "$run_id: LIVE NETCHECK CONTAINER NETWORK VIEW"

  if [ -f "$compose_file" ]; then
    local cid
    cid="$(docker compose -f "$compose_file" ps -q netcheck 2>/dev/null || true)"

    if [ -z "$cid" ]; then
      echo "netcheck container not running/found"
    else
      echo "\$ docker exec $cid sh -lc '<live netcheck probe>'"

      docker exec "$cid" sh -lc "
        echo \"date=\$(date -Is)\"
        echo \"hostname=\$(hostname)\"

        echo
        echo \"--- /etc/resolv.conf ---\"
        cat /etc/resolv.conf || true

        echo
        echo \"--- ip addr ---\"
        ip addr || true

        echo
        echo \"--- ip route ---\"
        ip route || true

        echo
        echo \"--- ss summary ---\"
        ss -s || true

        echo
        echo \"--- listening sockets ---\"
        ss -tulpen || true

        echo
        echo \"--- processes ---\"
        ps aux || true

        echo
        echo \"--- workers file inside netcheck ---\"
        ls -lh '/results/$run_id/workers.txt' || true
        wc -l '/results/$run_id/workers.txt' || true
        head -10 '/results/$run_id/workers.txt' || true
        tail -10 '/results/$run_id/workers.txt' || true

        echo
        echo \"--- DNS checks: ds relay selected workers ---\"
        getent hosts ds || true
        getent hosts relay || true
        getent hosts worker-00001 || true
        getent hosts worker-00002 || true
        getent hosts worker-00010 || true
        getent hosts worker-00100 || true
        getent hosts worker-00500 || true
        getent hosts worker-00750 || true
        getent hosts worker-01000 || true
        getent hosts worker-01500 || true
        getent hosts worker-02000 || true
        getent hosts '$last_worker' || true

        echo
        echo \"--- HTTP checks from netcheck ---\"
        curl -v --connect-timeout 2 --max-time 5 http://ds:3000/health || true
        curl -v --connect-timeout 2 --max-time 5 http://relay:4400/health || true
        curl -v --connect-timeout 2 --max-time 5 http://worker-00001:8080/health || true
        curl -v --connect-timeout 2 --max-time 5 http://worker-00010:8080/health || true
        curl -v --connect-timeout 2 --max-time 5 http://'$last_worker':8080/health || true
      "

      local rc=$?
      echo "[exit=$rc]"
    fi
  fi

  section "$run_id: DOCKER NETWORK MEMBERSHIP DETAIL"

  echo
  echo "\$ docker network inspect for networks matching mls-$run_id"

  local nets=()
  mapfile -t nets < <(docker network ls --format '{{.Name}}' | grep "mls-$run_id" || true)

  if [ "${#nets[@]}" -eq 0 ]; then
    echo "no Docker networks found for run"
  fi

  local net
  for net in "${nets[@]}"; do
    echo
    echo "$SECTION_LINE"
    echo "NETWORK $net"
    echo "$SECTION_LINE"

    docker network inspect "$net" --format 'driver={{.Driver}} id={{.Id}} containers={{len .Containers}} subnet={{range .IPAM.Config}}{{.Subnet}}{{end}} gateway={{range .IPAM.Config}}{{.Gateway}}{{end}}' || true

    echo
    echo "--- first attached containers ---"
    docker network inspect "$net" --format '{{range $id, $c := .Containers}}{{printf "%s %s %s\n" $c.Name $c.IPv4Address $c.MacAddress}}{{end}}' 2>/dev/null | sort | head -60 || true

    echo
    echo "--- last attached containers ---"
    docker network inspect "$net" --format '{{range $id, $c := .Containers}}{{printf "%s %s %s\n" $c.Name $c.IPv4Address $c.MacAddress}}{{end}}' 2>/dev/null | sort | tail -60 || true
  done

  section "$run_id: DOCKER EVENTS SINCE RUN START"

  run_shell "timeout 25 docker events --since '$run_start' --until \"$(date -Is)\" 2>/dev/null | grep -E 'mls-$run_id|network|oom|kill|die|destroy|disconnect|connect|health_status|create|start' | tail -700 || true"

  section "$run_id: CONNTRACK / IPTABLES / NFT SNAPSHOT"

  run_shell "command -v conntrack >/dev/null && conntrack -S 2>/dev/null || echo 'conntrack command unavailable or needs sudo'"
  run_shell "cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || true"
  run_shell "cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null || true"

  if have_sudo; then
    run_shell "sudo conntrack -S 2>/dev/null || true"
    run_shell "sudo nft list ruleset 2>/dev/null | grep -Ei 'docker|bridge|masquerade|forward|prerouting|postrouting|conntrack' | head -400 || true"
    run_shell "sudo iptables -S 2>/dev/null | grep -Ei 'docker|bridge|forward|masquerade|conntrack' | head -400 || true"
    run_shell "sudo iptables -t nat -S 2>/dev/null | grep -Ei 'docker|bridge|forward|masquerade|conntrack' | head -400 || true"
  else
    run_shell "nft list ruleset 2>/dev/null | grep -Ei 'docker|bridge|masquerade|forward|prerouting|postrouting|conntrack' | head -400 || true"
    run_shell "iptables -S 2>/dev/null | grep -Ei 'docker|bridge|forward|masquerade|conntrack' | head -400 || true"
    run_shell "iptables -t nat -S 2>/dev/null | grep -Ei 'docker|bridge|forward|masquerade|conntrack' | head -400 || true"
  fi
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
  run_shell "lscpu | sed -n '1,100p' || true"
  run_shell "free -h"
  run_shell "df -h"
  run_shell "ulimit -a"
  run_shell "cat /proc/loadavg || true"
  run_shell "cat /proc/meminfo | sed -n '1,80p' || true"

  section "IMPORTANT SYSCTL VALUES"
  run_shell "sysctl kernel.pid_max kernel.threads-max fs.file-max fs.nr_open vm.max_map_count net.core.somaxconn net.core.netdev_max_backlog net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse net.netfilter.nf_conntrack_max net.netfilter.nf_conntrack_count 2>/dev/null || true"

  section "NETWORK LIMITS / CONNTRACK PREFLIGHT"
  run_shell "sysctl net.ipv4.ip_forward net.bridge.bridge-nf-call-iptables net.bridge.bridge-nf-call-ip6tables 2>/dev/null || true"
  run_shell "cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null || true"
  run_shell "cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null || true"
  run_shell "command -v conntrack >/dev/null && conntrack -S 2>/dev/null || true"
  run_shell "lsmod | grep -E 'br_netfilter|nf_conntrack|overlay|veth' || true"

  section "DOCKER VERSION / INFO"
  run_shell "docker version || true"
  run_shell "docker compose version || true"
  run_shell "docker info || true"

  section "DOCKER IMAGES"
  run_shell "docker image ls | grep -E 'mls-|nicolaka/netshoot|rust|REPOSITORY' || true"

  section "DOCKER CURRENT STATE BEFORE TESTS"
  run_shell "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | head -160 || true"
  run_shell "docker network ls || true"
  run_shell "docker system df || true"
  run_shell "docker events --since 10m --until \"$(date -Is)\" 2>/dev/null | tail -250 || true"

  section "REPO SCRIPTS SNAPSHOT"
  run_shell "ls -lh scripts/run_compose_benchmark.py scripts/generate_compose.py 2>/dev/null || true"
  run_shell "grep -n 'include-netcheck\\|netcheck\\|monitor_script.replace' scripts/run_compose_benchmark.py scripts/generate_compose.py 2>/dev/null || true"
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
    run_shell "ls -lh /tmp/resolved-$run_id.yml 2>/dev/null || true"
    run_shell "docker compose -f '$compose_file' config --services 2>/dev/null | wc -l || true"
    run_shell "docker compose -f '$compose_file' config --services 2>/dev/null | sed -n '1,60p' || true"
    run_shell "docker compose -f '$compose_file' config --services 2>/dev/null | tail -60 || true"
  else
    echo "compose file missing"
  fi

  section "$run_id: GENERATED COMPOSE NETWORK SUMMARY"
  if [ -f "$compose_file" ]; then
    run_shell "grep -n 'bench-net' '$compose_file' | head -120 || true"
    run_shell "grep -n '^networks:' -A120 '$compose_file' || true"
    run_shell "grep -n '^  runner:' -A60 '$compose_file' || true"
    run_shell "grep -n '^  netcheck:' -A80 '$compose_file' || true"
    run_shell "grep -n '^  ds:' -A40 '$compose_file' || true"
    run_shell "grep -n '^  relay:' -A40 '$compose_file' || true"
  fi

  section "$run_id: WORKERS FILES"
  run_shell "wc -l 'workers.$run_id.txt' 'workers.$run_id.host.txt' 2>/dev/null || true"
  run_shell "head -20 'workers.$run_id.txt' 2>/dev/null || true"
  run_shell "tail -20 'workers.$run_id.txt' 2>/dev/null || true"

  section "$run_id: RUN DIRECTORY ARTIFACTS"
  run_shell "find '$run_dir' -maxdepth 1 -type f -printf '%f\t%k KB\n' 2>/dev/null | sort || true"
  run_shell "ls -lh '$run_dir/events.csv' 2>/dev/null || echo 'events.csv missing'"
  run_shell "test -f '$run_dir/events.csv' && { echo 'events.csv line count:'; wc -l '$run_dir/events.csv'; echo 'events.csv head:'; head -5 '$run_dir/events.csv'; echo 'events.csv tail:'; tail -5 '$run_dir/events.csv'; } || true"
  run_shell "echo 'jsonl count:'; find '$run_dir' -maxdepth 1 -name 'client-*.jsonl' 2>/dev/null | wc -l"
  run_shell "echo 'non-empty jsonl count:'; find '$run_dir' -maxdepth 1 -name 'client-*.jsonl' -size +0c 2>/dev/null | wc -l"
  run_shell "find '$run_dir' -maxdepth 1 -name 'client-*.jsonl' -size +0c 2>/dev/null | head -20 | xargs -r ls -lh"

  diagnose_netcheck "$workers" "$run_id" "$compose_file" "$run_dir" "$run_start"

  section "$run_id: TERMINAL OUTPUT"
  run_shell "test -f '$run_dir/terminal_output.txt' && sed -n '1,220p' '$run_dir/terminal_output.txt' || echo 'terminal_output.txt missing'"
  run_shell "test -f '$run_dir/terminal_output.txt' && { echo '--- tail terminal_output.txt ---'; tail -350 '$run_dir/terminal_output.txt'; } || true"
  run_shell "test -f '$run_dir/terminal_output.txt' && { echo '--- important terminal errors ---'; grep -nEi 'error|fail|panic|timeout|refused|unreachable|dns|health|worker|connection|oom|killed' '$run_dir/terminal_output.txt' | tail -300; } || true"

  section "$run_id: COMPOSE SERVICE LOG FILE"
  run_shell "test -f '$run_dir/compose_services.log' && sed -n '1,220p' '$run_dir/compose_services.log' || echo 'compose_services.log missing'"
  run_shell "test -f '$run_dir/compose_services.log' && { echo '--- tail compose_services.log ---'; tail -400 '$run_dir/compose_services.log'; } || true"
  run_shell "test -f '$run_dir/compose_services.log' && { echo '--- important compose log errors ---'; grep -nEi 'error|fail|panic|timeout|refused|unreachable|dns|health|connection|oom|killed|exited|die' '$run_dir/compose_services.log' | tail -300; } || true"

  section "$run_id: DOCKER COMPOSE PS"
  if [ -f "$compose_file" ]; then
    run_shell "docker compose -f '$compose_file' ps || true"
    run_shell "docker compose -f '$compose_file' ps -a || true"
  fi

  section "$run_id: DOCKER CONTAINERS MATCHING RUN"
  run_shell "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}\t{{.Ports}}' | grep 'mls-$run_id' | head -240 || true"
  run_shell "echo 'container count:'; docker ps -a --format '{{.Names}}' | grep 'mls-$run_id' | wc -l || true"
  run_shell "echo 'running count:'; docker ps --format '{{.Names}}' | grep 'mls-$run_id' | wc -l || true"
  run_shell "echo 'exited count:'; docker ps -a --format '{{.Names}} {{.Status}}' | grep 'mls-$run_id' | grep -i exited | wc -l || true"

  section "$run_id: DOCKER NETWORKS FOR RUN"
  run_shell "docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}' | grep 'mls-$run_id' || true"
  run_shell "for net in \$(docker network ls --format '{{.Name}}' | grep 'mls-$run_id' || true); do echo; echo \"=== \$net ===\"; docker network inspect \"\$net\" --format 'driver={{.Driver}} containers={{len .Containers}} subnet={{range .IPAM.Config}}{{.Subnet}}{{end}} gateway={{range .IPAM.Config}}{{.Gateway}}{{end}}' || true; done"

  section "$run_id: SELECTED COMPOSE LOGS"
  if [ -f "$compose_file" ]; then
    run_shell "docker compose -f '$compose_file' logs --no-color ds 2>/dev/null | tail -180 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color relay 2>/dev/null | tail -180 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color runner 2>/dev/null | tail -180 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color netcheck 2>/dev/null | tail -180 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color worker-00001 2>/dev/null | tail -120 || true"
    run_shell "docker compose -f '$compose_file' logs --no-color worker-$(printf '%05d' "$workers") 2>/dev/null | tail -120 || true"
  fi

  section "$run_id: HOST HEALTH CHECKS"
  run_shell "curl -v --max-time 5 http://127.0.0.1:3000/health || true"
  run_shell "curl -v --max-time 5 http://127.0.0.1:4400/health || true"

  section "$run_id: DOCKER STATS SAMPLE"
  run_shell "ids=\$(docker ps -q --filter 'name=mls-$run_id' | head -80 | tr '\n' ' '); if [ -n \"\$ids\" ]; then timeout 40 docker stats --no-stream \$ids || true; else echo 'no running containers for stats'; fi"

  section "$run_id: PROCESS / RESOURCE STATE AFTER RUN"
  run_shell "uptime"
  run_shell "free -h"
  run_shell "df -h"
  run_shell "ps -eo pid,ppid,stat,comm,%cpu,%mem --sort=-%cpu | head -60"
  run_shell "echo 'process count:'; ps -e --no-headers | wc -l"
  run_shell "echo 'thread count estimate:'; ps -eLf --no-headers | wc -l"
  run_shell "echo 'docker/containerd processes:'; ps -eo pid,ppid,stat,comm,args | grep -E 'dockerd|containerd|runc|docker compose|containerd-shim' | grep -v grep | head -160 || true"

  section "$run_id: NETWORK / SOCKET STATE"
  run_shell "ss -s || true"
  run_shell "echo 'established tcp count:'; ss -tan state established | wc -l || true"
  run_shell "echo 'time-wait tcp count:'; ss -tan state time-wait | wc -l || true"
  run_shell "echo 'listening tcp count:'; ss -tln | wc -l || true"
  run_shell "ip addr show | sed -n '1,240p' || true"
  run_shell "ip route show || true"
  run_shell "bridge link show 2>/dev/null | head -300 || true"

  section "$run_id: SYSCTL SNAPSHOT AFTER RUN"
  run_shell "sysctl kernel.pid_max kernel.threads-max fs.file-max fs.nr_open vm.max_map_count net.core.somaxconn net.core.netdev_max_backlog net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse net.netfilter.nf_conntrack_max net.netfilter.nf_conntrack_count 2>/dev/null || true"

  section "$run_id: DOCKER DAEMON LOGS SINCE RUN START"
  safe_journalctl_docker "$run_start"

  section "$run_id: DMESG TAIL"
  safe_dmesg

  section "$run_id: OOM / KILL / DOCKER ERROR HINTS"
  if have_sudo; then
    sudo journalctl --since "$run_start" --no-pager 2>/dev/null | grep -Ei 'oom|killed process|out of memory|dockerd|containerd|runc|veth|bridge|iptables|nft|conntrack|segfault|panic|failed|timeout|unreachable|dns' | tail -300 || true
  else
    journalctl --since "$run_start" --no-pager 2>/dev/null | grep -Ei 'oom|killed process|out of memory|dockerd|containerd|runc|veth|bridge|iptables|nft|conntrack|segfault|panic|failed|timeout|unreachable|dns' | tail -300 || true
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
echo "512 workers, 8 bridges"
echo "750 workers, 10 bridges"
echo "1000 workers, 10 bridges"
echo "1500 workers, 10 bridges"
echo "2000 workers, 10 bridges"
echo
echo "All output is being saved to: $REPORT"

run_benchmark_case 75 1 25 25
run_benchmark_case 256 4 50 50
run_benchmark_case 512 8 75 75
run_benchmark_case 750 10 100 100
run_benchmark_case 1000 10 100 100
run_benchmark_case 1500 10 100 100
run_benchmark_case 2000 10 100 100

section "FINAL DOCKER STATE"
run_shell "docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' | grep 'mls-diag' || true"
run_shell "docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}' | grep 'mls-diag' || true"
run_shell "docker system df || true"

section "DONE"
echo "Diagnosis report written to: $REPORT"