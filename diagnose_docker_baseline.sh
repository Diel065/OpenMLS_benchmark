#!/usr/bin/env bash

# diagnose_docker_baseline.sh
#
# Drop this file into the OpenMLS_benchmark repo root and run:
#
#   chmod +x diagnose_docker_baseline.sh
#   ./diagnose_docker_baseline.sh
#
# Defaults:
#   Relay host/container port: 5000
#   DS host/container port:    3000
#   Worker host ports:         18081, 18082, 18083
#
# Optional overrides:
#   RELAY_PORT=5000 ./diagnose_docker_baseline.sh
#   DS_PORT=13000 RELAY_PORT=5000 BASE_WORKER_PORT=18081 ./diagnose_docker_baseline.sh
#   FORCE_CLEANUP_MLS_PORTS=1 ./diagnose_docker_baseline.sh
#
# FORCE_CLEANUP_MLS_PORTS=1 passes --force-cleanup-mls-ports to the benchmark runner.
# Default is 0 because that removes old Docker containers whose names begin with mls-.

set -uo pipefail

RUN_STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_ID="docker-diag-${RUN_STAMP}"

WORKERS="${WORKERS:-3}"
MIN_SIZE="${MIN_SIZE:-2}"
MAX_SIZE="${MAX_SIZE:-3}"
ROUNDTRIPS="${ROUNDTRIPS:-1}"
UPDATE_ROUNDS="${UPDATE_ROUNDS:-1}"
APP_ROUNDS="${APP_ROUNDS:-1}"

DS_PORT="${DS_PORT:-3000}"
RELAY_PORT="${RELAY_PORT:-5000}"
BASE_WORKER_PORT="${BASE_WORKER_PORT:-18081}"

FORCE_CLEANUP_MLS_PORTS="${FORCE_CLEANUP_MLS_PORTS:-0}"
SAVE_IMAGE_TARS="${SAVE_IMAGE_TARS:-0}"

OUT_BASE="benchmark_output/docker_diagnosis/${RUN_ID}"
REPORT="${OUT_BASE}/diagnosis_report.txt"
BIN_DIR="${OUT_BASE}/binaries_baseline"
TAR_DIR="${OUT_BASE}/image_tars"

mkdir -p "$OUT_BASE" "$BIN_DIR" "$TAR_DIR"

exec > >(tee -a "$REPORT") 2>&1

section() {
  echo
  echo "================================================================================"
  echo "$1"
  echo "================================================================================"
}

run() {
  echo
  echo "+ $*"
  "$@"
  local rc=$?
  echo "[exit code: $rc]"
  return "$rc"
}

run_allow_fail() {
  run "$@" || true
}

print_file_if_exists() {
  local file="$1"
  if [ -f "$file" ]; then
    echo
    echo "----- BEGIN FILE: $file -----"
    sed -n '1,260p' "$file" || true
    echo "----- END FILE: $file -----"
  else
    echo
    echo "MISSING FILE: $file"
  fi
}

port_check() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | grep -E ":${port}\b" || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${port}" -sTCP:LISTEN || true
  else
    echo "Neither ss nor lsof is available for port checking."
  fi
}

copy_binary_from_image() {
  local image="$1"
  local src="$2"
  local name="$3"

  echo
  echo "Copying $src from $image to $BIN_DIR/$name"

  local cid
  cid="$(docker create "$image" 2>/dev/null)" || {
    echo "Could not create container from $image"
    return 0
  }

  docker cp "${cid}:${src}" "${BIN_DIR}/${name}" || true
  docker rm "$cid" >/dev/null 2>&1 || true
}

section "DIAGNOSIS START"
echo "Diagnosis run id:         $RUN_ID"
echo "Report file:              $REPORT"
echo "Working directory:        $(pwd)"
echo "Date:                     $(date)"
echo "Relay port requested:     $RELAY_PORT"
echo "DS port requested:        $DS_PORT"
echo "Base worker port:         $BASE_WORKER_PORT"
echo "Worker count:             $WORKERS"
echo "Small benchmark sizes:    min=$MIN_SIZE max=$MAX_SIZE"
echo "Rounds:                   roundtrips=$ROUNDTRIPS update_rounds=$UPDATE_ROUNDS app_rounds=$APP_ROUNDS"
echo "FORCE_CLEANUP_MLS_PORTS:  $FORCE_CLEANUP_MLS_PORTS"
echo "SAVE_IMAGE_TARS:          $SAVE_IMAGE_TARS"

section "REPO ROOT CHECK"
if [ ! -f "Cargo.toml" ] || [ ! -f "Dockerfile" ] || [ ! -d "scripts" ]; then
  echo "ERROR: This does not look like the repo root."
  echo "Expected Cargo.toml, Dockerfile, and scripts/ in the current directory."
  echo "Current directory contents:"
  ls -lah
  exit 1
fi

run pwd
run ls -lah

section "BASIC SYSTEM AND TOOL VERSIONS"
run date
run hostname
run uname -a
run_allow_fail git status --short
run_allow_fail git branch --show-current
run_allow_fail git rev-parse HEAD
run_allow_fail git log -1 --oneline
run_allow_fail docker --version
run_allow_fail docker compose version
run_allow_fail cargo --version
run_allow_fail rustc --version
run_allow_fail rustup show
run_allow_fail python3 --version

section "DOCKER DISK USAGE BEFORE BUILD"
run_allow_fail docker system df
run_allow_fail docker builder du

section "SOURCE FILE SNAPSHOT"
print_file_if_exists "Dockerfile"
print_file_if_exists "Cargo.toml"
print_file_if_exists "docker-compose.yml"
print_file_if_exists "docker/worker-entrypoint.sh"
print_file_if_exists "scripts/generate_compose.py"
print_file_if_exists "scripts/run_compose_benchmark.py"

section "CURRENT BENCHMARK CONTAINERS, IMAGES, NETWORKS"
echo
echo "Existing Docker containers with name containing mls:"
run_allow_fail docker ps -a --filter "name=mls" --format "table {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

echo
echo "Existing Docker images with repository containing mls:"
run_allow_fail bash -lc "docker image ls --format 'table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.Size}}' | grep -E 'mls-|REPOSITORY' || true"

echo
echo "Existing Docker networks matching likely benchmark names:"
run_allow_fail bash -lc "docker network ls | grep -E 'mls|compose|openmls' || true"

section "HOST PORT CHECKS"
echo "Port 4000 check. This one is allowed to be busy; the benchmark will use relay port $RELAY_PORT instead."
port_check 4000

echo
echo "Required diagnosis ports:"
echo "DS_PORT=$DS_PORT"
echo "RELAY_PORT=$RELAY_PORT"
echo "Worker ports: ${BASE_WORKER_PORT} through $((BASE_WORKER_PORT + WORKERS - 1))"

echo
echo "DS port check:"
port_check "$DS_PORT"

echo
echo "Relay port check:"
port_check "$RELAY_PORT"

echo
echo "Worker port checks:"
p="$BASE_WORKER_PORT"
while [ "$p" -le "$((BASE_WORKER_PORT + WORKERS - 1))" ]; do
  echo
  echo "Worker host port $p:"
  port_check "$p"
  p="$((p + 1))"
done

section "SCRIPT HELP OUTPUT"
run_allow_fail python3 scripts/generate_compose.py --help
run_allow_fail python3 scripts/run_compose_benchmark.py --help

section "MANUAL VERBOSE BASELINE IMAGE BUILDS"
echo "These builds use the current Dockerfile targets and tag them as :${RUN_ID}."
echo "The benchmark later also uses --build-images, but these explicit builds give us plain logs and stable tags."

section "BUILD ds-runtime"
DOCKER_BUILDKIT=1 run_allow_fail docker build \
  --progress=plain \
  --target ds-runtime \
  -t "mls-ds:${RUN_ID}" \
  .

section "BUILD relay-runtime"
DOCKER_BUILDKIT=1 run_allow_fail docker build \
  --progress=plain \
  --target relay-runtime \
  -t "mls-relay:${RUN_ID}" \
  .

section "BUILD app-runtime"
DOCKER_BUILDKIT=1 run_allow_fail docker build \
  --progress=plain \
  --target app-runtime \
  -t "mls-app:${RUN_ID}" \
  .

section "TAG DIAGNOSIS IMAGES AS latest FOR CURRENT SCRIPTS"
run_allow_fail docker tag "mls-ds:${RUN_ID}" mls-ds:latest
run_allow_fail docker tag "mls-relay:${RUN_ID}" mls-relay:latest
run_allow_fail docker tag "mls-app:${RUN_ID}" mls-app:latest

section "IMAGE SIZE LIST AFTER MANUAL BUILDS"
run_allow_fail docker image ls "mls-ds"
run_allow_fail docker image ls "mls-relay"
run_allow_fail docker image ls "mls-app"
run_allow_fail docker system df

section "IMAGE INSPECT SUMMARY"
for img in "mls-ds:${RUN_ID}" "mls-relay:${RUN_ID}" "mls-app:${RUN_ID}" "mls-ds:latest" "mls-relay:latest" "mls-app:latest"; do
  echo
  echo "### $img"
  docker image inspect "$img" \
    --format 'Id={{.Id}}
RepoTags={{json .RepoTags}}
SizeBytes={{.Size}}
VirtualSizeBytes={{.VirtualSize}}
Architecture={{.Architecture}}
OS={{.Os}}
Entrypoint={{json .Config.Entrypoint}}
Cmd={{json .Config.Cmd}}
WorkingDir={{json .Config.WorkingDir}}
User={{json .Config.User}}
Env={{json .Config.Env}}
RootFSLayers={{len .RootFS.Layers}}' 2>/dev/null || true
done

section "IMAGE HISTORY"
for img in "mls-ds:${RUN_ID}" "mls-relay:${RUN_ID}" "mls-app:${RUN_ID}"; do
  echo
  echo "### docker history --no-trunc $img"
  docker history --no-trunc "$img" 2>/dev/null || true
done

section "RUNTIME FILESYSTEM AND LINKING INSPECTION"
echo "Current runtime images are expected to be Debian-based. Scratch images later will not have /bin/sh."

for img in "mls-ds:${RUN_ID}" "mls-relay:${RUN_ID}" "mls-app:${RUN_ID}"; do
  echo
  echo "### Runtime inspection for $img"
  docker run --rm --entrypoint /bin/sh "$img" -lc '
    echo "Image hostname: $HOSTNAME"
    echo
    echo "Root filesystem:"
    ls -lah / || true
    echo
    echo "/usr/local/bin:"
    ls -lah /usr/local/bin || true
    echo
    echo "du -h /usr/local/bin/*:"
    du -h /usr/local/bin/* 2>/dev/null || true
    echo
    echo "which ldd:"
    command -v ldd || true
    echo
    echo "ldd for binaries:"
    for f in /usr/local/bin/*; do
      if [ -f "$f" ]; then
        echo
        echo "--- $f"
        ldd "$f" 2>&1 || true
      fi
    done
  ' 2>&1 || true
done

section "COPY BINARIES OUT OF CURRENT IMAGES"
copy_binary_from_image "mls-ds:${RUN_ID}" "/usr/local/bin/ds" "ds"
copy_binary_from_image "mls-relay:${RUN_ID}" "/usr/local/bin/message_relay" "message_relay"
copy_binary_from_image "mls-app:${RUN_ID}" "/usr/local/bin/worker" "worker"
copy_binary_from_image "mls-app:${RUN_ID}" "/usr/local/bin/benchmark_runner_http_staircase" "benchmark_runner_http_staircase"
copy_binary_from_image "mls-app:${RUN_ID}" "/usr/local/bin/worker-entrypoint.sh" "worker-entrypoint.sh"

echo
echo "Copied binary directory:"
run_allow_fail ls -lah "$BIN_DIR"

echo
echo "Binary byte sizes:"
run_allow_fail wc -c "$BIN_DIR"/*

echo
echo "Host-side file output:"
run_allow_fail file "$BIN_DIR"/*

echo
echo "Host-side ldd output:"
run_allow_fail ldd "$BIN_DIR"/ds "$BIN_DIR"/message_relay "$BIN_DIR"/worker "$BIN_DIR"/benchmark_runner_http_staircase

echo
echo "Host-side size output:"
run_allow_fail size "$BIN_DIR"/ds "$BIN_DIR"/message_relay "$BIN_DIR"/worker "$BIN_DIR"/benchmark_runner_http_staircase

section "OPTIONAL DOCKER SAVE TAR SIZES"
if [ "$SAVE_IMAGE_TARS" = "1" ]; then
  echo "Saving image tars because SAVE_IMAGE_TARS=1."
  run_allow_fail docker save "mls-ds:${RUN_ID}" -o "${TAR_DIR}/mls-ds-${RUN_ID}.tar"
  run_allow_fail docker save "mls-relay:${RUN_ID}" -o "${TAR_DIR}/mls-relay-${RUN_ID}.tar"
  run_allow_fail docker save "mls-app:${RUN_ID}" -o "${TAR_DIR}/mls-app-${RUN_ID}.tar"
  run_allow_fail ls -lh "$TAR_DIR"
  run_allow_fail gzip -c "${TAR_DIR}/mls-ds-${RUN_ID}.tar" > "${TAR_DIR}/mls-ds-${RUN_ID}.tar.gz"
  run_allow_fail gzip -c "${TAR_DIR}/mls-relay-${RUN_ID}.tar" > "${TAR_DIR}/mls-relay-${RUN_ID}.tar.gz"
  run_allow_fail gzip -c "${TAR_DIR}/mls-app-${RUN_ID}.tar" > "${TAR_DIR}/mls-app-${RUN_ID}.tar.gz"
  run_allow_fail ls -lh "${TAR_DIR}"/*.gz
else
  echo "Skipping docker save tar creation. Set SAVE_IMAGE_TARS=1 to enable."
fi

section "BINARY STARTUP SANITY CHECKS"
echo "These may exit non-zero if the binaries require arguments. That is okay."

echo
echo "### ds --help"
run_allow_fail docker run --rm "mls-ds:${RUN_ID}" --help

echo
echo "### relay --help"
run_allow_fail docker run --rm "mls-relay:${RUN_ID}" --help

echo
echo "### app image through worker entrypoint, MODE=worker, --help"
run_allow_fail docker run --rm \
  -e MODE=worker \
  -e WORKER_NAME=00001 \
  -e DS_URL="http://ds:${DS_PORT}" \
  -e RELAY_URL="http://relay:${RELAY_PORT}" \
  -e LISTEN_ADDR="0.0.0.0:8080" \
  "mls-app:${RUN_ID}" \
  --help

echo
echo "### app image direct worker binary --help"
run_allow_fail docker run --rm --entrypoint /usr/local/bin/worker "mls-app:${RUN_ID}" --help

echo
echo "### app image direct runner binary --help"
run_allow_fail docker run --rm --entrypoint /usr/local/bin/benchmark_runner_http_staircase "mls-app:${RUN_ID}" --help

section "GENERATE COMPOSE FILE ONLY FOR INSPECTION"
GEN_COMPOSE="${OUT_BASE}/docker-compose.generated.preflight.yml"
GEN_WORKERS_INTERNAL="${OUT_BASE}/workers.preflight.txt"
GEN_WORKERS_HOST="${OUT_BASE}/workers.host.preflight.txt"

run_allow_fail python3 scripts/generate_compose.py \
  --workers "$WORKERS" \
  --run-id "$RUN_ID" \
  --scenario "docker-diagnosis-preflight" \
  --output-dir "benchmark_output" \
  --compose-out "$GEN_COMPOSE" \
  --workers-out "$GEN_WORKERS_INTERNAL" \
  --workers-host-out "$GEN_WORKERS_HOST" \
  --project-name "mls-${RUN_ID}" \
  --base-worker-port "$BASE_WORKER_PORT" \
  --ds-port "$DS_PORT" \
  --relay-port "$RELAY_PORT"

print_file_if_exists "$GEN_COMPOSE"
print_file_if_exists "$GEN_WORKERS_INTERNAL"
print_file_if_exists "$GEN_WORKERS_HOST"

echo
echo "Resolved compose config:"
run_allow_fail docker compose -f "$GEN_COMPOSE" config

section "TINY BASELINE BENCHMARK RUN"
echo "Running the benchmark with relay port $RELAY_PORT instead of 4000."
echo "This intentionally includes --build-images, like the normal command, so we capture the current runner behavior."

BENCH_CMD=(
  python3 scripts/run_compose_benchmark.py
  --workers "$WORKERS"
  --build-images
  --min-size "$MIN_SIZE"
  --max-size "$MAX_SIZE"
  --roundtrips "$ROUNDTRIPS"
  --update-rounds "$UPDATE_ROUNDS"
  --app-rounds "$APP_ROUNDS"
  --base-worker-port "$BASE_WORKER_PORT"
  --ds-port "$DS_PORT"
  --relay-port "$RELAY_PORT"
  --run-id "${RUN_ID}-bench"
)

if [ "$FORCE_CLEANUP_MLS_PORTS" = "1" ]; then
  BENCH_CMD+=(--force-cleanup-mls-ports)
fi

echo
echo "+ ${BENCH_CMD[*]}"
"${BENCH_CMD[@]}"
BENCH_RC=$?
echo "[benchmark exit code: $BENCH_RC]"

section "POST-BENCHMARK CONTAINER STATE"
run_allow_fail docker ps -a --filter "name=mls" --format "table {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

section "POST-BENCHMARK IMAGE STATE"
run_allow_fail docker image ls "mls-ds"
run_allow_fail docker image ls "mls-relay"
run_allow_fail docker image ls "mls-app"

section "POST-BENCHMARK PORT CHECKS"
echo "Port 4000:"
port_check 4000
echo
echo "DS port $DS_PORT:"
port_check "$DS_PORT"
echo
echo "Relay port $RELAY_PORT:"
port_check "$RELAY_PORT"
echo
echo "Worker ports:"
p="$BASE_WORKER_PORT"
while [ "$p" -le "$((BASE_WORKER_PORT + WORKERS - 1))" ]; do
  echo
  echo "Worker host port $p:"
  port_check "$p"
  p="$((p + 1))"
done

section "BENCHMARK OUTPUT FILE DISCOVERY"
echo "Looking for files from run id ${RUN_ID}-bench"
run_allow_fail find "benchmark_output" -maxdepth 4 -type f -path "*${RUN_ID}-bench*" -print

BENCH_DIR="benchmark_output/${RUN_ID}-bench"
echo
echo "Expected benchmark directory: $BENCH_DIR"
run_allow_fail ls -lah "$BENCH_DIR"

echo
echo "Files in expected benchmark directory:"
run_allow_fail find "$BENCH_DIR" -maxdepth 2 -type f -print

section "BENCHMARK TERMINAL OUTPUT"
if [ -f "${BENCH_DIR}/terminal_output.txt" ]; then
  print_file_if_exists "${BENCH_DIR}/terminal_output.txt"
else
  echo "No terminal_output.txt found at ${BENCH_DIR}/terminal_output.txt"
fi

section "BENCHMARK COMPOSE LOGS"
if [ -f "${BENCH_DIR}/compose_services.log" ]; then
  print_file_if_exists "${BENCH_DIR}/compose_services.log"
else
  echo "No compose_services.log found at ${BENCH_DIR}/compose_services.log"
fi

section "BENCHMARK GENERATED COMPOSE AND WORKER LISTS"
print_file_if_exists "${BENCH_DIR}/docker-compose.generated.yml"
print_file_if_exists "${BENCH_DIR}/workers.txt"
print_file_if_exists "${BENCH_DIR}/workers.host.txt"

echo
echo "Resolved generated compose config from benchmark output:"
if [ -f "${BENCH_DIR}/docker-compose.generated.yml" ]; then
  run_allow_fail docker compose -f "${BENCH_DIR}/docker-compose.generated.yml" config
fi

section "EVENTS CSV PREVIEW"
LATEST_EVENTS="$(find benchmark_output -name events.csv -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)"
echo "Latest events.csv: $LATEST_EVENTS"

if [ -n "$LATEST_EVENTS" ] && [ -f "$LATEST_EVENTS" ]; then
  echo
  echo "Line count:"
  run_allow_fail wc -l "$LATEST_EVENTS"

  echo
  echo "Head:"
  run_allow_fail head -30 "$LATEST_EVENTS"

  echo
  echo "Tail:"
  run_allow_fail tail -30 "$LATEST_EVENTS"
else
  echo "No events.csv found."
fi

section "JSONL OUTPUT PREVIEW"
if [ -d "$BENCH_DIR" ]; then
  for f in "$BENCH_DIR"/client-*.jsonl; do
    if [ -f "$f" ]; then
      echo
      echo "### $f"
      run_allow_fail wc -l "$f"
      run_allow_fail head -5 "$f"
      run_allow_fail tail -5 "$f"
    fi
  done
fi

section "CARGO TARGET RELEASE BINARY SIZES IF PRESENT"
echo "These are host build artifacts, if present."
run_allow_fail ls -lah target/release/ds target/release/message_relay target/release/worker target/release/benchmark_runner_http_staircase
run_allow_fail wc -c target/release/ds target/release/message_relay target/release/worker target/release/benchmark_runner_http_staircase
run_allow_fail file target/release/ds target/release/message_relay target/release/worker target/release/benchmark_runner_http_staircase
run_allow_fail ldd target/release/ds target/release/message_relay target/release/worker target/release/benchmark_runner_http_staircase
run_allow_fail size target/release/ds target/release/message_relay target/release/worker target/release/benchmark_runner_http_staircase

section "DOCKER DISK USAGE AFTER EVERYTHING"
run_allow_fail docker system df
run_allow_fail docker builder du

section "FINAL SUMMARY"
echo "Diagnosis run id:        $RUN_ID"
echo "Main report file:        $REPORT"
echo "Benchmark run id:        ${RUN_ID}-bench"
echo "Benchmark directory:     $BENCH_DIR"
echo "Benchmark exit code:     $BENCH_RC"
echo "Relay port used:         $RELAY_PORT"
echo "DS port used:            $DS_PORT"
echo "Base worker port used:   $BASE_WORKER_PORT"

echo
echo "Important files:"
echo "- $REPORT"
echo "- $GEN_COMPOSE"
echo "- $GEN_WORKERS_INTERNAL"
echo "- $GEN_WORKERS_HOST"
echo "- ${BENCH_DIR}/docker-compose.generated.yml"
echo "- ${BENCH_DIR}/workers.host.txt"
echo "- ${BENCH_DIR}/terminal_output.txt"
echo "- ${BENCH_DIR}/compose_services.log"

echo
if [ "$BENCH_RC" -ne 0 ]; then
  echo "The tiny benchmark failed. That is still useful; send the report file."
  echo "If the failure is only old mls-* containers or ports, rerun with:"
  echo
  echo "  FORCE_CLEANUP_MLS_PORTS=1 ./diagnose_docker_baseline.sh"
else
  echo "The tiny benchmark completed successfully."
fi

echo
echo "Send me this single file:"
echo "$REPORT"

exit 0