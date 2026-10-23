#!/usr/bin/env bash
set -uo pipefail

: "${SERVER_CMD:?SERVER_CMD is required}"
LOADGEN_CPUS=${LOADGEN_CPUS:-2-3}
SEED=seed/feed.db
DB=bench-db/feed.db
mkdir -p bench-db

cleanup() {
  docker rm -f bench >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_level() {
  local n=$1
  local log=/tmp/level.log
  cleanup
  rm -f "$DB" "$DB-wal" "$DB-shm"
  cp "$SEED" "$DB"
  echo "LEVEL_START n=$n $(date -u +%H:%M:%S)"

  docker run -d --name bench --network host \
    --cpuset-cpus=0 --cpus=1 --memory=2g --memory-swap=2g \
    --ulimit nofile=65535:65535 --security-opt seccomp=unconfined \
    -v "$PWD":/work -w /work \
    -e SQLITE_PATH=/work/bench-db/feed.db \
    -e JWT_SECRET=twelve-dollar-challenge \
    -e HOST=127.0.0.1 -e PORT=3000 \
    -e IOURING_BUFS=256 -e ZEROCOPY_STATS=0 \
    ubuntu:24.04 bash -lc "apt-get update -qq && (apt-get install -y -qq libssl3t64 >/dev/null 2>&1 || apt-get install -y -qq libssl3 >/dev/null 2>&1 || true); exec $SERVER_CMD" >/dev/null

  local up=0
  for _ in $(seq 150); do
    if curl -sf http://127.0.0.1:3000/health >/dev/null 2>&1; then up=1; break; fi
    if ! docker inspect -f '{{.State.Running}}' bench 2>/dev/null | grep -q true; then break; fi
    sleep 1
  done
  if [ "$up" -ne 1 ]; then
    echo "LEVEL_FAIL n=$n reason=startup"
    docker logs bench 2>&1 | tail -40 || true
    cleanup
    return 1
  fi

  ulimit -n 1000000 2>/dev/null || ulimit -n "$(ulimit -Hn)" 2>/dev/null || true
  timeout 420 taskset -c "$LOADGEN_CPUS" /tmp/loadgen -H 127.0.0.1 -p 3000 \
    -c "$n" -d 360 -t 9000 -m mixed -T "$PWD/seed/tokens.json" >"$log" 2>&1
  local rc=$?

  local connected completed errors p95 p99 pass
  connected=$(awk '/^connections /{for(i=1;i<=NF;i++) if($i ~ /^connected=/){sub("connected=","",$i); print $i}}' "$log" | tail -1)
  completed=$(awk '/^requests /{for(i=1;i<=NF;i++) if($i ~ /^completed=/){sub("completed=","",$i); print $i}}' "$log" | tail -1)
  errors=$(awk '/^errors /{for(i=1;i<=NF;i++) if($i ~ /^total=/){sub("total=","",$i); print $i}}' "$log" | tail -1)
  p95=$(grep -o 'p95=[0-9.]*' "$log" | tail -1 | cut -d= -f2)
  p99=$(grep -o 'p99=[0-9.]*' "$log" | tail -1 | cut -d= -f2)
  connected=${connected:-0}
  completed=${completed:-0}
  errors=${errors:-0}
  p95=${p95:-999999}
  p99=${p99:-999999}
  pass=1
  [ "$rc" -eq 0 ] || pass=0
  [ "$connected" -ge "$n" ] || pass=0
  [ "$completed" -gt 0 ] || pass=0
  awk -v e="$errors" -v c="$completed" 'BEGIN{exit !(c > 0 && e * 100 < c)}' || pass=0
  awk -v v="$p95" 'BEGIN{exit !(v < 500)}' || pass=0
  awk -v v="$p99" 'BEGIN{exit !(v < 1000)}' || pass=0

  echo "LEVEL_RESULT n=$n rc=$rc connected=$connected completed=$completed errors=$errors p95=$p95 p99=$p99 pass=$pass"
  tail -12 "$log" || true
  if [ "$pass" -ne 1 ]; then
    echo "LEVEL_SERVER_LOG n=$n"
    docker logs bench 2>&1 | tail -60 || true
  fi
  cleanup
  [ "$pass" -eq 1 ]
}

# Establish a known-passing lower bound.
if ! run_level 64000; then
  echo "MAX_SEARCH_FAILED lower_bound_64000"
  exit 1
fi

lo=64000
hi=512000

# Check the upper bound before binary search.
if run_level "$hi"; then
  lo=$hi
  hi=640000
  if run_level "$hi"; then
    echo "MAX_RANGE lo=$hi hi=over_cap"
    exit 0
  fi
fi

while [ $((hi - lo)) -gt 10000 ]; do
  mid=$(( (lo + hi) / 2 ))
  mid=$(( (mid / 5000) * 5000 ))
  if [ "$mid" -le "$lo" ] || [ "$mid" -ge "$hi" ]; then
    break
  fi
  if run_level "$mid"; then
    lo=$mid
  else
    hi=$mid
  fi
done

echo "MAX_RANGE lo=$lo hi=$hi"
