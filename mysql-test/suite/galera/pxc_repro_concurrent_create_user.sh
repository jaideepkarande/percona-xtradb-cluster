#!/usr/bin/env bash
#
# Reproduce concurrent CREATE USER on two PXC / Galera nodes (different users
# on each node). Can bootstrap a 2-node cluster via mysql-test-run.pl or use
# your own ports.
#
# === One-shot: start cluster, hammer, stop cluster ===
#   export MYSQL_PWD=secret
#   export MTR_BINDIR=/path/to/build/BIN
#   ./pxc_repro_concurrent_create_user.sh run --sleep 0.5 \
#       --seq2-start 10001 --seq2-end 20000 \
#       --seq1-start 20001 --seq1-end 30000
#
# === Manual: start servers, then hammer ===
#   export MYSQL_PWD=secret MTR_BINDIR=/path/to/build/BIN
#   ./pxc_repro_concurrent_create_user.sh start-cluster
#   source mysql-test/suite/galera/pxc_repro_cluster.env   # or path printed by start-cluster
#   ./pxc_repro_concurrent_create_user.sh hammer --mysql "$MTR_BINDIR/runtime_output_directory/mysql" \
#       --port1 "$PXC_REPRO_PORT1" --port2 "$PXC_REPRO_PORT2"
#   ./pxc_repro_concurrent_create_user.sh stop-cluster
#
# === Already have a cluster (custom ports) ===
#   ./pxc_repro_concurrent_create_user.sh hammer --mysql ./mysql \
#       --port1 3306 --port2 3307 --sleep 0.5
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# This script lives in mysql-test/suite/galera/
MYSQL_TEST_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${PXC_REPRO_ENV_FILE:-${SCRIPT_DIR}/pxc_repro_cluster.env}"
MTR_GALERA_TEST="${MTR_GALERA_TEST:-galera.pxc_create_user_concurrent_nodes}"

SUBCMD="hammer"
if [[ $# -ge 1 && "$1" =~ ^(start-cluster|stop-cluster|hammer|run|help)$ ]]; then
  SUBCMD="$1"
  shift
fi

MYSQL_BIN="${MYSQL_BIN:-mysql}"
MYSQLADMIN_BIN="${MYSQLADMIN_BIN:-}"
USER="${MYSQL_USER:-root}"
PASS_ENV="${MYSQL_PWD-}"
HOST1="127.0.0.1"
PORT1=""
HOST2="127.0.0.1"
PORT2=""
SEQ1_START="${SEQ1_START:-2001}"
SEQ1_END="${SEQ1_END:-2100}"
SEQ2_START="${SEQ2_START:-1001}"
SEQ2_END="${SEQ2_END:-2000}"
SLEEP_SEC="${SLEEP_SEC:-0.05}"
PASSWD="${CREATE_USER_PASSWORD:-Iab3ohl5Du}"
PREFIX1="${USER_PREFIX1:-u}"
PREFIX2="${USER_PREFIX2:-u}"
MTR_BINDIR="${MTR_BINDIR:-}"

usage() {
  sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
  echo ""
  echo "Subcommands: start-cluster | stop-cluster | hammer | run | help"
  echo "Options (hammer / run):"
  echo "  --mysql PATH          mysql client (default: mysql or \$MTR_BINDIR/.../mysql)"
  echo "  --mysqladmin PATH     for stop-cluster (default: next to --mysql or MTR bindir)"
  echo "  --user|-u USER        default root"
  echo "  --host1 / --port1     node 1 TCP (ports from start-cluster if omitted and env file exists)"
  echo "  --host2 / --port2     node 2 TCP"
  echo "  --seq1-start/end      CREATE USER on node1 (default u2001..u2100)"
  echo "  --seq2-start/end      CREATE USER on node2 (default u1001..u2000)"
  echo "  --sleep SEC           pause between statements (e.g. 0.5)"
  echo "  --password PASS       sets MYSQL_PWD for this process"
  echo "  --create-user-password / --prefix1 / --prefix2"
  echo "start-cluster extras:"
  echo "  --mtr-bindir DIR      same as env MTR_BINDIR (build tree BIN dir)"
  echo "  --mysql-test-dir DIR  default: mysql-test containing this script"
  echo "  --mtr-test NAME       default: ${MTR_GALERA_TEST}"
  echo "  --env-file PATH       where to write ports (default: ${ENV_FILE})"
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysql) MYSQL_BIN="$2"; shift 2 ;;
    --mysqladmin) MYSQLADMIN_BIN="$2"; shift 2 ;;
    --user|-u) USER="$2"; shift 2 ;;
    --host1) HOST1="$2"; shift 2 ;;
    --port1) PORT1="$2"; shift 2 ;;
    --host2) HOST2="$2"; shift 2 ;;
    --port2) PORT2="$2"; shift 2 ;;
    --seq1-start) SEQ1_START="$2"; shift 2 ;;
    --seq1-end) SEQ1_END="$2"; shift 2 ;;
    --seq2-start) SEQ2_START="$2"; shift 2 ;;
    --seq2-end) SEQ2_END="$2"; shift 2 ;;
    --sleep) SLEEP_SEC="$2"; shift 2 ;;
    --password) PASS_ENV="$2"; export MYSQL_PWD="$2"; shift 2 ;;
    --create-user-password) PASSWD="$2"; shift 2 ;;
    --prefix1) PREFIX1="$2"; shift 2 ;;
    --prefix2) PREFIX2="$2"; shift 2 ;;
    --mtr-bindir) MTR_BINDIR="$2"; shift 2 ;;
    --mysql-test-dir) MYSQL_TEST_DIR="$2"; shift 2 ;;
    --mtr-test) MTR_GALERA_TEST="$2"; shift 2 ;;
    --env-file) ENV_FILE="$2"; shift 2 ;;
    -h|--help) usage ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
done

resolve_build_bins() {
  if [[ -z "$MTR_BINDIR" ]]; then
    return 0
  fi
  local rt="$MTR_BINDIR/runtime_output_directory"
  if [[ -x "$rt/mysql" && "$MYSQL_BIN" == "mysql" ]]; then
    MYSQL_BIN="$rt/mysql"
  fi
  if [[ -z "$MYSQLADMIN_BIN" ]]; then
    if [[ -x "$rt/mysqladmin" ]]; then
      MYSQLADMIN_BIN="$rt/mysqladmin"
    fi
  fi
}

find_generated_mycnf() {
  local d f
  for d in "$MTR_BINDIR/mysql-test/var" "$MYSQL_TEST_DIR/var"; do
    [[ -d "$d" ]] || continue
    f=$(find "$d" -name my.cnf -type f 2>/dev/null | head -1)
    if [[ -n "$f" ]]; then
      echo "$f"
      return 0
    fi
  done
  return 1
}

extract_env_port() {
  local mycnf=$1 key=$2
  awk -F= -v k="$key" '$1==k { gsub(/^ +| +$/,"",$2); print $2; exit }' "$mycnf"
}

cmd_start_cluster() {
  if [[ -z "$MTR_BINDIR" ]]; then
    echo "ERROR: set MTR_BINDIR to your build tree (directory that contains runtime_output_directory/), or pass --mtr-bindir." >&2
    exit 1
  fi
  local mtr="$MYSQL_TEST_DIR/mysql-test-run.pl"
  if [[ ! -f "$mtr" ]]; then
    echo "ERROR: mysql-test-run.pl not found under MYSQL_TEST_DIR=$MYSQL_TEST_DIR" >&2
    exit 1
  fi
  echo "==> Starting 2-node Galera via MTR (leaves mysqld running):"
  echo "    cd $MYSQL_TEST_DIR && MTR_BINDIR=$MTR_BINDIR $mtr --start-and-exit $MTR_GALERA_TEST"
  (
    cd "$MYSQL_TEST_DIR"
    export MTR_BINDIR
    ./mysql-test-run.pl --start-and-exit "$MTR_GALERA_TEST"
  )
  local mycnf
  mycnf=$(find_generated_mycnf) || {
    echo "ERROR: could not find generated var/**/my.cnf under $MTR_BINDIR/mysql-test/var or $MYSQL_TEST_DIR/var" >&2
    exit 1
  }
  local p1 p2
  p1=$(extract_env_port "$mycnf" NODE_MYPORT_1)
  p2=$(extract_env_port "$mycnf" NODE_MYPORT_2)
  if [[ -z "$p1" || -z "$p2" ]]; then
    echo "ERROR: could not read NODE_MYPORT_1/2 from $mycnf" >&2
    exit 1
  fi
  umask 077
  cat >"$ENV_FILE" <<EOF
# Generated by $0 start-cluster — source before hammer, or use \`run\` subcommand.
export PXC_REPRO_PORT1=$p1
export PXC_REPRO_PORT2=$p2
export PXC_REPRO_MYCNF=$mycnf
export MTR_BINDIR=$MTR_BINDIR
export MYSQL_TEST_DIR=$MYSQL_TEST_DIR
export MYSQL_BIN="${MTR_BINDIR}/runtime_output_directory/mysql"
export MYSQLADMIN_BIN="${MTR_BINDIR}/runtime_output_directory/mysqladmin"
EOF
  echo ""
  echo "Wrote $ENV_FILE"
  echo "  NODE 1 (donor/bootstrap side in MTR): 127.0.0.1:$p1"
  echo "  NODE 2: 127.0.0.1:$p2"
  echo ""
  echo "Next:"
  echo "  export MYSQL_PWD=...   # if needed"
  echo "  source $ENV_FILE"
  echo "  $0 hammer --mysql \"\$MYSQL_BIN\" --port1 \"\$PXC_REPRO_PORT1\" --port2 \"\$PXC_REPRO_PORT2\""
}

cmd_stop_cluster() {
  if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
  fi
  PORT1="${PORT1:-${PXC_REPRO_PORT1:-}}"
  PORT2="${PORT2:-${PXC_REPRO_PORT2:-}}"
  resolve_build_bins
  if [[ -z "$MYSQLADMIN_BIN" ]]; then
    echo "ERROR: mysqladmin not found; set MYSQLADMIN_BIN or MTR_BINDIR and run stop-cluster with pxc_repro_cluster.env present." >&2
    exit 1
  fi
  if [[ -z "$PORT1" || -z "$PORT2" ]]; then
    echo "ERROR: need ports (source $ENV_FILE or pass --port1/--port2)." >&2
    exit 1
  fi
  if [[ -n "${PASS_ENV}" ]]; then
    export MYSQL_PWD="$PASS_ENV"
  fi
  echo "==> Shutting down mysqld on :$PORT2 then :$PORT1"
  "$MYSQLADMIN_BIN" -u"$USER" -h127.0.0.1 -P"$PORT2" shutdown || true
  "$MYSQLADMIN_BIN" -u"$USER" -h127.0.0.1 -P"$PORT1" shutdown || true
  echo "Done (ignore errors if already stopped)."
}

run_loop() {
  local host=$1 port=$2 start=$3 end=$4 prefix=$5
  local i
  for i in $(seq "$start" "$end"); do
    echo "CREATE USER '${prefix}${i}'@'%' IDENTIFIED BY '${PASSWD}';"
    sleep "$SLEEP_SEC"
  done | "$MYSQL_BIN" -u"$USER" -N -B -h"$host" -P"$port"
}

cmd_hammer() {
  if [[ -z "$PORT1" || -z "$PORT2" ]]; then
    if [[ -f "$ENV_FILE" ]]; then
      # shellcheck disable=SC1090
      source "$ENV_FILE"
      PORT1="${PORT1:-${PXC_REPRO_PORT1:-}}"
      PORT2="${PORT2:-${PXC_REPRO_PORT2:-}}"
    fi
  fi
  if [[ -z "$PORT1" || -z "$PORT2" ]]; then
    echo "ERROR: need --port1/--port2 or run start-cluster first (see $ENV_FILE)." >&2
    exit 1
  fi
  resolve_build_bins
  if [[ -n "${PASS_ENV}" ]]; then
    export MYSQL_PWD="$PASS_ENV"
  fi

  echo "==> Node1 ${HOST1}:${PORT1}  CREATE USER ${PREFIX1}<${SEQ1_START}..${SEQ1_END}>@'%'"
  echo "==> Node2 ${HOST2}:${PORT2}  CREATE USER ${PREFIX2}<${SEQ2_START}..${SEQ2_END}>@'%'"
  echo "    sleep between statements: ${SLEEP_SEC}s"
  echo ""

  local p1 p2 pid1 pid2 st1 st2
  p1=$(mktemp -t pxc_repro_node1.XXXXXX.log)
  p2=$(mktemp -t pxc_repro_node2.XXXXXX.log)
  pid1=""
  pid2=""

  kill_workers() {
    if [[ -n "${pid1}" ]] && kill -0 "${pid1}" 2>/dev/null; then kill "${pid1}" 2>/dev/null || true; fi
    if [[ -n "${pid2}" ]] && kill -0 "${pid2}" 2>/dev/null; then kill "${pid2}" 2>/dev/null || true; fi
    wait || true
  }
  trap kill_workers INT TERM

  set +e
  run_loop "$HOST1" "$PORT1" "$SEQ1_START" "$SEQ1_END" "$PREFIX1" >"$p1" 2>&1 &
  pid1=$!
  run_loop "$HOST2" "$PORT2" "$SEQ2_START" "$SEQ2_END" "$PREFIX2" >"$p2" 2>&1 &
  pid2=$!

  echo "Started workers pid1=$pid1 pid2=$pid2 (Ctrl+C to stop)"
  wait "$pid1"
  st1=$?
  wait "$pid2"
  st2=$?
  pid1=""
  pid2=""
  set -e
  trap - INT TERM
  kill_workers

  echo "--- node1 log tail ($p1) ---"
  tail -n 20 "$p1" 2>/dev/null || true
  echo "--- node2 log tail ($p2) ---"
  tail -n 20 "$p2" 2>/dev/null || true
  rm -f "$p1" "$p2"

  echo "node1 exit=$st1 node2 exit=$st2"
  if [[ $st1 -ne 0 || $st2 -ne 0 ]]; then
    echo "One or both loops failed; see logs above." >&2
    exit 1
  fi
  echo "Done. Drop users (from any node), e.g.:"
  echo "  for i in \$(seq $SEQ2_START $SEQ2_END); do echo \"DROP USER IF EXISTS '${PREFIX2}\$i'@'%';\"; done | $MYSQL_BIN -u$USER -h$HOST1 -P$PORT1"
  echo "  for i in \$(seq $SEQ1_START $SEQ1_END); do echo \"DROP USER IF EXISTS '${PREFIX1}\$i'@'%';\"; done | $MYSQL_BIN -u$USER -h$HOST1 -P$PORT1"
}

cmd_run() {
  if [[ -z "$MTR_BINDIR" ]]; then
    echo "ERROR: run requires MTR_BINDIR or --mtr-bindir" >&2
    exit 1
  fi
  cmd_start_cluster
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  PORT1="$PXC_REPRO_PORT1"
  PORT2="$PXC_REPRO_PORT2"
  cmd_hammer
  cmd_stop_cluster
}

case "$SUBCMD" in
  help) usage ;;
  start-cluster) cmd_start_cluster ;;
  stop-cluster) cmd_stop_cluster ;;
  hammer) cmd_hammer ;;
  run) cmd_run ;;
  *) echo "BUG: bad subcmd $SUBCMD" >&2; exit 1 ;;
esac
