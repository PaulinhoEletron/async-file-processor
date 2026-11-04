#!/usr/bin/env bash
# End-to-end check: starts the stack, runs the API and worker, uploads CSVs and asserts results.
set -euo pipefail

cd "$(dirname "$0")/.."

API=http://localhost:8080
WORKER_METRICS=http://localhost:9091
TMP=$(mktemp -d)
PIDS=()

cleanup() {
	local status=$?
	trap - EXIT
	for pid in "${PIDS[@]:-}"; do
		[ -n "$pid" ] && kill "$pid" 2>/dev/null || true
	done
	for pid in "${PIDS[@]:-}"; do
		[ -n "$pid" ] && wait "$pid" 2>/dev/null || true
	done
	if [ "$status" -ne 0 ]; then
		echo "--- api log (tail) ---" >&2
		tail -n 20 "$TMP/api.log" 2>/dev/null >&2 || true
		echo "--- worker log (tail) ---" >&2
		tail -n 20 "$TMP/worker.log" 2>/dev/null >&2 || true
		echo "E2E FAIL" >&2
	fi
	rm -rf "$TMP"
	exit "$status"
}
trap cleanup EXIT

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

# json <file> <python expression over d>: print a field of a JSON document.
json() {
	python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(eval(sys.argv[2]))' "$1" "$2"
}

port_in_use() {
	(exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

for port in 8080 9091; do
	if port_in_use "$port"; then
		fail "port $port is already in use; stop the process using it and retry"
	fi
done

for tool in curl python3 go; do
	command -v "$tool" >/dev/null || fail "$tool is required"
done

# Load settings the same way make does: .env, falling back to the example file.
set -a
# shellcheck disable=SC1091
if [ -f .env ]; then . ./.env; else . ./.env.example; fi
set +a
export FAIL_RATE=0

echo "== starting services"
make up >/dev/null

echo "== building binaries"
go build -o "$TMP/api" ./cmd/api
go build -o "$TMP/worker" ./cmd/worker

"$TMP/api" >"$TMP/api.log" 2>&1 &
PIDS+=($!)
# The worker expects the schema, which the API creates on start: wait for the API first.
for _ in $(seq 1 60); do
	[ "$(curl -s -o /dev/null -w '%{http_code}' "$API/readyz" || true)" = 200 ] && break
	kill -0 "${PIDS[0]}" 2>/dev/null || fail "api exited during startup"
	sleep 1
done
[ "$(curl -s -o /dev/null -w '%{http_code}' "$API/readyz" || true)" = 200 ] || fail "/readyz did not return 200"

"$TMP/worker" >"$TMP/worker.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 30); do
	[ "$(curl -s -o /dev/null -w '%{http_code}' "$WORKER_METRICS/healthz" || true)" = 200 ] && break
	kill -0 "${PIDS[1]}" 2>/dev/null || fail "worker exited during startup"
	sleep 1
done
[ "$(curl -s -o /dev/null -w '%{http_code}' "$WORKER_METRICS/healthz" || true)" = 200 ] || fail "worker /healthz did not return 200"
echo "ok: api and worker are ready"

# wait_for_status <job id> <status> <output file>: poll until the job reaches the status.
wait_for_status() {
	local id=$1 want=$2 out=$3 status=""
	for _ in $(seq 1 120); do
		curl -sf "$API/api/jobs/$id" -o "$out" || fail "could not fetch job $id"
		status=$(json "$out" 'd["status"]')
		[ "$status" = "$want" ] && return 0
		case "$status" in COMPLETED | FAILED) fail "job $id ended as $status, expected $want: $(cat "$out")" ;; esac
		sleep 0.5
	done
	fail "job $id did not reach $want within 60s (last status $status)"
}

expect() { # expect <description> <actual> <expected>
	[ "$2" = "$3" ] || fail "$1: got '$2', expected '$3'"
	echo "ok: $1 = $2"
}

echo "== sample file"
curl -sf "$API/api/sample.csv" -o "$TMP/sample.csv" || fail "could not download sample.csv"
curl -sf -F "file=@$TMP/sample.csv;filename=sample.csv" "$API/api/jobs" -o "$TMP/job.json" || fail "upload failed"
ID=$(json "$TMP/job.json" 'd["id"]')
echo "job $ID"
wait_for_status "$ID" COMPLETED "$TMP/job.json"
expect "total_rows" "$(json "$TMP/job.json" 'd["total_rows"]')" 200
expect "valid_rows" "$(json "$TMP/job.json" 'd["valid_rows"]')" 180
expect "invalid_rows" "$(json "$TMP/job.json" 'd["invalid_rows"]')" 20

curl -sf "$API/api/jobs/$ID/errors.csv" -o "$TMP/errors.csv" || fail "could not download errors.csv"
python3 - "$TMP/errors.csv" <<'PY' || fail "errors.csv does not have 20 data lines"
import csv, sys
rows = list(csv.reader(open(sys.argv[1], newline="")))
assert rows[0] == ["row", "field", "error", "raw"], rows[0]
assert len(rows) - 1 == 20, len(rows) - 1
PY
echo "ok: errors.csv has 20 data lines"

curl -sf "$API/api/jobs/$ID/report" -o "$TMP/report.json" || fail "could not fetch report"
expect "report error_count" "$(json "$TMP/report.json" 'd["error_count"]')" 20

echo "== bad header file"
printf 'a,b,c\n1,2,3\n' >"$TMP/bad.csv"
curl -sf -F "file=@$TMP/bad.csv;filename=bad.csv" "$API/api/jobs" -o "$TMP/bad.json" || fail "bad-header upload failed"
BAD=$(json "$TMP/bad.json" 'd["id"]')
wait_for_status "$BAD" FAILED "$TMP/bad.json"
expect "bad header attempts" "$(json "$TMP/bad.json" 'd["attempts"]')" 1

echo "== metrics"
curl -sf "$WORKER_METRICS/metrics" -o "$TMP/worker-metrics.txt" || fail "could not fetch worker metrics"
grep -q 'jobs_total{status="completed"}' "$TMP/worker-metrics.txt" || fail 'worker metrics lack jobs_total{status="completed"}'
grep -q '^worker_busy 0$' "$TMP/worker-metrics.txt" || fail "worker_busy is not 0 after the jobs finished"
curl -sf "$API/metrics" -o "$TMP/api-metrics.txt" || fail "could not fetch api metrics"
grep -q 'jobs_total{status="created"} [1-9]' "$TMP/api-metrics.txt" || fail 'api metrics lack jobs_total{status="created"}'
grep -q '^upload_bytes_total [1-9]' "$TMP/api-metrics.txt" || fail "upload_bytes_total did not move"
echo "ok: metrics exposed"

echo "E2E PASS"
