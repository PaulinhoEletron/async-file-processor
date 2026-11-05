#!/usr/bin/env bash
# End-to-end check against the containerised stack: builds and starts it with Docker Compose,
# uploads CSVs through the public API and asserts results, limits and worker sharing.
# The stack is left running afterwards (stop it with `make down`); stored data is not deleted.
set -euo pipefail

cd "$(dirname "$0")/.."

API=http://localhost:8080
TMP=$(mktemp -d)

cleanup() {
	local status=$?
	trap - EXIT
	if [ "$status" -ne 0 ]; then
		echo "--- compose logs (tail) ---" >&2
		docker compose logs --no-color --tail 15 api worker >&2 || true
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

for tool in curl python3 docker go; do
	command -v "$tool" >/dev/null || fail "$tool is required"
done

echo "== starting the stack"
# Defaults, not whatever a local .env sets, so the limits below are the documented ones.
export UPLOAD_RATE_PER_MIN=10 UPLOAD_RATE_BURST=5 WORKER_CONCURRENCY=4 RETENTION=24h
unset DEMO_MAX_UPLOAD_BYTES
docker compose up -d --build --wait >/dev/null || fail "docker compose up failed"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$API/readyz" || true)" = 200 ] || fail "/readyz did not return 200"
echo "ok: all services are healthy"
LIMIT=52428800

# upload_as <client ip> <file> <output>: POST a CSV as a given client (the rate limit is per
# client, and the API trusts CF-Connecting-IP, so each logical client gets its own bucket).
upload_as() {
	curl -s -o "$3" -w '%{http_code}' -H "CF-Connecting-IP: $1" -F "file=@$2;filename=$(basename "$2")" "$API/api/jobs"
}

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

RUN_IP="198.51.100.$((RANDOM % 200 + 1))"

echo "== sample file"
curl -sf "$API/api/sample.csv" -o "$TMP/sample.csv" || fail "could not download sample.csv"
expect "sample upload status" "$(upload_as "$RUN_IP" "$TMP/sample.csv" "$TMP/job.json")" 202
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
expect "bad header upload status" "$(upload_as "$RUN_IP" "$TMP/bad.csv" "$TMP/bad.json")" 202
BAD=$(json "$TMP/bad.json" 'd["id"]')
wait_for_status "$BAD" FAILED "$TMP/bad.json"
expect "bad header attempts" "$(json "$TMP/bad.json" 'd["attempts"]')" 1

echo "== two workers share the load"
go run ./scripts/gencsv -rows 300000 -bad 0.05 -seed 7 -out "$TMP/batch.csv" 2>/dev/null
IDS=()
for i in $(seq 1 8); do
	[ "$(upload_as "198.51.101.$i" "$TMP/batch.csv" "$TMP/batch$i.json")" = 202 ] || fail "batch upload $i failed"
	IDS+=("$(json "$TMP/batch$i.json" 'd["id"]')")
done
for id in "${IDS[@]}"; do
	wait_for_status "$id" COMPLETED "$TMP/batch.json"
	expect "batch job total_rows" "$(json "$TMP/batch.json" 'd["total_rows"]')" 300000
done
docker compose logs --no-color worker >"$TMP/worker.log" 2>&1
printf '%s\n' "${IDS[@]}" >"$TMP/ids.txt"
WORKERS=$(grep '"job started"' "$TMP/worker.log" | grep -F -f "$TMP/ids.txt" | cut -d'|' -f1 | sort -u | wc -l)
[ "$WORKERS" -ge 2 ] || fail "only $WORKERS worker container handled the batch; expected 2"
echo "ok: $WORKERS worker containers processed the batch"

echo "== limits"
CODE=$(upload_as 198.51.102.1 "$TMP/sample.csv" "$TMP/limit.json")
expect "upload before the limit" "$CODE" 202
head -c $((LIMIT + 1048576)) /dev/zero >"$TMP/huge.csv"
CODE=$(upload_as 198.51.102.2 "$TMP/huge.csv" "$TMP/huge.json" || true)
expect "oversized upload status" "$CODE" 413
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["error"]' "$TMP/huge.json" || fail "413 body is not a JSON error"
for i in 1 2 3 4 5; do upload_as 198.51.102.3 "$TMP/bad.csv" "$TMP/limit.json" >/dev/null; done
curl -s -D "$TMP/limit.headers" -o "$TMP/limit.json" -H "CF-Connecting-IP: 198.51.102.3" -F "file=@$TMP/bad.csv" "$API/api/jobs" >/dev/null
grep -q '^HTTP/.* 429' "$TMP/limit.headers" || fail "sixth rapid upload was not rate limited"
grep -qi '^retry-after: [0-9]' "$TMP/limit.headers" || fail "429 has no Retry-After header"
python3 -c 'import json,sys; assert json.load(open(sys.argv[1]))["error"]' "$TMP/limit.json" || fail "429 body is not a JSON error"
echo "ok: rapid uploads are limited with 429 and Retry-After"

echo "== security headers"
curl -s -D "$TMP/ui.headers" -o /dev/null "$API/"
for h in 'content-security-policy: default-src' 'x-content-type-options: nosniff' 'x-frame-options: DENY'; do
	grep -qi "^$h" "$TMP/ui.headers" || fail "UI response lacks header $h"
done
echo "ok: security headers present"

echo "== metrics"
curl -sf "$API/metrics" -o "$TMP/api-metrics.txt" || fail "could not fetch api metrics"
grep -q 'jobs_total{status="created"} [1-9]' "$TMP/api-metrics.txt" || fail 'api metrics lack jobs_total{status="created"}'
grep -q '^upload_bytes_total [1-9]' "$TMP/api-metrics.txt" || fail "upload_bytes_total did not move"
echo "ok: metrics exposed"

echo "E2E PASS"
