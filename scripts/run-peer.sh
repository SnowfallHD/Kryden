#!/usr/bin/env bash
set -euo pipefail

: "${KRYDEN_PEER_ID:?Set KRYDEN_PEER_ID}"
: "${KRYDEN_CAPACITY_BYTES:?Set KRYDEN_CAPACITY_BYTES}"
: "${KRYDEN_TRUSTED_AUTHORITY_ID:?Set KRYDEN_TRUSTED_AUTHORITY_ID}"
: "${KRYDEN_TRUSTED_AUTHORITY_PUBLIC_KEY_BASE64:?Set KRYDEN_TRUSTED_AUTHORITY_PUBLIC_KEY_BASE64}"

HOST="${KRYDEN_HOST:-0.0.0.0}"
PORT="${KRYDEN_PORT:-9443}"
STORAGE_DIR="${KRYDEN_STORAGE_DIR:-/var/lib/kryden/${KRYDEN_PEER_ID}}"
FAILURE_BUCKET="${KRYDEN_FAILURE_BUCKET:-${KRYDEN_PEER_ID}}"
FAILURE_HOST="${KRYDEN_FAILURE_HOST:-$(hostname)}"
HEARTBEAT_TTL_MS="${KRYDEN_HEARTBEAT_TTL_MS:-30000}"
RESERVED_BYTES="${KRYDEN_RESERVED_BYTES:-0}"

args=(
  "dist/cli.js"
  "peer-runtime"
  "--id" "${KRYDEN_PEER_ID}"
  "--capacity-bytes" "${KRYDEN_CAPACITY_BYTES}"
  "--host" "${HOST}"
  "--port" "${PORT}"
  "--reserved-bytes" "${RESERVED_BYTES}"
  "--heartbeat-ttl-ms" "${HEARTBEAT_TTL_MS}"
  "--failure-bucket" "${FAILURE_BUCKET}"
  "--failure-host" "${FAILURE_HOST}"
  "--storage-dir" "${STORAGE_DIR}"
  "--trusted-authority-id" "${KRYDEN_TRUSTED_AUTHORITY_ID}"
  "--trusted-authority-public-key-base64" "${KRYDEN_TRUSTED_AUTHORITY_PUBLIC_KEY_BASE64}"
)

if [[ -n "${KRYDEN_REPAIR_HEADROOM_BYTES:-}" ]]; then
  args+=("--repair-headroom-bytes" "${KRYDEN_REPAIR_HEADROOM_BYTES}")
fi

if [[ -n "${KRYDEN_TLS_CERT:-}" || -n "${KRYDEN_TLS_KEY:-}" ]]; then
  : "${KRYDEN_TLS_CERT:?Set KRYDEN_TLS_CERT when KRYDEN_TLS_KEY is set}"
  : "${KRYDEN_TLS_KEY:?Set KRYDEN_TLS_KEY when KRYDEN_TLS_CERT is set}"
  args+=("--tls-cert" "${KRYDEN_TLS_CERT}" "--tls-key" "${KRYDEN_TLS_KEY}")
fi

exec node "${args[@]}"
