#!/usr/bin/env bash
# Espera até que todos os serviços do compose com healthcheck estejam saudáveis.
# Uso: scripts/wait_healthy.sh [timeout_segundos]
set -euo pipefail
cd "$(dirname "$0")/.."
TIMEOUT="${1:-120}"
deadline=$(( $(date +%s) + TIMEOUT ))

while :; do
  # Serviços sem healthcheck aparecem com Health vazio; esses contam como prontos se Up.
  pending=$(docker compose ps --format '{{.Service}}|{{.Health}}|{{.State}}' \
            | awk -F'|' '$2 == "starting" || ($2 == "" && $3 != "running") || $2 == "unhealthy" {print $1}')
  if [ -z "$pending" ]; then
    docker compose ps --format '{{.Service}}	{{.Status}}'
    exit 0
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "TIMEOUT esperando: $pending" >&2
    docker compose ps --format '{{.Service}}	{{.Status}}' >&2
    exit 1
  fi
  sleep 2
done
