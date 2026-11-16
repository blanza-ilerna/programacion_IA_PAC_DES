#!/usr/bin/env bash
# test_connectivity.sh - Verifica que todos los servicios responden
set -euo pipefail

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
NC="\033[0m"

PASS=0
FAIL=0

check_http() {
  local name="$1"
  local url="$2"
  local expected="${3:-200}"
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "${url}" 2>/dev/null || echo "000")
  if [[ "${code}" == "${expected}" || ( "${expected}" == "200" && "${code}" =~ ^[23] ) ]]; then
    echo -e "  ${GREEN}✅ ${name}${NC} → ${url} (HTTP ${code})"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}❌ ${name}${NC} → ${url} (HTTP ${code}, esperado ${expected})"
    FAIL=$((FAIL + 1))
  fi
}

check_tcp() {
  local name="$1"
  local host="$2"
  local port="$3"
  if timeout 3 bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null; then
    echo -e "  ${GREEN}✅ ${name}${NC} → ${host}:${port} (TCP OK)"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}❌ ${name}${NC} → ${host}:${port} (TCP sin respuesta)"
    FAIL=$((FAIL + 1))
  fi
}

check_docker_health() {
  local name="$1"
  local service="$2"
  local cid
  cid=$(docker ps -q --filter "label=com.docker.compose.service=${service}" 2>/dev/null | head -1 || true)
  if [[ -z "${cid}" ]]; then
    echo -e "  ${RED}❌ ${name}${NC} → contenedor no encontrado"
    FAIL=$((FAIL + 1))
    return
  fi
  local health
  health=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' "${cid}" 2>/dev/null)
  if [[ "${health}" == "healthy" || "${health}" == "no-healthcheck" ]]; then
    echo -e "  ${GREEN}✅ ${name}${NC} → ${health}"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}❌ ${name}${NC} → ${health}"
    FAIL=$((FAIL + 1))
  fi
}

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║     ILERNA PAC DES - Test de Conectividad            ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

echo "── Docker Health ───────────────────────────────────────"
check_docker_health "Redpanda"         "redpanda"
check_docker_health "Redpanda Console" "redpanda-console"
check_docker_health "Flink JobManager" "jobmanager"
check_docker_health "Flink TaskManager" "taskmanager"
check_docker_health "InfluxDB"         "influxdb"
check_docker_health "MinIO"            "minio"

echo ""
echo "── HTTP / Web UIs ──────────────────────────────────────"
check_http "Redpanda Console" "http://redpanda-console:8080/"
check_http "Flink UI"         "http://jobmanager:8081/"
check_http "InfluxDB UI"      "http://influxdb:8086/health"
check_http "S3 (RustFS) health" "http://minio:9000/health"
check_http "S3 (RustFS) Console" "http://minio:9001/"
check_http "Grafana"          "http://grafana:3000/api/health"

echo ""
echo "── TCP / Protocolos ────────────────────────────────────"
check_tcp "MQTT Broker"    "mosquitto"  "1883"
check_tcp "Redpanda Kafka" "redpanda"   "29092"
check_tcp "MinIO S3 API"   "minio"      "9000"

echo ""
echo "──────────────────────────────────────────────────────"
TOTAL=$((PASS + FAIL))
if [[ "${FAIL}" -eq 0 ]]; then
  echo -e "  ${GREEN}✅ Todos los checks pasaron (${PASS}/${TOTAL})${NC}"
else
  echo -e "  ${YELLOW}⚠️  ${PASS}/${TOTAL} OK — ${FAIL} fallos${NC}"
fi
echo ""
exit "${FAIL}"
