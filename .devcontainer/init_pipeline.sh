#!/usr/bin/env bash
# init_pipeline.sh — Inicializa topics Kafka, bucket MinIO, jobs Flink y aliases
# Ejecutar manualmente tras confirmar que la infraestructura está healthy:
#   source .devcontainer/init_pipeline.sh

GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
CYAN="\033[0;36m"
NC="\033[0m"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║     ILERNA - Inicializando pipeline de datos...     ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. Topics Redpanda ───────────────────────────────────────
echo ">>> [1/4] Recreando topics Kafka (purga mensajes de sesiones anteriores)..."
RP_ID="$(docker ps -qf 'label=com.docker.compose.service=redpanda' 2>/dev/null | head -1 || true)"

if [[ -n "${RP_ID}" ]]; then
  for TOPIC in sensors_raw sensors_clean sensors_invalid sensors_verified; do
    docker exec "${RP_ID}" rpk topic delete "${TOPIC}" > /dev/null 2>&1 || true
    docker exec "${RP_ID}" rpk topic create "${TOPIC}" -p 1 -r 1 > /dev/null 2>&1 \
      && echo -e "    ${GREEN}✅ Topic ${TOPIC} recreado${NC}" \
      || echo -e "    ${YELLOW}⚠️  No se pudo crear topic ${TOPIC}${NC}"
  done
else
  echo -e "    ${YELLOW}⚠️  Redpanda no encontrado${NC}"
fi

# ── 2. Bucket MinIO ──────────────────────────────────────────
echo ""
echo ">>> [2/4] Creando bucket MinIO y purgando archivos con schema incorrecto..."
python3 -c "
from minio import Minio
import json, sys

try:
    c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)

    if not c.bucket_exists('datalake'):
        c.make_bucket('datalake')
        print('    \033[32m✅ Bucket datalake creado\033[0m')
    else:
        print('    \033[33mℹ️  Bucket datalake ya existe\033[0m')

    # Purgar archivos con schema incorrecto (temperature en lugar de temperature_c).
    # Ocurre cuando el pipeline se ejecutó antes de que la normalización Flink estuviera
    # lista, escribiendo datos de sensors_raw en lugar de sensors_clean.
    objs = list(c.list_objects('datalake', prefix='clean/', recursive=True))
    if objs:
        sample_obj = objs[0]
        data = c.get_object('datalake', sample_obj.object_name)
        first_line = data.read(512).decode('utf-8', errors='ignore').splitlines()[0]
        data.close()
        try:
            fields = set(json.loads(first_line).keys())
        except Exception:
            fields = set()

        if 'temperature' in fields and 'temperature_c' not in fields:
            for o in objs:
                c.remove_object('datalake', o.object_name)
            print(f'    \033[33m⚠️  Schema incorrecto detectado (temperature sin _c) — {len(objs)} archivos purgados\033[0m')
            print(f'    \033[32m✅ MinIO listo para recibir datos con schema correcto (temperature_c)\033[0m')
        else:
            print(f'    \033[32m✅ {len(objs)} archivos en datalake con schema correcto\033[0m')
    else:
        print('    \033[32m✅ Bucket datalake vacío — listo\033[0m')

except Exception as e:
    print(f'    \033[33m⚠️  MinIO no disponible: {e}\033[0m', file=sys.stderr)
" 2>&1 || true

# (El servidor S3 es RustFS: no incluye el cliente `mc`, por lo que ya no se
#  configura ningún alias. El bucket se crea arriba con la librería Python `minio`.)

# ── 3. Jobs Flink ────────────────────────────────────────────
echo ""
echo ">>> [3/4] Lanzando jobs Flink..."
JM_ID="$(docker ps -qf 'label=com.docker.compose.service=jobmanager' 2>/dev/null | head -1 || true)"

if [[ -n "${JM_ID}" ]]; then
  # Obtener estado actual de jobs
  JOBS_JSON=$(docker exec "${JM_ID}" bash -c \
    "curl -s http://localhost:8081/jobs/overview 2>/dev/null || echo '{\"jobs\":[]}'")

  RUNNING_JOBS=$(echo "${JOBS_JSON}" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(len([j for j in d.get('jobs',[]) if j['status']=='RUNNING']))" \
    2>/dev/null || echo 0)

  FAILED_IDS=$(echo "${JOBS_JSON}" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(' '.join(j['jid'] for j in d.get('jobs',[]) if j['status']=='FAILED'))" \
    2>/dev/null || echo "")

  if [[ "${RUNNING_JOBS}" -eq 3 ]]; then
    echo -e "    ${GREEN}✅ Los 3 jobs ya están RUNNING — nada que hacer${NC}"
  else
    # Cancelar jobs fallidos
    if [[ -n "${FAILED_IDS}" ]]; then
      echo -e "    ${YELLOW}⚠️  Cancelando jobs fallidos...${NC}"
      for ID in ${FAILED_IDS}; do
        docker exec "${JM_ID}" bash -c \
          "curl -s -X PATCH 'http://localhost:8081/jobs/${ID}?mode=cancel' > /dev/null 2>&1" || true
      done
      sleep 2
    fi

    # Si quedan jobs corriendo (parcialmente activo), cancelar todo para evitar duplicados
    if [[ "${RUNNING_JOBS}" -gt 0 && "${RUNNING_JOBS}" -lt 3 ]]; then
      echo -e "    ${YELLOW}⚠️  Pipeline incompleto (${RUNNING_JOBS}/3 running) — reiniciando todos${NC}"
      ALL_IDS=$(echo "${JOBS_JSON}" | python3 -c \
        "import sys,json; d=json.load(sys.stdin); print(' '.join(j['jid'] for j in d.get('jobs',[]) if j['status']=='RUNNING'))" \
        2>/dev/null || echo "")
      for ID in ${ALL_IDS}; do
        docker exec "${JM_ID}" bash -c \
          "curl -s -X PATCH 'http://localhost:8081/jobs/${ID}?mode=cancel' > /dev/null 2>&1" || true
      done
      sleep 3
    fi

    # Lanzar los 3 jobs
    JOBS=(flink_normalization_job.py flink_hash_verifier_job.py flink_analytics_job.py)
    TOTAL=${#JOBS[@]}
    IDX=0
    for JOB in "${JOBS[@]}"; do
      IDX=$((IDX + 1))
      echo -n "    [${IDX}/${TOTAL}] Enviando ${JOB}..."
      docker exec "${JM_ID}" bash -c \
        "nohup flink run -py /opt/flink/jobs/${JOB} \
         > /tmp/flink_${JOB%.py}.log 2>&1 &" \
        && echo -e " ${GREEN}✅${NC}" \
        || echo -e " ${YELLOW}⚠️  (ver /tmp/flink_${JOB%.py}.log)${NC}"
      sleep 4
    done

    # Esperar a que los 3 jobs estén RUNNING antes de continuar
    echo -n "    Esperando RUNNING"
    MAX_WAIT=90
    WAITED=0
    while true; do
      RUNNING_COUNT=$(curl -s http://jobmanager:8081/jobs/overview 2>/dev/null \
        | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(len([j for j in d.get('jobs',[]) if j['status']=='RUNNING']))
" 2>/dev/null || echo 0)
      if [[ "${RUNNING_COUNT}" -ge 3 ]]; then
        echo -e " ${GREEN}✅ Los 3 jobs están RUNNING${NC}"
        break
      fi
      if [[ ${WAITED} -ge ${MAX_WAIT} ]]; then
        echo -e " ${YELLOW}⚠️  Timeout — verifica con: flink-jobs${NC}"
        break
      fi
      echo -n "."
      sleep 3
      WAITED=$((WAITED + 3))
    done
  fi
else
  echo -e "    ${YELLOW}⚠️  jobmanager no encontrado${NC}"
fi

# ── 4. MinIO writer (cold path) ──────────────────────────────
echo ""
echo ">>> [4/4] Arrancando minio-writer (cold path)..."
if pgrep -f "kafka_to_minio.py" > /dev/null 2>&1; then
  echo -e "    ${GREEN}✅ minio-writer ya está corriendo${NC}"
else
  nohup python /workspaces/programacion_IA_PAC_DES/src/03_storage/kafka_to_minio.py \
    > /tmp/minio_writer.log 2>&1 &
  MINIO_PID=$!
  sleep 3
  if kill -0 "${MINIO_PID}" 2>/dev/null; then
    echo -e "    ${GREEN}✅ minio-writer corriendo (PID ${MINIO_PID}) — log: /tmp/minio_writer.log${NC}"
  else
    echo -e "    ${RED}❌ minio-writer se cayó al arrancar — revisa: tail /tmp/minio_writer.log${NC}"
  fi
fi

# ── Aliases de desarrollo ────────────────────────────────────
BASHRC="/home/vscode/.bashrc"
MARKER="# === ILERNA PAC DES helpers ==="

# Eliminar bloque anterior si existe (para actualizar aliases al re-ejecutar)
if grep -q "${MARKER}" "${BASHRC}" 2>/dev/null; then
  sed -i "/^# === ILERNA PAC DES helpers ===/,/^# === fin ILERNA PAC DES helpers ===/d" "${BASHRC}"
fi
cat >> "${BASHRC}" << 'BASHRC_EOF'

# === ILERNA PAC DES helpers ===
jm()  { docker ps -qf "label=com.docker.compose.service=jobmanager"; }
rp()  { docker ps -qf "label=com.docker.compose.service=redpanda";   }

flink-run()  { docker exec "$(jm)" flink run "$@"; }
flink-list() { curl -s http://jobmanager:8081/jobs | python3 -m json.tool; }
flink-jobs() { curl -s http://jobmanager:8081/jobs/overview | python3 -c "import sys,json; [print(j['jid'][:8], j['state'], j['name'][:60]) for j in json.load(sys.stdin)['jobs'] if j['state'] not in ('CANCELED','FAILED','FINISHED')]"; }
flink-status() {
  echo "=== Jobs activos ==="
  curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys, json
jobs = json.load(sys.stdin)['jobs']
for j in jobs:
    print(f\"  {j['jid'][:8]}  {j['state']:<10}  {j['name'][:55]}\")
print(f'  Total: {len(jobs)} jobs')
"
  echo ""
  echo "=== TaskManagers ==="
  curl -s http://jobmanager:8081/taskmanagers | python3 -c "
import sys, json
tms = json.load(sys.stdin)['taskmanagers']
for t in tms:
    print(f\"  {t['id']}  slots={t['slotsNumber']}  free={t['freeSlots']}\")
print(f'  Total TMs: {len(tms)}')
"
}
flink-logs() {
  for JOB in flink_normalization_job flink_hash_verifier_job flink_analytics_job; do
    echo "=== ${JOB} ==="
    docker exec "$(jm)" bash -c "tail -20 /tmp/${JOB}.log 2>/dev/null || echo '  (sin log)'"
    echo ""
  done
}
tm-health() {
  TM_ID="$(docker ps -qf 'label=com.docker.compose.service=taskmanager' | head -1)"
  if [[ -n "${TM_ID}" ]]; then
    docker inspect "${TM_ID}" --format='  Restarts={{.State.RestartCount}}  OOM={{.State.OOMKilled}}  Status={{.State.Status}}'
    docker stats --no-stream --format '  CPU={{.CPUPerc}}  RAM={{.MemUsage}}' "${TM_ID}"
  else
    echo "  taskmanager no encontrado"
  fi
}
flink-restart() {
  local JM_ID
  JM_ID="$(jm)"
  echo ">>> Cancelando jobs existentes..."
  local ACTIVE_IDS
  ACTIVE_IDS=$(docker exec "${JM_ID}" bash -c "curl -s http://localhost:8081/jobs/overview" 2>/dev/null \
    | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(' '.join(j['jid'] for j in d.get('jobs',[]) if j['status'] not in ('CANCELED','FINISHED')))
" 2>/dev/null || echo "")
  if [[ -n "${ACTIVE_IDS}" ]]; then
    for ID in ${ACTIVE_IDS}; do
      docker exec "${JM_ID}" bash -c "curl -s -X PATCH 'http://localhost:8081/jobs/${ID}?mode=cancel' > /dev/null"
      echo "  Cancelado: ${ID:0:8}"
    done
    sleep 5
  else
    echo "  No hay jobs activos"
  fi
  echo ">>> Relanzando jobs Flink..."
  for JOB in flink_normalization_job flink_hash_verifier_job flink_analytics_job; do
    echo "  Lanzando ${JOB}..."
    docker exec "${JM_ID}" bash -c "nohup flink run -py /opt/flink/jobs/${JOB}.py > /tmp/${JOB}.log 2>&1 &"
    sleep 4
  done
  echo ">>> Estado:"
  flink-jobs
}

alias sim='python src/01_ingestion/sensor_simulator.py --machines 5 --fault-rate 0.1'
alias bridge='python src/01_ingestion/mqtt_to_redpanda_bridge.py'
alias minio-writer='python src/03_storage/kafka_to_minio.py'
alias minio-status='pgrep -fa kafka_to_minio && tail -20 /tmp/minio_writer.log || echo "minio-writer NO está corriendo. Lanza: minio-writer &"'
alias minio-log='tail -f /tmp/minio_writer.log'
alias api='uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload'
alias ui='streamlit run src/05_ui/app.py --server.port 8501'
alias nb='jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token="" --ServerApp.password="" --NotebookApp.token="" --NotebookApp.password=""'
resources() {
  echo "=== Host (Codespaces) ==="
  echo "CPU:  $(nproc) cores  |  Load: $(cut -d' ' -f1-3 /proc/loadavg)"
  echo "RAM:  $(free -h | awk '/^Mem/{print $3 " used / " $2 " total"}')"
  echo "Disk: $(df -h /workspaces | awk 'NR==2{print $3 " used / " $2 " total  (" $5 ")"}')"
  echo ""
  echo "=== Contenedores Docker ==="
  docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" 2>/dev/null | \
    sed 's/programacion_ia_pac_des_devcontainer-//g' | sed 's/-1//g'
}
alias mqtt-install='sudo apt-get update -qq && sudo apt-get install -y mosquitto-clients'
alias mqtt-sub='mosquitto_sub -h mosquitto -p 1883 -t "sensors/telemetry" -v'
alias verify='bash /workspaces/programacion_IA_PAC_DES/tests/verify_pipeline.sh'
alias verify-db='python /workspaces/programacion_IA_PAC_DES/tests/test_databases.py'
alias aliases='echo "
  sim            → Simulador de sensores (5 máquinas, fault-rate 0.1)
  bridge         → Puente MQTT → Redpanda
  minio-writer   → Cold path: kafka_to_minio.py (lanzar en background: minio-writer &)
  minio-status   → Verifica si minio-writer está corriendo + últimas líneas de log
  minio-log      → Sigue el log de minio-writer en tiempo real
  api            → FastAPI en :8000
  ui             → Streamlit dashboard en :8501
  nb             → JupyterLab en :8888
  flink-run      → flink run dentro del jobmanager
  flink-list     → Lista jobs Flink (JSON raw)
  flink-jobs     → Lista jobs activos con nombre y estado
  flink-status   → Resumen de jobs + TaskManagers y slots
  flink-logs     → Últimas 20 líneas de cada log de job
  flink-restart  → Relanza los 3 jobs Flink
  tm-health      → Restarts, OOM y recursos del TaskManager
  resources      → CPU, RAM, disco y stats de contenedores
  verify         → Verificación completa del pipeline
  mqtt-sub       → Suscribe a sensors/telemetry
  aliases        → Muestra esta ayuda
"'
# === fin ILERNA PAC DES helpers ===
BASHRC_EOF
echo -e "    ${GREEN}✅ Aliases de desarrollo añadidos a ~/.bashrc${NC}"

# Cargar aliases en el shell actual (funciona si el script se ejecuta con 'source')
# shellcheck disable=SC1090
source "${BASHRC}" 2>/dev/null || true

# ── Banner final ─────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo -e "║  ${GREEN}✅ Pipeline listo${NC}                                   ║"
echo "╠══════════════════════════════════════════════════════╣"
echo -e "║  ${RED}🔴${NC} Redpanda Console  → http://localhost:18080       ║"
echo -e "║  ${YELLOW}🟠${NC} Flink UI          → http://localhost:18081       ║"
echo -e "║  ${YELLOW}🟡${NC} InfluxDB UI       → http://localhost:18086       ║"
echo -e "║  ${GREEN}🟢${NC} MinIO Console     → http://localhost:19001       ║"
echo -e "║  ${CYAN}🔵${NC} Grafana           → http://localhost:13000       ║"
echo    "║  ⚪ FastAPI           → http://localhost:18000       ║"
echo    "║  ⚪ Streamlit         → http://localhost:18501       ║"
echo    "║  ⚪ Jupyter           → http://localhost:18888       ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "    Aliases disponibles: sim | bridge | minio-writer | minio-status | minio-log | api | ui | nb | flink-run | flink-list | flink-jobs | flink-restart | mqtt-sub | verify | aliases"
echo ""
