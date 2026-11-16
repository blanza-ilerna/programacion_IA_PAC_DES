#!/usr/bin/env bash
# verify_pipeline.sh — Verificación rápida del estado del pipeline
# Uso: bash tests/verify_pipeline.sh

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
CYAN="\033[0;36m"
NC="\033[0m"

PASS=0
FAIL=0
WARN=0

ok()   { PASS=$((PASS+1));  echo -e "  ${GREEN}✅ $1${NC}${2:+ — $2}"; }
fail() { FAIL=$((FAIL+1));  echo -e "  ${RED}❌ $1${NC}${2:+ — $2}"; }
warn() { WARN=$((WARN+1));  echo -e "  ${YELLOW}⚠️  $1${NC}${2:+ — $2}"; }
section() { echo -e "\n${CYAN}── $1 $(printf '─%.0s' $(seq 1 $((48-${#1}))))${NC}"; }

# ── Resolver IDs de contenedores ──────────────────────────────
RP=$(docker ps -qf "label=com.docker.compose.service=redpanda"   2>/dev/null | head -1)
JM=$(docker ps -qf "label=com.docker.compose.service=jobmanager" 2>/dev/null | head -1)

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║     ILERNA PAC DES - Verificación del Pipeline       ║"
echo "╚══════════════════════════════════════════════════════╝"

# ── 1. Flink Jobs ─────────────────────────────────────────────
section "Flink Jobs"
JOBS_JSON=$(curl -s --max-time 5 http://jobmanager:8081/jobs 2>/dev/null)
if [[ -z "$JOBS_JSON" ]]; then
    fail "Flink API" "jobmanager no responde en :8081"
else
    RUNNING=$(echo "$JOBS_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
jobs = [j for j in d.get('jobs', []) if j['status'] == 'RUNNING']
for j in jobs:
    print(j['id'][:12] + '...')
print(f'TOTAL:{len(jobs)}')
" 2>/dev/null)
    COUNT=$(echo "$RUNNING" | grep "TOTAL:" | cut -d: -f2)
    if [[ "${COUNT:-0}" -ge 2 ]]; then
        ok "Jobs RUNNING" "$COUNT job(s)"
        echo "$RUNNING" | grep -v "TOTAL:" | while read id; do echo "    • $id"; done
    elif [[ "${COUNT:-0}" -eq 1 ]]; then
        warn "Jobs RUNNING" "solo 1 job (faltan normalization + analytics)"
    else
        fail "Jobs RUNNING" "0 jobs activos — lanza los jobs Flink"
    fi
fi

# ── 2. Redpanda Topics ────────────────────────────────────────
section "Redpanda — Mensajes por tópico"
if [[ -z "$RP" ]]; then
    fail "Redpanda" "contenedor no encontrado"
else
    for TOPIC in sensors_raw sensors_clean sensors_invalid sensors_verified; do
        # rpk topic describe devuelve la info de cada partición
        COUNT=$(docker exec "$RP" rpk topic describe "$TOPIC" --print-partitions 2>/dev/null \
            | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print sum+0}')
        if [[ "$COUNT" -gt 0 ]]; then
            ok "Topic $TOPIC" "$COUNT mensajes"
        else
            warn "Topic $TOPIC" "0 mensajes (¿pipeline corriendo?)"
        fi
    done
fi

# ── 3. Hash Chain ─────────────────────────────────────────────
section "Hash Chain (sensors_verified / sensors_invalid)"
if [[ -n "$RP" ]]; then
    # sensors_verified — contar mensajes por watermark
    VER_COUNT=$(docker exec "$RP" rpk topic describe sensors_verified --print-partitions 2>/dev/null \
        | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print sum+0}')
    if [[ "${VER_COUNT:-0}" -gt 0 ]]; then
        # Consumir un mensaje para mostrar el hash
        VERIFIED=$(timeout 5 docker exec "$RP" rpk topic consume sensors_verified -n 1 --offset start 2>/dev/null \
            | python3 -c "
import sys, json
for line in sys.stdin:
    try:
        outer = json.loads(line.strip())
        msg = json.loads(outer.get('value','{}'))
        h = msg.get('hash','')
        p = msg.get('prev_hash','')
        if h:
            print(f'hash={h[:16]}... prev={p[:16]}...')
    except: pass
" 2>/dev/null)
        ok "sensors_verified" "$VER_COUNT mensajes — $(echo "$VERIFIED" | head -1)"
    else
        warn "sensors_verified" "0 mensajes — si sim arrancó antes que el verifier, reinicia sim para resetear la cadena"
    fi

    # sensors_invalid — razones de rechazo (watermark + muestra)
    INV_COUNT=$(docker exec "$RP" rpk topic describe sensors_invalid --print-partitions 2>/dev/null \
        | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print sum+0}')
    if [[ "${INV_COUNT:-0}" -gt 0 ]]; then
        INVALID=$(timeout 5 docker exec "$RP" rpk topic consume sensors_invalid -n 5 --offset start 2>/dev/null \
            | python3 -c "
import sys, json, collections
reasons = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        outer = json.loads(line)
        val = outer.get('value')
        # val puede ser: str JSON-encoded, dict ya parseado, None o ''
        if isinstance(val, dict):
            msg = val
        elif isinstance(val, str) and val:
            try:
                msg = json.loads(val)
            except Exception:
                msg = {}
        else:
            msg = {}
        r = msg.get('reason') or msg.get('error') or 'unknown'
        reasons.append(r)
    except Exception:
        pass
if reasons:
    c = collections.Counter(reasons)
    print(', '.join(f'{r}:{n}' for r,n in c.most_common()))
else:
    print('(razones no disponibles — ver rpk topic consume sensors_invalid -n 1)')
" 2>/dev/null)
        ok "sensors_invalid (DLQ)" "$INV_COUNT mensajes — ${INVALID:-razones no disponibles}"
    else
        warn "sensors_invalid" "0 mensajes (¿hash verifier corriendo? flink-jobs)"
    fi
fi

# ── 4. InfluxDB ───────────────────────────────────────────────
section "InfluxDB — machine_stats"
INFLUX_CSV=$(curl -s --max-time 5 \
    -H "Authorization: Token supersecrettoken" \
    -H "Accept: application/csv" \
    -H "Content-Type: application/vnd.flux" \
    --data 'from(bucket:"sensores") |> range(start:-15m) |> filter(fn:(r)=>r._measurement=="machine_stats") |> last() |> keep(columns:["device_id","_field","_value"])' \
    "http://influxdb:8086/api/v2/query?org=ilerna" 2>/dev/null)

if [[ -z "$INFLUX_CSV" ]]; then
    fail "InfluxDB query" "sin respuesta"
else
    ROWS=$(echo "$INFLUX_CSV" | grep -c "^,result" 2>/dev/null || echo 0)
    if [[ "$ROWS" -gt 0 ]]; then
        ok "machine_stats" "$ROWS campo(s) en los últimos 15 min"
        # Mostrar valores
        echo "$INFLUX_CSV" | python3 -c "
import sys, csv
reader = csv.DictReader(sys.stdin)
seen = set()
for row in reader:
    dev = row.get('device_id','')
    field = row.get('_field','')
    val = row.get('_value','')
    key = f'{dev}:{field}'
    if key not in seen and dev:
        seen.add(key)
        print(f'    • {dev} {field}={val}')
" 2>/dev/null | head -15
    else
        warn "machine_stats" "sin datos en los últimos 15 min (ventana Tumble aún no cerró?)"
    fi
fi

# ── 5. RustFS — cold path ──────────────────────────────────────
section "RustFS — cold path (JSON)"
FILE_COUNT=$(python3 -c "
from minio import Minio
try:
    c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)
    objs = list(c.list_objects('datalake', recursive=True))
    files = [o for o in objs if o.object_name.endswith('.json') or o.object_name.endswith('.parquet')]
    print(len(files))
except Exception as e:
    print(f'ERROR:{e}')
" 2>/dev/null)

if echo "$FILE_COUNT" | grep -q "ERROR"; then
    fail "RustFS datalake" "$FILE_COUNT"
elif [[ "${FILE_COUNT:-0}" -gt 0 ]]; then
    ok "Archivos en RustFS" "$FILE_COUNT archivo(s)"
else
    warn "RustFS datalake" "0 archivos (kafka_to_minio.py escribe cada 60s — ¿está corriendo? pgrep -f kafka_to_minio)"
fi

# ── 6. FastAPI ────────────────────────────────────────────────
section "FastAPI"
API_HEALTH=$(curl -s --max-time 3 http://localhost:8000/health 2>/dev/null)
if echo "$API_HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('status')=='ok' else 1)" 2>/dev/null; then
    INFLUX_STATUS=$(echo "$API_HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('influxdb','?'))" 2>/dev/null)
    ok "FastAPI /health" "influxdb=$INFLUX_STATUS"

    MACHINES=$(curl -s --max-time 3 http://localhost:8000/machines/status 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null)
    if [[ "${MACHINES:-0}" -gt 0 ]]; then
        ok "FastAPI /machines/status" "$MACHINES máquina(s) con datos"
    else
        warn "FastAPI /machines/status" "0 máquinas (sin datos en InfluxDB aún)"
    fi
else
    warn "FastAPI" "no disponible en :8000 — ejecuta: api"
fi

# ── Resumen ───────────────────────────────────────────────────
TOTAL=$((PASS+FAIL+WARN))
echo ""
echo "──────────────────────────────────────────────────────"
echo -e "  ${GREEN}✅ OK: $PASS${NC}  ${YELLOW}⚠️  WARN: $WARN${NC}  ${RED}❌ FAIL: $FAIL${NC}  (total: $TOTAL checks)"
echo ""
exit "$FAIL"
