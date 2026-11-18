# Guía de Debug del Pipeline

Comandos y procedimientos para diagnosticar el estado del pipeline en GitHub Codespaces.

---

## Scripts de verificación

```bash
# Verificación rápida del pipeline completo
bash tests/verify_pipeline.sh

# Verificación detallada de bases de datos + Grafana
python tests/test_databases.py

# Test de integración end-to-end (conectividad de servicios)
python tests/test_flow.py
```

---

## 1. Flink Jobs

### Ver jobs activos (resumen)
```bash
flink-jobs
```
Debe mostrar exactamente **3 jobs RUNNING**:
| Job | Descripción |
|-----|-------------|
| `insert-into...sensors_clean,sensors_invalid` | flink_normalization_job |
| `sensor-analytics-tumble-1min` | flink_analytics_job |
| `sensor-hash-chain-verifier` | flink_hash_verifier_job |

> El cold path (RustFS) lo gestiona **`kafka_to_minio.py`** (proceso Python), no Flink.

### Ver todos los jobs (incluye FAILED/CANCELED)
```bash
flink-list
```

### Ver jobs con nombre y estado (filtra terminados)
```bash
curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys,json
for j in json.load(sys.stdin)['jobs']:
    print(j['jid'][:8], j['state'], j['name'][:60])
"
```

### Ver excepciones de un job
```bash
curl -s http://jobmanager:8081/jobs/<JOB_ID>/exceptions | python3 -m json.tool
```

### Ver logs de lanzamiento de un job
```bash
docker exec $(jm) cat /tmp/flink_analytics_job.log
docker exec $(jm) cat /tmp/flink_normalization_job.log
docker exec $(jm) cat /tmp/flink_hash_verifier_job.log
```

### Lanzar un job directamente (modo interactivo, muestra errores)
```bash
docker exec $(jm) flink run -py /opt/flink/jobs/flink_normalization_job.py
```

### Relanzar todos los jobs
```bash
flink-restart
```

### Cancelar un job por ID
```bash
curl -s -X PATCH 'http://jobmanager:8081/jobs/<JOB_ID>?mode=cancel'
```

### Ver logs del JobManager
```bash
docker logs $(jm) --tail 100
```

### Ver logs del TaskManager (errores de ejecución Python)
```bash
docker logs $(docker ps -qf 'label=com.docker.compose.service=taskmanager') 2>&1 | grep -i "error\|exception\|ALERTA\|machine-" | tail -30
```

> **Nota**: Los contadores "Records Received: 0" en la UI de Flink son un bug de métricas de PyFlink con el runner Beam — no reflejan la realidad. Verifica el flujo real leyendo los topics.

---

## 2. Redpanda (Kafka)

### Contar mensajes por topic
```bash
for TOPIC in sensors_raw sensors_clean sensors_invalid sensors_verified; do
  COUNT=$(docker exec $(rp) rpk topic describe $TOPIC --print-partitions \
    | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print sum+0}')
  echo "$TOPIC: $COUNT mensajes"
done
```

### Consumir mensajes de un topic (últimos N)
```bash
docker exec $(rp) rpk topic consume sensors_clean --num 3
```

### Consumir desde el offset más reciente (datos en tiempo real)
```bash
docker exec $(rp) rpk topic consume sensors_clean --offset end --num 5
```

### Ver offsets (watermarks) de todos los topics
```bash
docker exec $(rp) rpk topic describe sensors_raw --print-partitions
docker exec $(rp) rpk topic describe sensors_clean --print-partitions
```

---

## 3. InfluxDB

### Verificar datos recientes (últimos 30 min)
```bash
curl -s -X POST 'http://influxdb:8086/api/v2/query?org=ilerna' \
  -H 'Authorization: Token supersecrettoken' \
  -H 'Content-Type: application/vnd.flux' \
  --data-raw 'from(bucket:"sensores") |> range(start: -30m) |> filter(fn:(r) => r._measurement == "machine_stats") |> last()' \
  | grep -E "device_id|_value|_time" | head -20
```

### Si no hay datos en 30 min, ampliar a 3 horas
```bash
curl -s -X POST 'http://influxdb:8086/api/v2/query?org=ilerna' \
  -H 'Authorization: Token supersecrettoken' \
  -H 'Content-Type: application/vnd.flux' \
  --data-raw 'from(bucket:"sensores") |> range(start: -3h) |> filter(fn:(r) => r._measurement == "machine_stats") |> group() |> last() |> keep(columns: ["_time"])' \
  | grep _time
```

> Si los datos están en las últimas 3h pero no en los últimos 30 min, los jobs Flink estuvieron caídos y acaban de reiniciarse. Hay que esperar ~1 min a que la ventana Tumble cierre.

### Contar registros por máquina
```bash
curl -s -X POST 'http://influxdb:8086/api/v2/query?org=ilerna' \
  -H 'Authorization: Token supersecrettoken' \
  -H 'Content-Type: application/vnd.flux' \
  --data-raw 'from(bucket:"sensores") |> range(start: -1h) |> filter(fn:(r) => r._measurement == "machine_stats") |> count()' \
  | grep _value | head -10
```

---

## 4. RustFS (Cold Path)

### Ver archivos en el datalake
```bash
python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)
objs = list(c.list_objects('datalake', recursive=True))
print(f'Total: {len(objs)} objetos')
for o in objs[:10]:
    print(f'  {o.object_name}  ({(o.size or 0)/1024:.1f} KB)')
"
```

### RustFS writer (cold path)

El cold path usa `kafka_to_minio.py` — arranca automáticamente con `init_pipeline.sh`.
Si no está corriendo:

```bash
pgrep -f kafka_to_minio || rustfs-writer &
cat /tmp/minio_writer.log   # ver log
```

Escribe un archivo JSON cada 60 segundos en:
`s3://datalake/clean/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>.json`

---

## 5. Grafana

### Verificar datasource InfluxDB
```bash
curl -s 'http://admin:Ilerna_Programaci0n@grafana:3000/api/datasources/uid/influxdb/health'
# Debe devolver: {"status":"OK","message":"datasource is working. 3 buckets found"}
```

### Verificar que Grafana puede ejecutar queries
```bash
curl -s -X POST 'http://admin:Ilerna_Programaci0n@grafana:3000/api/ds/query' \
  -H 'Content-Type: application/json' \
  -d '{
    "queries": [{
      "refId": "A",
      "datasource": {"uid": "influxdb"},
      "query": "from(bucket:\"sensores\") |> range(start: -30m) |> filter(fn:(r) => r._measurement == \"machine_stats\") |> last()"
    }],
    "from": "now-30m",
    "to": "now"
  }' | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['results']['A'].get('status'), len(d['results']['A'].get('frames',[])))"
# Debe devolver: 200 <N_frames>
```

### Ver logs de errores de Grafana
```bash
docker logs $(docker ps -qf 'label=com.docker.compose.service=grafana') 2>&1 | grep -i "error\|400\|influx" | tail -20
```

### El dashboard muestra "No data" aunque InfluxDB tenga datos

Causas más comunes:
1. **Datos fuera del rango de tiempo** — El dashboard *Telemetría IoT* usa *Last 30 minutes*. Si el analytics job procesó un backlog antiguo, amplía el rango.
2. **Analytics job recién reiniciado** — Espera 2-3 min: cada punto es una ventana Tumble de 1 min que solo se escribe al cerrar (watermark de 10 s).
3. **Jobs Flink caídos** — Verificar con `flink-jobs` y relanzar con `flink-restart`.

### Ver el error real de cada panel

```bash
python tests/check_grafana_panels.py      # ejecuta cada panel por /api/ds/query: error o nº de filas
python tests/check_grafana_endpoints.py   # endpoints y campos que consumen los dashboards
```

### Errores 404 en paneles del dashboard Redpanda/Kafka

El plugin Infinity hace peticiones REST desde el contenedor de Grafana. Un 404 significa que la URL no existe en esta versión de Redpanda. Comprobado en Redpanda v25.3: la Admin API (`:9644`) no da el número de mensajes por topic y Pandaproxy (`:8082`) devuelve 404 en `/topics/<t>/partitions/0/offsets`. Por eso los paneles de mensajes usan la API REST de **Redpanda Console**: `http://redpanda-console:8080/api/topics/<t>/partitions` → `waterMarkHigh`.

### Un panel Infinity da error tras cambiar la URL a otro host

El host debe estar en `allowedHosts` del datasource Infinity (`config/grafana/provisioning/datasources/datasources.yml`). Los datasources **solo se leen al arrancar Grafana**:

```bash
docker restart $(docker ps -qf label=com.docker.compose.service=grafana)
```

---

## 6. FastAPI

### Health check completo
```bash
curl -s http://localhost:8000/health | python3 -m json.tool
```

### Estado de máquinas
```bash
curl -s http://localhost:8000/machines/status | python3 -m json.tool
```

### Alertas activas
```bash
curl -s http://localhost:8000/alerts | python3 -m json.tool
```

> Si `/machines/status` devuelve `{"detail":"'_time'"}`, es un KeyError en el mapeo de campos de InfluxDB. Verificar que el analytics job esté escribiendo correctamente.

---

## 7. Flujo completo de diagnóstico

Seguir este orden cuando el pipeline no funciona:

```
1. ¿Están corriendo los 3 Flink jobs?
   → flink-jobs
   → Si no: flink-restart

2. ¿Llegan datos a Redpanda?
   → docker exec $(rp) rpk topic consume sensors_raw --num 3
   → Si no: sim & ; bridge &

3. ¿Flink normaliza? ¿sensors_clean tiene datos?
   → docker exec $(rp) rpk topic describe sensors_clean --print-partitions
   → Si no: ver logs del TaskManager

4. ¿InfluxDB tiene datos recientes?
   → curl ... range(start: -30m) ... machine_stats
   → Si no: esperar 1 min (ventana Tumble) o ver logs TaskManager

5. ¿Grafana muestra datos?
   → Verificar health del datasource
   → Ejecutar query directa via /api/ds/query
   → python tests/check_grafana_panels.py (error real de cada panel)
   → Si query OK pero dashboard "No data": cambiar rango de tiempo

6. ¿RustFS tiene archivos?
   → python tests/list_datalake.py   (o: s3 ls s3://datalake/ --recursive)
   → Si no después de 2 min: rustfs-status ; rustfs-writer &
```

---

## 8. Comandos útiles de un vistazo

| Alias/Comando | Descripción |
|---|---|
| `flink-jobs` | Lista jobs RUNNING con nombre |
| `flink-list` | Lista jobs (JSON raw, todos los estados) |
| `flink-restart` | Relanza los 3 jobs Flink |
| `sim` | Simulador de sensores (5 máquinas) |
| `bridge` | Puente MQTT → Redpanda |
| `rustfs-writer` | Writer Python directo sensors_clean → RustFS |
| `rustfs-status` / `rustfs-log` | Estado y log del writer del cold path |
| `s3` | AWS CLI contra RustFS (`s3 ls s3://datalake/ --recursive`) |
| `s3web` | Cliente web S3 en :19002 (`s3web start\|stop\|status`) |
| `dps` / `dc` | Estado de contenedores / `docker compose` del proyecto de Codespaces (`dc logs grafana`) |
| `python tests/list_datalake.py` | Contenido del bucket sin consola web |
| `python tests/check_grafana_panels.py` | Error real de cada panel de Grafana |
| `verify` | Verificación rápida del pipeline |
| `verify-db` | Verificación detallada de bases de datos y Grafana |
| `resources` | CPU, RAM, disco y stats de contenedores |
| `api` | Lanza FastAPI en :8000 |
| `ui` | Lanza Streamlit en :8501 |
