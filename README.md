# ILERNA Smart-Industry — Programación IA PAC DES

Sistema de telemetría IoT con procesamiento en tiempo real para monitorización de temperatura de máquinas industriales.

## Arquitectura del pipeline

```
[sensor_simulator.py]
        │  MQTT (sensors/telemetry)  QoS 1
        ▼
[Mosquitto]  :11883
        │
        ▼
[mqtt_to_redpanda_bridge.py]              ← Hito 1
  · Valida schema (device_id, temperature, unit, ts)
  · Valida unit ∈ {C, F, K}
  · Enriquece con _mqtt_topic, _ingested_at
  · Publica con enable.idempotence=True, key=device_id
        │  Kafka  sensors_raw
        ▼
[Redpanda]  :19092
        │
        ├──▶ [flink_hash_verifier_job.py]      ← Seguridad
        │      · DataStream API + KeyedProcessFunction
        │      · Verifica SHA256 hash-chain por device_id
        │      · OK  → sensors_verified
        │      · KO  → sensors_invalid (DLQ)
        │      ⚠️  Debe estar RUNNING antes de que arranque el simulador
        │
        └──▶ [flink_normalization_job.py]      ← Hito 2
               · Table API + UDF ScalarFunction to_celsius
               · F→C: (F-32)×5/9  |  K→C: K-273.15
               · StatementSet: válidos → sensors_clean, inválidos → sensors_invalid
                       │  Kafka  sensors_clean
                       │  {device_id, temperature_c, unit_original, ts, _ingested_at}
                       ▼
              [flink_analytics_job.py]         ← Hito 3
               · Table API + Tumble Window 1 min / device_id
               · Watermark 10s (tolera mensajes desordenados)
               · Calcula: avg_temp_c, max_temp_c, count, alert (avg>80°C)
               · Escribe en InfluxDB vía Line Protocol (urllib, no SDK)
                       │
                       ├──▶ [InfluxDB]  :18086  bucket=sensores
                       │     measurement: machine_stats
                       │     tags: device_id
                       │     fields: avg_temp_c, max_temp_c, count, alert
                       │         │
                       │         ├──▶ [FastAPI]    :18000        ← Hito 4
                       │         │     GET  /machines/status
                       │         │     GET  /machines/{id}
                       │         │     GET  /alerts
                       │         │     POST /model/train
                       │         │     GET  /machines/{id}/predict
                       │         │     GET  /stats
                       │         │
                       │         └──▶ [Streamlit]  :18501        ← Hito 4
                       │               Tiempo Real · Historial
                       │               Alertas · Lambda Query
                       │               IA Anomalías (IsolationForest)
                       │
                       └──▶ [kafka_to_minio.py]   ← Cold path (Python, no Flink)
                              · Consume sensors_clean (confluent_kafka)
                              · Ventana 60s → NDJSON particionado Hive
                              · s3://datalake/clean/year=.../month=.../day=.../hour=.../
                              · Arranca automáticamente con init_pipeline.sh
                              · DuckDB lee con read_json + hive_partitioning=true
                                      │
                                      ▼
                              [S3: RustFS]  :19000/:19001  bucket=datalake
```

## Servicios Docker

| Servicio         | Puerto externo | Descripción                    | Credenciales                |
|------------------|----------------|--------------------------------|-----------------------------|
| Mosquitto        | 11883          | Broker MQTT                    | anónimo                     |
| Grafana          | 13000          | Dashboards                     | sin login (acceso anónimo)  |
| FastAPI          | 18000          | API REST sensores + modelo IA  | —                           |
| Redpanda Console | 18080          | UI de tópicos y mensajes       | —                           |
| Flink JobManager | 18081          | Cluster Flink (REST + UI)      | —                           |
| InfluxDB         | 18086          | Base de datos de series temp.  | admin / Ilerna_Programaci0n |
| Streamlit        | 18501          | Dashboard interactivo          | —                           |
| JupyterLab       | 18888          | Notebooks de análisis          | —                           |
| RustFS (S3)      | 19000 / 19001  | S3 API / Consola web (host `minio`) | admin / Ilerna_Programaci0n |
| S3 Web (s3manager) | 19002        | Navegador del bucket, solo lectura (lo lanza `init_pipeline.sh`) | — |
| Redpanda         | 19092          | Kafka-compatible broker        | —                           |

> **Nota sobre el almacenamiento S3.** MinIO dejó de publicar imágenes oficiales (repositorio
> archivado en abril de 2026), así que el cold path usa **RustFS** (`rustfs/rustfs:1.0.6`), compatible
> con la API S3. Para no tocar el código, **se conservan estos nombres**: el servicio/hostname Docker
> `minio` (`MINIO_ENDPOINT=minio:9000`), las variables `MINIO_*`, el cliente Python `minio` (habla S3,
> sirve con cualquier servidor compatible), el script `kafka_to_minio.py`, su log `/tmp/minio_writer.log`
> y su consumer group de Kafka `py-minio-writer`. Los alias de desarrollo sí se llaman `rustfs-*`
> (`rustfs-writer`, `rustfs-status`, `rustfs-log`).
> Comprobar que responde: `python -c "import urllib.request;print(urllib.request.urlopen('http://minio:9000/health').status)"`.
> La imagen no incluye el cliente `mc`.
>
> **Consola web (:19001).** Exige login (`admin` / `Ilerna_Programaci0n`; no existe modo anónimo). En
> Codespaces el login puede fallar con `SignatureDoesNotMatch` por el proxy de puertos. Alternativas
> (las prepara `init_pipeline.sh`; si alguna falla solo avisa, no bloquea nada):
>
> | Herramienta | Cómo | Para qué |
> |---|---|---|
> | **Cliente web S3** (s3manager) | puerto **19002** en la pestaña Ports · `s3web start\|stop\|status` | Navegar buckets y descargar archivos. **Solo lectura**. Firma desde el servidor, así que no le afecta el proxy |
> | **AWS CLI** (perfil `rustfs`) | `s3 ls s3://datalake/ --recursive --human-readable` · `s3 cp s3://datalake/<clave> -` | Terminal: listar, ver contenido, descargar |
> | **Script Python** | `python tests/list_datalake.py [--preview]` | Resumen de objetos, tamaños y particiones por hora |
>
> El AWS CLI se instala como binario en `~/.local` (no toca los paquetes Python del proyecto) mediante
> `.devcontainer/setup_s3_cli.sh`, y s3manager se lanza con `docker run` desde `.devcontainer/s3web.sh`
> (no está en el compose: si su imagen fallara, el Codespace seguiría arrancando).

## Estructura del proyecto

```
.
├── .devcontainer/
│   ├── docker-compose.yml          # Todos los servicios Docker
│   ├── Dockerfile                  # Imagen del workspace (Python, dependencias)
│   ├── Dockerfile.jobmanager       # Imagen Flink con JARs Kafka/InfluxDB y PyFlink
│   ├── devcontainer.json           # Configuración GitHub Codespaces
│   ├── start.sh                    # postStartCommand: levanta Docker y espera healthy
│   ├── init_pipeline.sh            # MANUAL: topics, bucket RustFS, jobs Flink, herramientas S3, aliases
│   ├── setup_s3_cli.sh             # AWS CLI v2 + perfil "rustfs" + función `s3` (no bloqueante)
│   └── s3web.sh                    # Cliente web S3 s3manager en :19002 (docker run, no bloqueante)
│
├── src/
│   ├── 01_ingestion/
│   │   ├── sensor_simulator.py         # Simula 5 máquinas (C/F/K, fault_rate, hash-chain)
│   │   └── mqtt_to_redpanda_bridge.py  # Hito 1: MQTT → Redpanda, validación + idempotencia
│   ├── processing/
│   │   ├── hash_chain.py               # Lógica SHA256 hash-chain compartida
│   │   ├── transformations.py          # UDF to_celsius (F/K → °C)
│   │   └── validators.py               # Validación de schema de mensajes
│   ├── 03_storage/
│   │   └── kafka_to_minio.py           # Cold path: sensors_clean → RustFS NDJSON (60s ventana)
│   ├── api/
│   │   ├── main.py                     # Hito 4: FastAPI REST (/machines, /alerts, /stats)
│   │   └── anomaly_model.py            # IsolationForest: train + predict
│   └── 05_ui/                          # Hito 4: Streamlit + DuckDB Lambda Query hot+cold
│       ├── app.py                      # Punto de entrada: página, sidebar y reparto de pestañas
│       ├── settings.py                 # Configuración (variables de entorno)
│       ├── datasources.py              # Acceso a datos: InfluxDB, RustFS/DuckDB, Flink, Kafka
│       └── views/                      # Una pestaña por módulo (cada una expone render())
│           ├── realtime.py  history.py  alerts.py  lambda_query.py  ai_anomalies.py
│           └── pipeline_health.py  hash_security.py  advanced_models.py  ayuda.py
│
├── jobs/
│   ├── flink_normalization_job.py      # Hito 2: Table API + UDF + StatementSet (2 sinks)
│   ├── flink_analytics_job.py          # Hito 3: Tumble Window 1min + alertas → InfluxDB
│   └── flink_hash_verifier_job.py      # Seguridad: DataStream API + hash-chain SHA256
│
├── notebooks/
│   ├── 01_exploracion_datos.ipynb      # Hot path, cold path, Lambda Query, IsolationForest
│   ├── 02_analisis_calidad.ipynb       # DLQ, hash-chain, completitud, distribución unidades
│   └── 03_bases_datos_calculos.ipynb   # Stats avanzadas: Z-score, tendencia, correlación
│
├── config/
│   ├── mosquitto.conf                  # Broker MQTT: puerto 1883, acceso anónimo
│   ├── requirements.txt                # Dependencias Python del workspace
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── datasources.yml     # InfluxDB (Flux) + Infinity (HTTP genérico)
│           └── dashboards/
│               ├── provider.yml
│               ├── telemetria.json     # Dashboard Telemetría IoT (InfluxDB: temperatura, alertas, throughput)
│               └── redpanda.json       # Dashboard Redpanda/Kafka (brokers, salud, mensajes por topic)
│
├── tests/
│   ├── test_connectivity.sh            # Smoke test: ¿responden todos los servicios?
│   ├── test_flow.py                    # Test E2E: publica mensaje y verifica en cada capa
│   ├── test_processing.py              # Tests unitarios (pytest): transformations, validators, hash_chain
│   ├── test_databases.py               # Verifica InfluxDB, RustFS y DuckDB query
│   ├── verify_pipeline.sh              # Diagnóstico completo del pipeline en ejecución
│   ├── check_grafana_endpoints.py      # Endpoints/campos que usan los dashboards de Grafana y si responden
│   ├── check_grafana_panels.py         # Ejecuta cada panel de Grafana por /api/ds/query y muestra su error real
│   └── list_datalake.py                # Lista el bucket S3 (objetos, tamaños, particiones) sin consola web
│
└── docs/
    ├── 00_smoke_test_checklist.md      # Checklist de validación paso a paso
    ├── 01_practica.md                  # Guía de la práctica PAC DES
    ├── 02_ficha_tecnica.md             # Ficha técnica del proyecto
    ├── 03_debug_guide.md               # Guía de depuración
    ├── 04_preguntas_teoria.md          # Preguntas y respuestas de teoría (examen)
    ├── 05_ejercicios_practicos.md      # Ejercicios prácticos
    └── 05_grafana_endpoints.md         # Endpoints Redpanda/Flink/FastAPI para Grafana
```

## Inicio rápido

### 1. Abrir en GitHub Codespaces

El entorno arranca automáticamente. `start.sh` levanta todos los contenedores Docker (incluido **Mosquitto**) y espera a que estén healthy. Puede tardar 2-3 minutos.

FastAPI y Streamlit **no** arrancan solos: se lanzan con los alias `api` y `ui` (paso 3).

### 2. Inicializar el pipeline (una vez por Codespace)

Una vez que `start.sh` finalice, ejecuta en el terminal:

```bash
source .devcontainer/init_pipeline.sh
```

Esto:
- Crea los **topics Kafka** (`sensors_raw`, `sensors_clean`, `sensors_verified`, `sensors_invalid`)
- Crea el **bucket RustFS** `datalake`
- Lanza los **3 jobs Flink** (normalización, analytics, hash verifier)
- Arranca el **rustfs-writer** (cold path Python)
- Prepara las **herramientas S3** opcionales: AWS CLI con la función `s3` y el cliente web en el puerto 19002
  (si alguna falla, solo avisa)
- Añade **aliases de desarrollo** al shell (`sim`, `bridge`, `api`, `ui`, `nb`, `flink-list`, ...)

### 3. Arrancar el pipeline completo

> **Importante:** `init_pipeline.sh` **purga y recrea los topics** cada vez que se ejecuta (se pierden
> los mensajes y se reinicia la hash-chain). Ejecútalo una vez por sesión.
>
> `init_pipeline.sh` ya arranca el `rustfs-writer` automáticamente y espera
> a que los 3 jobs Flink estén `RUNNING`. **No arranques `sim` hasta que el script confirme
> que los jobs están listos** — si el simulador arranca antes que el hash verifier,
> todos los mensajes irán al DLQ y `sensors_verified` quedará vacío.

```bash
# Terminal 1 — Bridge MQTT → Redpanda (debe arrancar ANTES que el simulador)
bridge
# equivale a: python src/01_ingestion/mqtt_to_redpanda_bridge.py

# Terminal 2 — Simulador de sensores con hash-chaining
# ⚠️  Solo arrancar después de que init_pipeline.sh haya confirmado los 3 jobs RUNNING
sim
# equivale a: python src/01_ingestion/sensor_simulator.py

# Terminal 3 — FastAPI REST + modelo IA
api
# equivale a: uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 4 — Dashboard Streamlit
ui
# equivale a: streamlit run src/05_ui/app.py --server.port 8501

# Terminal 5 (opcional) — Ver mensajes MQTT en tiempo real
mqtt-sub
# equivale a: mosquitto_sub -h mosquitto -p 1883 -t "sensors/telemetry" -v
```

> **rustfs-writer** ya corre en background (arrancado por `init_pipeline.sh`).
> Verifica con `rustfs-status`. Si se cayó, relánzalo con `rustfs-writer &`.
> Escribe archivos JSON a RustFS cada 60s — espera ~1 min para ver el primer archivo.

En ~30 segundos empezarán a llegar datos a Redpanda y desde ahí Flink los procesará hacia InfluxDB y RustFS.

### 4. Verificar estado del pipeline

Ejecuta los scripts en este orden:

```bash
# 1. Infraestructura: ¿responden todos los servicios? (justo tras init_pipeline.sh)
bash tests/test_connectivity.sh

# 2. Conexiones E2E: ¿funciona cada servicio por separado?
python tests/test_flow.py

# 3. Datos reales: ¿fluyen datos por el pipeline? (tras ~2 min con bridge + sim corriendo)
bash tests/verify_pipeline.sh      # alias: verify

# 4. Diagnóstico detallado de BBDDs y Grafana (si hay dudas)
python tests/test_databases.py     # alias: verify-db

# 5. Grafana: ¿responde cada panel? ¿están los endpoints que usan?
python tests/check_grafana_panels.py
python tests/check_grafana_endpoints.py

# 6. Cold path: ¿qué hay en el bucket?
python tests/list_datalake.py --preview      # o: s3 ls s3://datalake/ --recursive
```

### 5. Entrenar el modelo de anomalías (primera vez)

Una vez que haya datos fluyendo (~1 min), entrena el modelo IsolationForest:

```bash
curl -s -X POST http://localhost:8000/model/train | python3 -m json.tool
```

O desde el dashboard Streamlit → pestaña **🤖 IA Anomalías** → botón **Entrenar modelo**.

### 6. Acceder a las UIs

Desde la pestaña **Ports** de Codespaces:

| Interfaz          | Puerto | Descripción                    |
|-------------------|--------|--------------------------------|
| Streamlit         | 18501  | Dashboard principal            |
| FastAPI docs      | 18000  | API interactiva (`/docs`)      |
| Grafana           | 13000  | Dashboards (sin login)         |
| Flink UI          | 18081  | Jobs y métricas Flink          |
| Redpanda Console  | 18080  | Topics Kafka y mensajes        |
| JupyterLab        | 18888  | Notebooks de análisis          |
| InfluxDB          | 18086  | Series temporales (hot path)   |
| Consola S3 (RustFS) | 19001 | Consola RustFS (el login puede fallar en Codespaces) |
| S3 Web (s3manager)  | 19002 | Navegar el bucket `datalake` (solo lectura) |

### 7. JupyterLab (análisis ad-hoc)

```bash
nb
# Abrir: notebooks/01_exploracion_datos.ipynb
```

El notebook cubre: hot path (InfluxDB), cold path (RustFS NDJSON vía DuckDB), Lambda Query (UNION ALL), entrenamiento IsolationForest y verificación de hash-chain.

## Resolución de problemas

### `docker compose ps` no muestra nada o da error

El compose está en `.devcontainer/` y Codespaces lo arranca con **su propio nombre de proyecto**,
así que `docker compose ps` desde la raíz no lo encuentra. Usa los helpers (tras `init_pipeline.sh`):

```bash
dps                    # estado de los contenedores (equivale a docker compose ps)
dc logs -f grafana     # docker compose del proyecto de Codespaces
dc restart grafana
```

No uses `docker compose -f .devcontainer/docker-compose.yml up -d`: crea un **segundo** proyecto
que choca con los puertos del primero. Para recrear todo: *Codespaces: Rebuild Container*.

### Flink jobs caídos

Si `flink-list` muestra menos de 3 jobs `RUNNING`:

```bash
# Ver qué job falló y su excepción
flink-jobs
JID=$(curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys,json; jobs=json.load(sys.stdin)['jobs']
failed=[j for j in jobs if j['state']=='FAILED']
print(failed[0]['jid'] if failed else '')")
curl -s "http://jobmanager:8081/jobs/${JID}/exceptions" | python3 -c "
import sys,json; print(json.load(sys.stdin).get('root-exception','')[:500])"

# Relanzar todos los jobs
flink-restart
```

### Sin datos en Grafana

1. Verifica que `sim` y `bridge` están corriendo: `ps aux | grep -E "simulator|bridge"`
2. Verifica topics con mensajes en Redpanda Console (puerto 18080)
3. Verifica que los 3 jobs Flink están `RUNNING`: `flink-jobs`
4. El dashboard *Telemetría IoT* usa rango de 30 min y cada punto es una ventana de 1 min: tras reiniciar
   los jobs espera 2-3 minutos.
5. Para ver el error real de cada panel sin pasar el ratón por encima:
   `python tests/check_grafana_panels.py` (y `python tests/check_grafana_endpoints.py` para los endpoints).
6. Si cambias `config/grafana/provisioning/datasources/datasources.yml` (p. ej. `allowedHosts` de
   Infinity), **reinicia Grafana**: los datasources solo se leen al arrancar (los dashboards se recargan
   solos cada 30 s):
   `docker restart $(docker ps -qf label=com.docker.compose.service=grafana)`

### El Codespace arranca en "modo recuperación"

Abre el log de creación del Codespace y busca el **primer** `Error pull` (los `Interrupted` y
`No such image` de las demás imágenes son consecuencia: Compose cancela el resto cuando uno falla).
Fue lo que pasó con `minio/minio:latest` (imagen retirada) y por eso se cambió a RustFS con versión fija.
Tras corregir el `docker-compose.yml`: `git pull` y *Codespaces: Rebuild Container*.

### La consola de RustFS (:19001) no deja iniciar sesión

Si el log de `minio` muestra `SignatureDoesNotMatch` con la contraseña correcta, el problema es la firma
de la petición tras el proxy de puertos de Codespaces, no la contraseña. No es bloqueante; usa:

- el cliente web **s3manager** en el puerto **19002** (`s3web status` / `s3web start`), o
- la terminal: `s3 ls s3://datalake/ --recursive` o `python tests/list_datalake.py --preview`.

### Sin datos en RustFS (cold path)

```bash
# Verificar si está corriendo y arrancarlo si no
pgrep -f kafka_to_minio && echo "rustfs-writer OK" || { echo "Arrancando..."; rustfs-writer & }

# Seguir el log
tail -f /tmp/minio_writer.log
```

Los archivos JSON aparecen en `datalake/clean/year=.../month=.../day=.../hour=.../` cada 60 segundos.

**Verificar objetos en RustFS y que DuckDB los lee correctamente:**

```bash
python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)
objs = list(c.list_objects('datalake', recursive=True))
print(f'{len(objs)} objetos en datalake')
for o in objs[-3:]:
    print(f'  {o.object_name}  ({o.size} bytes)')
"

python3 -c "
import duckdb
conn = duckdb.connect()
conn.execute('LOAD httpfs;')
conn.execute(\"\"\"
    SET s3_endpoint='minio:9000'; SET s3_access_key_id='admin';
    SET s3_secret_access_key='Ilerna_Programaci0n'; SET s3_use_ssl=false; SET s3_url_style='path';
\"\"\")
df = conn.execute(\"SELECT * FROM read_json('s3://datalake/clean/**/*.json', auto_detect=true) LIMIT 2\").df()
print('Campos:', df.columns.tolist())
print(df[['device_id','temperature_c','ts']])
"
```

El schema correcto de `sensors_clean` tiene `temperature_c`. Si los archivos existentes muestran `temperature` (schema antiguo), borrarlos y dejar que `kafka_to_minio` regenere:

```bash
python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)
objs = list(c.list_objects('datalake', recursive=True))
for o in objs:
    c.remove_object('datalake', o.object_name)
print(f'{len(objs)} archivos eliminados — espera 60s para los nuevos')
"
```

**Verificar que no hay dos instancias compitiendo:**

```bash
pgrep -fa kafka_to_minio
# Si hay dos PIDs, matar el duplicado: kill <PID_duplicado>
```

### `sensors_verified` tiene 0 mensajes (todo va al DLQ)

**Causa:** el simulador arrancó antes que el hash verifier, o el Codespace fue reiniciado
(los topics conservan mensajes de la sesión anterior y la cadena hash se rompe).
Si `sensors_verified` se queda **congelado** mientras `sensors_raw` crece, el simulador es anterior
a la corrección del fallo `broken_chain` (`git pull` y reinicia `sim`).

**Solución — purgar topics y reiniciar en orden correcto:**

```bash
# 1. Para el simulador
pkill -f simulator.py

# 2. Purga topics (borra mensajes viejos)
RP="$(docker ps -qf 'label=com.docker.compose.service=redpanda')"
for T in sensors_raw sensors_clean sensors_verified sensors_invalid; do
  docker exec "$RP" rpk topic delete "$T" 2>/dev/null
  docker exec "$RP" rpk topic create "$T" -p 1 -r 1
done

# 3. Reinicia jobs Flink (cancela existentes y relanza)
flink-restart

# 4. Espera ~15s a que los jobs estén RUNNING
flink-jobs

# 5. Arranca bridge y luego sim
bridge &
sleep 3
sim &
```

Tras esto `sensors_verified` empezará a recibir mensajes en ~30 segundos.

```bash
# Comprobar cuántos mensajes hay en cada topic
python3 -c "
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
a = AdminClient({'bootstrap.servers': 'redpanda:29092'})
c = Consumer({'bootstrap.servers': 'redpanda:29092', 'group.id': 'debug'})
for topic in ['sensors_raw','sensors_clean','sensors_verified','sensors_invalid']:
    meta = a.list_topics(topic).topics[topic]
    parts = [TopicPartition(topic, p) for p in meta.partitions]
    lo_hi = c.get_watermark_offsets(parts[0], timeout=3)
    print(f'  {topic:<25}: {lo_hi[1] - lo_hi[0]:>6} mensajes')
c.close()
"
```

```bash
# Ver schema real de un mensaje de sensors_clean (debe tener temperature_c)
python3 -c "
from confluent_kafka import Consumer, TopicPartition
import json
c = Consumer({'bootstrap.servers':'redpanda:29092','group.id':'debug-schema','auto.offset.reset':'latest','enable.auto.commit':False})
c.assign([TopicPartition('sensors_clean', 0)])
for _ in range(5):
    msg = c.poll(2.0)
    if msg and not msg.error():
        print('sensors_clean campos:', list(json.loads(msg.value()).keys()))
        break
c.close()
"
```

## Topics Kafka

| Topic               | Productor                 | Consumidor(es)                           | Contenido                                           |
|---------------------|---------------------------|------------------------------------------|-----------------------------------------------------|
| `sensors/telemetry` | sensor_simulator          | mqtt_to_redpanda_bridge                  | MQTT raw (C/F/K, fallos posibles)                   |
| `sensors_raw`       | mqtt_to_redpanda_bridge   | flink_normalization, flink_hash_verifier | `{device_id, temperature, unit, ts, hash, prev_hash}` |
| `sensors_clean`     | flink_normalization_job   | flink_analytics_job, kafka_to_minio      | `{device_id, temperature_c, unit_original, ts}`     |
| `sensors_verified`  | flink_hash_verifier_job   | —                                        | Mensajes con hash-chain íntegra                     |
| `sensors_invalid`   | flink_hash_verifier_job   | —                                        | DLQ: mensajes con hash roto o JSON inválido         |

## Jobs Flink

| Job                        | Entrada          | Salida                           | Descripción                          |
|----------------------------|------------------|----------------------------------|--------------------------------------|
| flink_normalization_job    | sensors_raw      | sensors_clean                    | Normaliza unidades a °C              |
| flink_analytics_job        | sensors_clean    | InfluxDB (machine_stats)         | Ventana 1 min, alertas > 80°C        |
| flink_hash_verifier_job    | sensors_raw      | sensors_verified / sensors_invalid | Verifica integridad SHA256         |

**Cold path (proceso Python, no Flink):**

| Script                  | Entrada       | Salida                      | Descripción                                 |
|-------------------------|---------------|-----------------------------|---------------------------------------------|
| kafka_to_minio.py       | sensors_clean | RustFS datalake/clean/       | NDJSON particionado Hive, ventana 60s       |

## Variables de entorno principales

Los scripts Python usan **hostnames internos Docker** por defecto (red `ilerna_ia_big_data`).
Sobrescribir con variables de entorno si se ejecutan fuera del devcontainer.

| Variable          | Default (interno Docker)    | Descripción                      |
|-------------------|-----------------------------|----------------------------------|
| `MQTT_HOST`       | `mosquitto`                 | Host del broker MQTT             |
| `MQTT_PORT`       | `1883`                      | Puerto MQTT (interno)            |
| `KAFKA_BROKER`    | `redpanda:29092`            | Bootstrap servers Kafka          |
| `INFLUX_URL`      | `http://influxdb:8086`      | URL InfluxDB                     |
| `INFLUX_TOKEN`    | `supersecrettoken`          | Token de autenticación InfluxDB  |
| `MINIO_ENDPOINT`  | `minio:9000`                | Endpoint RustFS S3                |
| `ALERT_THRESHOLD` | `80.0`                      | Temperatura de alerta (°C)       |
| `MINIO_BUCKET`    | `datalake`                  | Bucket RustFS para histórico      |
