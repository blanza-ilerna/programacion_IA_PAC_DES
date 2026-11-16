# Guía de la Práctica — ILERNA Smart-Industry PAC DES

## Contexto

**ILERNA Smart-Industry** necesita monitorizar la temperatura de sus máquinas en tiempo real. Los sensores envían datos en diferentes unidades (Celsius, Fahrenheit, Kelvin) y a veces presentan fallos o intentos de manipulación. El objetivo es construir un pipeline de datos completo que recoja, limpie, analice y visualice esta información para detectar anomalías antes de que ocurra una avería.

---

## Arquitectura completa del sistema

```
┌─────────────────────────────────────────────────────────────────────┐
│  GENERACIÓN                                                          │
│  sensor_simulator.py                                                 │
│  · 5 máquinas | C/F/K aleatorio | fallos 5% | hash-chaining SHA256  │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ MQTT QoS 1  topic: sensors/telemetry
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  INGESTA                                                             │
│  Mosquitto :11883  →  mqtt_to_redpanda_bridge.py                    │
│  · Valida schema (device_id, temperature, unit∈{C,F,K}, ts)        │
│  · Enriquece (_mqtt_topic, _ingested_at)                            │
│  · enable.idempotence=True | QoS 1 | clave Kafka = device_id       │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ Kafka  topic: sensors_raw
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│  REDPANDA (Kafka API)  :19092                                        │
│  Topics: sensors_raw · sensors_clean · sensors_invalid              │
│          sensors_verified                                           │
└──────┬───────────────────┬───────────────────────────────────────────┘
       │                   │
       │ Flink Job A       │ Flink Job B (opcional, seguridad)
       ▼                   ▼
┌──────────────────┐  ┌───────────────────────────────────────┐
│ NORMALIZACIÓN    │  │ HASH VERIFIER                          │
│ Hito 2           │  │ · KeyedProcessFunction por device_id  │
│ Table API + UDF  │  │ · Verifica SHA256 chain por máquina   │
│ to_celsius(F,K→C)│  │ · Válidos → sensors_verified           │
│ DLQ StatementSet │  │ · Roto/Tampered → sensors_invalid(DLQ)│
│ ✅sensors_clean  │  └───────────────────────────────────────┘
│ ❌sensors_invalid│
└──────┬───────────┘
       │ Kafka  topic: sensors_clean
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ANALÍTICA  Hito 3 — flink_analytics_job.py                         │
│  · Tumble Window 1 min por device_id (watermark 10s)                │
│  · AVG/MAX temperature_c + count                                    │
│  · ALERTA si avg_temp_c > 80°C (flag alert=1)                       │
│  · Sink: InfluxDB via HTTP Line Protocol (urllib, sin deps extras)   │
└──────────────────────────┬──────────────────────────────────────────┘
       ┌────────────────────┴──────────────────────┐
       │ measurement: machine_stats               │ Python process (cold path)
       ▼                                         ▼
┌──────────────────┐              ┌──────────────────────────────────────┐
│  HOT PATH        │              │  COLD PATH                            │
│  InfluxDB :18086 │              │  kafka_to_minio.py (proceso Python)   │
│  bucket: sensores│              │  Consume sensors_clean vía Kafka      │
│  (tiempo real)   │              │  Escribe NDJSON particionado Hive:    │
└──────────────────┘              │  datalake/clean/year=.../month=.../   │
       │                          │  day=.../hour=.../<ts>.json           │
       │                          │  RustFS :19000  (ventana 60s)          │
       │                          └──────────────────────────────────────┘
       │                                          │
       └──────────────────┬───────────────────────┘
                          │ Arquitectura Lambda: Hot + Cold
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SERVICIO  Hito 4                                                    │
│  ┌───────────────────────────┐  ┌──────────────────────────────────┐│
│  │ FastAPI :18000             │  │ Streamlit :18501                 ││
│  │ /machines/status           │  │ 📡 Tiempo Real (gauges)          ││
│  │ /machines/{id}             │  │ 📈 Historial (series temporales) ││
│  │ /machines/{id}/predict ←IA │  │ ⚠️  Alertas (scatter)           ││
│  │ /alerts                    │  │ 🗄️  Lambda Query (DuckDB UNION) ││
│  │ /model/train               │  │ 🤖 IA Anomalías (IsolationForest)││
│  │ /model/status              │  │ 🏥 Pipeline Health               ││
│  └───────────────────────────┘  │ 🔐 Hash Chain / DLQ              ││
│  IsolationForest (scikit-learn) — entrenado con datos de InfluxDB   │
│  Edge: Flink detecta avg>80°C | Cloud: ML detecta anomalías raras  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Guía paso a paso — Puesta en marcha completa

### Paso 0 — Arrancar el entorno

`start.sh` se ejecuta automáticamente al abrir el Codespace y levanta todos los contenedores Docker. Cuando finalice, verás el mensaje `Infraestructura lista`.

A continuación inicializa el pipeline (topics, bucket, jobs Flink):

```bash
source .devcontainer/init_pipeline.sh
```

Para verificar que todo está healthy:

```bash
bash tests/test_connectivity.sh
```

Si algún servicio falla, reiniciar:
```bash
docker compose -f .devcontainer/docker-compose.yml down --remove-orphans
docker compose -f .devcontainer/docker-compose.yml up -d
```

### Paso 1 — Verificar Flink

`Dockerfile.jobmanager` incluye el JAR Kafka SQL Connector y extrae `pyflink.zip` para que los UDFs Python funcionen. `init_pipeline.sh` lanza los 3 jobs al arrancar. Verifica:

```bash
# TaskManager registrado
curl -s http://jobmanager:8081/taskmanagers | python3 -m json.tool | grep -i "id\|slots"

# Jobs corriendo
flink-list
```

### Paso 2 — Verificar topics Kafka

Creados automáticamente por `init_pipeline.sh`. Para verificar:

```bash
RP=$(docker ps -qf "label=com.docker.compose.service=redpanda")
docker exec $RP rpk topic list
```

### Paso 3 — Verificar bucket RustFS

Creado automáticamente por `init_pipeline.sh`. Para verificar:

```bash
python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)
print('✅ Bucket datalake existe' if c.bucket_exists('datalake') else '❌ Bucket datalake NO existe')
"
```

### Paso 4 — Lanzar los jobs Flink

Lanzados automáticamente por `init_pipeline.sh`. Para lanzar manualmente o relanzar:

```bash
JM=$(docker ps -qf "label=com.docker.compose.service=jobmanager")

# Hito 2: Normalización + DLQ
docker exec $JM flink run -py /opt/flink/jobs/flink_normalization_job.py &

# Aportación A1: Hash verifier
docker exec $JM flink run -py /opt/flink/jobs/flink_hash_verifier_job.py &

# Hito 3: Analítica + alertas → InfluxDB
docker exec $JM flink run -py /opt/flink/jobs/flink_analytics_job.py &

# Verificar los 3 jobs RUNNING
flink-list
```

> **Nota**: El cold path (RustFS) lo gestiona `kafka_to_minio.py` como proceso Python independiente,
> no un job Flink. Se arranca automáticamente con `init_pipeline.sh` en el paso 4/4.

### Paso 5 — Arrancar el pipeline Python

Abre terminales separadas en Codespaces (botón `+` en el panel de terminales):

```bash
# Terminal 1 — Bridge MQTT → Redpanda — Hito 1 (alias: bridge)
# ⚠️  Debe arrancar ANTES que el simulador para no perder mensajes
python src/01_ingestion/mqtt_to_redpanda_bridge.py

# Terminal 2 — Simulador con hash-chaining + fallos (alias: sim)
python src/01_ingestion/sensor_simulator.py --machines 5 --interval 2 --fault-rate 0.1

# Terminal 3 — FastAPI — Hito 4 (alias: api)
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 4 — Dashboard Streamlit — Hito 4 (alias: ui)
streamlit run src/05_ui/app.py --server.port 8501
```

> **Cold path (RustFS)**: `kafka_to_minio.py` lo arranca automáticamente `init_pipeline.sh`.
> Si necesitas relanzarlo manualmente:
> ```bash
> python src/03_storage/kafka_to_minio.py &
> # alias: rustfs-writer
> ```

### Paso 6 — Entrenar el modelo de IA

> **El entrenamiento es manual.** El modelo IsolationForest no se entrena
> automáticamente al arrancar FastAPI. Esto permite elegir la ventana temporal
> y el parámetro de contaminación según los datos disponibles.
> Una vez entrenado, el modelo se persiste en `/tmp/isolation_forest.pkl`
> y se recarga automáticamente si FastAPI se reinicia (mientras el contenedor esté vivo).

```bash
# Prerequisito: esperar al menos 5 min con el pipeline corriendo
# para que haya datos en InfluxDB (measurement: machine_stats)

# 1. Verificar estado antes de entrenar (debe mostrar "trained": false)
curl -s http://localhost:8000/model/status | python3 -m json.tool

# 2. Entrenar con los últimos 60 minutos de datos
curl -X POST "http://localhost:8000/model/train?range_minutes=60&contamination=0.1" \
  | python3 -m json.tool

# Si el pipeline lleva poco tiempo y hay pocos datos, usar ventana corta:
curl -X POST "http://localhost:8000/model/train?range_minutes=10&contamination=0.1" \
  | python3 -m json.tool

# 3. Confirmar que el modelo está entrenado
curl -s http://localhost:8000/model/status | python3 -m json.tool
# Respuesta esperada: {"trained": true, "samples": N, "contamination": 0.1, "stats": {...}}
```

### Paso 7 — Verificar el sistema completo

```bash
# Estado de máquinas
curl -s http://localhost:8000/machines/status | python3 -m json.tool

# Alertas activas
curl -s http://localhost:8000/alerts | python3 -m json.tool

# Predicción IA para machine-004 (la que suele superar 80°C)
curl -s "http://localhost:8000/machines/machine-004/predict?temperature_c=85.0" | python3 -m json.tool

# Ver DLQ — mensajes rechazados
docker exec $(docker ps -qf "label=com.docker.compose.service=redpanda") \
  rpk topic consume sensors_invalid -n 5
```

### Paso 8 — Abrir el dashboard

En la pestaña **Ports** de Codespaces, abre el puerto **18501** (Streamlit).

Navega por las pestañas en orden:
1. **📡 Tiempo Real** → ver gauges de las 5 máquinas
2. **📈 Historial** → ver serie temporal de temperatura
3. **⚠️ Alertas** → verificar que machine-004 genera alertas
4. **🗄️ Lambda Query** → clic en "Ejecutar Query Federada" y ver unión hot+cold
5. **🤖 IA Anomalías** → entrenar modelo y hacer predicciones
6. **🏥 Pipeline Health** → estado de jobs Flink, topics Kafka y servicios
7. **🔐 Hash Chain** → mensajes verificados y DLQ
8. **🔬 Modelos Avanzados** → Prophet, RandomForest, CUSUM, K-Means
9. **❓ Ayuda** → arquitectura, endpoints, comandos de arranque

---

## Hito 1 — El Puente de Ingesta

**Archivo:** `src/01_ingestion/mqtt_to_redpanda_bridge.py`

### Objetivo
Construir un cliente que actúe de puente entre el broker MQTT y Redpanda (Kafka), con validación de mensajes.

### Schema del mensaje (topic `sensors/telemetry`)

Con hash-chaining activo (el simulador añade `prev_hash` y `hash`):

```json
{
  "device_id":   "machine-001",
  "temperature": 176.5,
  "unit":        "F",
  "ts":          "2026-03-11T09:00:00Z",
  "prev_hash":   "a3f8c2...",
  "hash":        "d7e91b..."
}
```

Campos obligatorios validados por el bridge: `device_id` (str no vacío), `temperature` (numérico), `unit` ∈ {C, F, K}, `ts` (presente).

### Funcionamiento

1. **Conexión MQTT** con `CallbackAPIVersion.VERSION2` (paho-mqtt ≥ 2.0) y QoS 1.
2. **Validación** en `on_message`: rechaza mensajes con JSON malformado, campos faltantes o unidades no reconocidas (log warning, no crash).
3. **Enriquecimiento**: añade `_mqtt_topic`, `_mqtt_qos`, `_ingested_at` (ms).
4. **Producción a Kafka** con `enable.idempotence=True`. La clave Kafka es `device_id` para garantizar orden por dispositivo.

### Cómo probar

```bash
# Publicar mensaje válido con hash
mosquitto_pub -h localhost -p 11883 -t "sensors/telemetry" \
  -m '{"device_id":"machine-001","temperature":176.0,"unit":"F","ts":"2026-03-11T00:00:00Z","prev_hash":"0000000000000000000000000000000000000000000000000000000000000000","hash":"abc123"}'

# Publicar mensaje corrupto (sin temperature)
mosquitto_pub -h localhost -p 11883 -t "sensors/telemetry" \
  -m '{"device_id":"machine-001","unit":"C","ts":"2026-03-11T00:00:00Z"}'
# → WARNING | Mensaje rechazado — campo 'temperature' ausente

# Ver lo que llegó a Redpanda
docker exec $(docker ps -qf "label=com.docker.compose.service=redpanda") \
  rpk topic consume sensors_raw -n 1
```

---

## Hito 2 — Normalización con Flink y UDF

**Archivo:** `jobs/flink_normalization_job.py`

### Objetivo
Usar Apache Flink Table API con una UDF Python para limpiar, normalizar y enrutar los datos.

### UDF `to_celsius`

```python
@udf(result_type=DataTypes.DOUBLE())
def to_celsius(temperature: float, unit: str) -> float:
    unit = unit.strip().upper()
    if unit == "F":
        return (temperature - 32.0) * 5.0 / 9.0
    if unit == "K":
        return temperature - 273.15
    return float(temperature)  # "C" → pass-through
```

### Conversiones de referencia

| Unidad | Fórmula → Celsius    | Ejemplo          |
|--------|----------------------|------------------|
| F      | `(F - 32) × 5 / 9`  | 176°F → 80.0°C   |
| K      | `K - 273.15`         | 353.15K → 80.0°C |
| C      | pass-through         | 80°C → 80.0°C    |

### Dead Letter Queue (DLQ)

En lugar de descartar silenciosamente los mensajes inválidos, el job los enruta al topic `sensors_invalid` con un campo `reason` que explica el motivo de rechazo. Esto permite auditoría y reprocess posterior.

El job usa `StatementSet` para ejecutar **dos INSERT INTO en el mismo job Flink** de forma atómica:

```python
stmt_set = t_env.create_statement_set()
stmt_set.add_insert_sql("INSERT INTO sensors_clean  SELECT ... FROM sensors_raw WHERE <válidos>")
stmt_set.add_insert_sql("INSERT INTO sensors_invalid SELECT ... FROM sensors_raw WHERE <inválidos>")
stmt_set.execute()
```

Cada mensaje va a **exactamente uno** de los dos sinks.

Ejemplos de mensajes en `sensors_invalid`:
```json
{"device_id": null,       "temperature": 75.0, "unit": "C",       "reason": "device_id ausente o vacío"}
{"device_id": "m-001",   "temperature": null,  "unit": "C",       "reason": "temperature ausente o no numérico"}
{"device_id": "m-fault", "temperature": 75.0,  "unit": "RANKINE", "reason": "unidad no reconocida: RANKINE"}
{"device_id": "m-001",   "temperature": 99999, "unit": "C",       "reason": "temperatura sobre máximo: 99999.00C"}
```

### Filtros para `sensors_clean`

- `device_id IS NOT NULL AND device_id <> ''`
- `temperature IS NOT NULL`
- `UPPER(TRIM(unit)) IN ('C', 'F', 'K')`
- `to_celsius(temperature, unit) >= -50.0`
- `to_celsius(temperature, unit) <= 1000.0`

### Schema de salida (topic `sensors_clean`)

```json
{
  "device_id":      "machine-001",
  "temperature_c":  80.28,
  "unit_original":  "F",
  "ts":             "2026-03-11T09:00:00Z",
  "_ingested_at":   1741687200000
}
```

### Cómo ejecutar y verificar

```bash
JM=$(docker ps -qf "label=com.docker.compose.service=jobmanager")

# Lanzar
docker exec $JM flink run -py /opt/flink/jobs/flink_normalization_job.py

# Verificar en Flink UI
curl -s http://localhost:18081/jobs | python3 -m json.tool

# Ver mensajes válidos normalizados
docker exec $(docker ps -qf "label=com.docker.compose.service=redpanda") \
  rpk topic consume sensors_clean -n 3

# Ver mensajes rechazados (DLQ)
docker exec $(docker ps -qf "label=com.docker.compose.service=redpanda") \
  rpk topic consume sensors_invalid -n 3
```

---

## Hito 3 — Inteligencia y Alertas

**Archivo:** `jobs/flink_analytics_job.py`

### Objetivo
Calcular estadísticas por ventana de tiempo y generar alertas cuando la temperatura media supera 80°C.

### Tumble Window de 1 minuto

```sql
SELECT
    device_id,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time,   INTERVAL '1' MINUTE) AS window_end,
    AVG(temperature_c)                             AS avg_temp_c,
    MAX(temperature_c)                             AS max_temp_c,
    COUNT(*)                                       AS count_readings
FROM sensors_clean
WHERE temperature_c IS NOT NULL
GROUP BY device_id, TUMBLE(event_time, INTERVAL '1' MINUTE)
```

**Watermark**: 10 segundos de tolerancia a mensajes tardíos.

### Lógica de alerta

El campo `alert` en InfluxDB vale `1` si `avg_temp_c > 80.0` (configurable con `ALERT_THRESHOLD`).

### Escritura en InfluxDB (HTTP Line Protocol)

```
machine_stats,device_id=machine-001 avg_temp_c=82.34,max_temp_c=87.12,count=28i,alert=1i 1741687260
```

El job usa `urllib.request` (stdlib Python, sin dependencias externas en el contenedor Flink).

### Cómo verificar

```bash
# Datos en InfluxDB
curl -s -X POST "http://localhost:18086/api/v2/query?org=ilerna" \
  -H "Authorization: Token supersecrettoken" \
  -H "Content-Type: application/vnd.flux" \
  -d 'from(bucket:"sensores") |> range(start:-5m) |> filter(fn:(r) => r._measurement=="machine_stats") |> last()'

# Alertas via FastAPI
curl -s http://localhost:8000/alerts | python3 -m json.tool
```

---

## Hito 4 — API de Servicio y Dashboard

### FastAPI — `src/api/main.py`

```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
# Docs interactivos: http://localhost:8000/docs
```

#### Endpoints completos

| Método | Endpoint                      | Descripción                                         |
|--------|-------------------------------|-----------------------------------------------------|
| GET    | `/health`                     | Estado API + conectividad InfluxDB                  |
| GET    | `/machines/status`            | Última temperatura y alerta por máquina             |
| GET    | `/machines/{id}`              | Historial de temperatura de una máquina             |
| GET    | `/machines/{id}/predict`      | Predicción IsolationForest (IA Cloud)               |
| GET    | `/alerts`                     | Ventanas donde avg_temp_c superó el umbral          |
| GET    | `/stats`                      | Estadísticas (media, min, max) por máquina          |
| POST   | `/machines/publish`           | Publica lectura manual vía MQTT                     |
| POST   | `/model/train`                | Entrena IsolationForest con datos de InfluxDB       |
| GET    | `/model/status`               | Estado del modelo ML                                |

#### Ejemplos de uso

```bash
# Ver estado de todas las máquinas
curl -s http://localhost:8000/machines/status | python3 -m json.tool

# Publicar lectura manual (200°F = 93.3°C → generará alerta)
curl -X POST http://localhost:8000/machines/publish \
  -H "Content-Type: application/json" \
  -d '{"device_id":"machine-001","temperature":200.0,"unit":"F"}'

# Entrenar modelo con los últimos 10 minutos
curl -X POST "http://localhost:8000/model/train?range_minutes=10&contamination=0.1"

# Predecir si 85°C es anómalo para machine-004
curl -s "http://localhost:8000/machines/machine-004/predict?temperature_c=85.0"
```

---

### Streamlit Dashboard — `src/05_ui/app.py`

```bash
streamlit run src/05_ui/app.py --server.port 8501
# Abrir: http://localhost:8501
```

#### Pestañas del dashboard

**📡 Tiempo Real**
- Gauges de temperatura por máquina (verde = normal, rojo = alerta)
- Auto-refresco cada 5 segundos

**📈 Historial**
- Línea temporal de temperatura media por minuto
- Línea de umbral a 80°C

**⚠️ Alertas**
- Scatter plot de eventos sobre 80°C con timestamp y máquina

**🗄️ Lambda Query** (Arquitectura Lambda)

Botón que lanza una **query federada con DuckDB**:
1. Consulta InfluxDB (hot path) → `pandas.DataFrame`
2. Lee NDJSON de RustFS vía `read_json()` con `hive_partitioning=true` (cold path) → `pandas.DataFrame`
3. DuckDB registra ambos DataFrames como tablas virtuales y hace `UNION ALL`
4. Muestra gráfica unificada coloreada por fuente y estadísticas cruzadas

```python
conn.register("hot_data",  hot_df)   # InfluxDB → RAM
conn.register("cold_data", cold_df)  # RustFS NDJSON → RAM
result = conn.execute("""
    SELECT device_id, value, ts, 'InfluxDB (hot)' AS source FROM hot_data
    UNION ALL
    SELECT device_id, value, ts, 'RustFS (cold)'   AS source FROM cold_data
    ORDER BY ts DESC LIMIT 5000
""").df()
```

**🤖 IA Anomalías**
- Panel de entrenamiento del modelo (selección de ventana + tasa de contaminación)
- Predictor interactivo: introduce temperatura → obtén `is_anomaly` + `failure_prob`
- Análisis masivo: evalúa todo el histórico reciente y colorea por probabilidad de fallo

---

### Jupyter Notebook — `notebooks/01_exploracion_datos.ipynb`

Notebook de exploración de datos que cubre el pipeline completo:

```bash
# Abrir Jupyter (puerto 18888 en Codespaces)
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
```

#### Secciones del notebook

**Sección 1 — Conexión y configuración**
- Imports, supresión de warnings y variables de conexión para InfluxDB, RustFS y FastAPI (hostnames internos del devcontainer)

**Sección 2 — InfluxDB: machine_stats (Hot Path)**
- Query Flux de los últimos 60 min con `pivot()` sobre el measurement `machine_stats`
- Gráfica de líneas de `avg_temp_c` por `device_id` con umbral de alerta a 80°C
- Gauges de temperatura actual por máquina
- Tabla de estadísticas (media, máximo, alertas) por dispositivo

**Sección 3 — RustFS: Cold Path NDJSON**
- DuckDB con extensión `httpfs` conectado a RustFS (`minio:9000`)
- Query sobre `s3://datalake/clean/**/*.json` con `read_json()` y `hive_partitioning=true`
- Columnas explícitas para manejar esquemas mixtos de archivos históricos
- Distribución de registros por dispositivo

**Sección 4 — Lambda Query: UNION ALL Hot + Cold**
- Query federada DuckDB que unifica InfluxDB (hot) y RustFS NDJSON (cold) vía `UNION ALL`
- Gráfica unificada coloreada por fuente de datos

**Sección 5 — IsolationForest: Entrenamiento y Predicción**
- `GET /model/status` — estado del modelo
- `POST /model/train?range_minutes=60&contamination=0.1` — entrenamiento
- Predicciones individuales (normal 70°C vs anómala 95°C)
- Predicciones masivas sobre los últimos 30 min con scatter plot por `failure_prob`

**Sección 6 — Hash Chain: Verificación de integridad**
- Explicación del mecanismo SHA256 y los tópicos `sensors_verified` / `sensors_invalid`

---

## Aportaciones avanzadas

### A1 — Hash-Chaining SHA256 (Seguridad e Integridad)

**Archivos:** `sensor_simulator.py` + `jobs/flink_hash_verifier_job.py`

#### Concepto

Cada mensaje incluye un hash SHA256 que depende del contenido del mensaje y del hash del mensaje anterior. Esto forma una cadena criptográfica (similar a blockchain) que hace detectables las manipulaciones.

```
msg_1: { ..., prev_hash="0"×64,   hash=SHA256(content_1 + "0"×64) }
msg_2: { ..., prev_hash=hash_1,   hash=SHA256(content_2 + hash_1) }
msg_3: { ..., prev_hash=hash_2,   hash=SHA256(content_3 + hash_2) }
```

Si un atacante modifica `msg_2`, su hash cambia y `msg_3` detecta que `prev_hash` declarado ≠ hash real de `msg_2`.

#### Implementación en el simulador

```python
_chain_state: dict[str, str] = {}   # device_id → último hash

def compute_hash(payload: dict, prev_hash: str) -> str:
    content = {k: v for k, v in payload.items() if k not in ("hash", "prev_hash")}
    raw = json.dumps(content, sort_keys=True) + prev_hash
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def sign_message(payload: dict, device_id: str) -> dict:
    prev_hash = _chain_state.get(device_id, "0" * 64)
    payload["prev_hash"] = prev_hash
    payload["hash"]      = compute_hash(payload, prev_hash)
    _chain_state[device_id] = payload["hash"]
    return payload
```

#### Verificación en Flink (DataStream + estado)

```python
class HashChainVerifier(KeyedProcessFunction):
    def open(self, runtime_context):
        self.last_hash_state = runtime_context.get_state(
            ValueStateDescriptor("last_valid_hash", Types.STRING())
        )

    def process_element(self, raw_msg, ctx, out):
        data         = json.loads(raw_msg)
        state_hash   = self.last_hash_state.value() or "0" * 64
        reported_prev = data.get("prev_hash", state_hash)
        reported_hash = data.get("hash", "")

        # Verificación 1: ¿prev_hash coincide con el estado de Flink?
        if reported_prev != state_hash:
            data["reason"] = "hash_chain_broken: prev_hash no coincide"
            ctx.output(TAMPERED_TAG, json.dumps(data))   # → DLQ
            return

        # Verificación 2: ¿el hash es correcto?
        expected = compute_hash(data, reported_prev)
        if reported_hash != expected:
            data["reason"] = "hash_chain_broken: hash incorrecto"
            ctx.output(TAMPERED_TAG, json.dumps(data))   # → DLQ
            return

        self.last_hash_state.update(reported_hash)
        out.collect(raw_msg)                             # → sensors_verified
```

#### Cómo ejecutar y verificar

```bash
JM=$(docker ps -qf "label=com.docker.compose.service=jobmanager")
docker exec $JM flink run -py /opt/flink/jobs/flink_hash_verifier_job.py

# Simular una rotura deliberada de cadena
python src/01_ingestion/sensor_simulator.py --machines 1 --fault-rate 0.5 --once

# Ver mensajes detectados como manipulados
docker exec $(docker ps -qf "label=com.docker.compose.service=redpanda") \
  rpk topic consume sensors_invalid -n 5

# Los mensajes válidos van a sensors_verified
docker exec $(docker ps -qf "label=com.docker.compose.service=redpanda") \
  rpk topic consume sensors_verified -n 3
```

---

### A2 — Arquitectura Lambda con Python → RustFS NDJSON

**Archivo:** `src/03_storage/kafka_to_minio.py`

#### Concepto

La Arquitectura Lambda divide el procesamiento en dos caminos paralelos:

| Path | Latencia | Tecnología | Uso |
|------|----------|-----------|-----|
| **Hot** | segundos | Flink → InfluxDB | Alertas, monitorización en tiempo real |
| **Cold** | ~60 s | Python → RustFS/NDJSON | Análisis histórico, ML, reportes |

Ambos paths se combinan en la capa de consulta (DuckDB) para obtener una vista unificada.

> **Nota de implementación**: El conector FileSystem de Flink requería JARs S3/Hadoop que
> presentaban conflictos de dependencias en el entorno. El cold path se implementa como
> un proceso Python independiente (`kafka_to_minio.py`) que produce resultados equivalentes.

#### Funcionamiento de `kafka_to_minio.py`

```
1. Consume sensors_clean (confluent_kafka Consumer)
2. Acumula mensajes en una ventana de 60 segundos
3. Cada 60s escribe un archivo NDJSON (una línea JSON por mensaje)
4. Ruta: datalake/clean/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>.json
5. Hace commit de offsets solo cuando la escritura en RustFS fue exitosa
```

#### Particionado Hive

```
datalake/clean/
  year=2026/
    month=03/
      day=11/
        hour=09/
          1741687260.json
          1741687320.json
```

DuckDB aprovecha este particionado con filtros eficientes:
```sql
SELECT device_id, CAST(temperature_c AS DOUBLE), ts
FROM read_json(
    's3://datalake/clean/**/*.json',
    hive_partitioning = true,
    columns = {device_id: 'VARCHAR', temperature_c: 'DOUBLE', ts: 'VARCHAR'}
)
WHERE year = '2026' AND month = '03'   -- Solo lee las particiones necesarias
```

#### Query federada Lambda en Streamlit

```python
# Hot path: últimos 60 min de InfluxDB
hot_df  = query_influxdb(range_minutes=60)

# Cold path: histórico de RustFS NDJSON (puede ser días/semanas)
cold_df = query_cold_path()   # DuckDB read_json con hive_partitioning

# DuckDB une ambas fuentes en memoria (UNION ALL)
conn = duckdb.connect()
conn.register("hot",  hot_df)
conn.register("cold", cold_df)
unified = conn.execute("""
    SELECT device_id, value, ts, 'InfluxDB (hot)' AS source FROM hot
    UNION ALL
    SELECT device_id, value, ts, 'RustFS (cold)'   AS source FROM cold
    ORDER BY ts DESC LIMIT 5000
""").df()
```

#### Cómo ejecutar y verificar

```bash
# El proceso arranca automáticamente con init_pipeline.sh
# Para lanzarlo manualmente:
python src/03_storage/kafka_to_minio.py &
# alias: rustfs-writer

# Ver el log
tail -f /tmp/minio_writer.log

# Verificar archivos en RustFS (aparecen cada 60 segundos)
python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='admin', secret_key='Ilerna_Programaci0n', secure=False)
objects = list(c.list_objects('datalake', prefix='clean/', recursive=True))
print(f'{len(objects)} archivos NDJSON en RustFS')
for o in objects[:5]:
    print(' ', o.object_name, f'({(o.size or 0)/1024:.1f} KB)')
"
```

---

### A3 — IA Cloud: Detección de Anomalías con IsolationForest

**Archivos:** `src/api/anomaly_model.py` + `src/api/main.py`

#### Edge vs Cloud Intelligence

| Nivel | Dónde | Tecnología | Qué detecta | Latencia |
|-------|-------|-----------|-------------|----------|
| **Edge** | Flink (streaming) | Regla hardcoded | avg_temp > 80°C | ms |
| **Cloud** | FastAPI | IsolationForest (ML) | Comportamiento estadísticamente raro | ~100ms |

La detección Edge es rápida pero rígida. La detección Cloud es flexible: aprende el patrón normal de cada máquina y detecta anomalías aunque no superen el umbral fijo (por ejemplo, una temperatura inusualmente baja en una máquina que siempre estuvo caliente).

#### IsolationForest: cómo funciona

```
1. Entrenamiento (POST /model/train):
   · Lee N temperaturas de InfluxDB (histórico real)
   · Construye 100 árboles de decisión aleatorios
   · Los puntos normales necesitan muchos splits para ser aislados
   · Los puntos anómalos son aislados rápidamente (pocas particiones)

2. Predicción (GET /machines/{id}/predict?temperature_c=85.0):
   · score_samples() → float negativo
   · Más negativo = más anómalo (aislado con pocas particiones)
   · Se normaliza a failure_prob ∈ [0.0, 1.0]
```

#### Entrenamiento — siempre manual

El modelo **no se entrena automáticamente** al arrancar FastAPI.
Razón: el usuario debe decidir con qué ventana temporal entrenar
(depende de cuánto tiempo lleva el pipeline corriendo) y qué tasa
de contaminación usar (qué proporción de los datos considera anómalos).

Una vez entrenado, el modelo se serializa en `/tmp/isolation_forest.pkl`
y se recarga al reiniciar FastAPI, sin necesidad de re-entrenar
mientras el contenedor Docker siga activo.

#### Flujo completo de predicción

```bash
# 0. Verificar estado antes de entrenar
curl -s "http://localhost:8000/model/status" | python3 -m json.tool
# → {"trained": false, "message": "Modelo no entrenado. Ejecuta POST /model/train primero."}

# 1. Entrenar con datos reales (prerequisito: analytics job corriendo ≥5 min)
curl -X POST "http://localhost:8000/model/train?range_minutes=60&contamination=0.1" \
  | python3 -m json.tool
# → {"status": "trained", "samples": 1200, "stats": {"mean": 72.4, "std": 8.1, ...}}

# Si hay pocos datos (pipeline recién iniciado):
curl -X POST "http://localhost:8000/model/train?range_minutes=10&contamination=0.1"

# 2. Predecir temperatura normal (dentro del rango histórico)
curl -s "http://localhost:8000/machines/machine-003/predict?temperature_c=58.0" \
  | python3 -m json.tool
# → {"is_anomaly": false, "failure_prob": 0.02, "interpretation": "Temperatura normal..."}

# 3. Predecir temperatura anómala (alta)
curl -s "http://localhost:8000/machines/machine-003/predict?temperature_c=95.0" \
  | python3 -m json.tool
# → {"is_anomaly": true, "failure_prob": 0.87, "interpretation": "Temperatura excepcionalmente ALTA..."}
```

---

## Flows de datos completos (schemas por topic)

### `sensors/telemetry` (MQTT)
```json
{
  "device_id":   "machine-001",
  "temperature": 176.5,
  "unit":        "F",
  "ts":          "2026-03-11T09:00:00Z",
  "prev_hash":   "a3f8c2d1...",
  "hash":        "d7e91b3f..."
}
```

### `sensors_raw` (Redpanda — enriquecido por el bridge)
```json
{
  "device_id":    "machine-001",
  "temperature":  176.5,
  "unit":         "F",
  "ts":           "2026-03-11T09:00:00Z",
  "prev_hash":    "a3f8c2d1...",
  "hash":         "d7e91b3f...",
  "_mqtt_topic":  "sensors/telemetry",
  "_mqtt_qos":    1,
  "_ingested_at": 1741687200000
}
```

### `sensors_clean` (Redpanda — normalizado por Flink Hito 2)
```json
{
  "device_id":      "machine-001",
  "temperature_c":  80.28,
  "unit_original":  "F",
  "ts":             "2026-03-11T09:00:00Z",
  "_ingested_at":   1741687200000
}
```

### `sensors_invalid` (Redpanda — DLQ: rechazados + manipulados)
```json
{
  "device_id":   "machine-fault",
  "temperature": null,
  "unit":        "C",
  "ts":          "2026-03-11T09:00:00Z",
  "reason":      "temperature ausente o no numérico"
}
```

### `sensors_verified` (Redpanda — cadena hash verificada por Flink)
Mismo schema que `sensors_raw` + garantía de integridad criptográfica.

### `machine_stats` (InfluxDB — escrito por Flink Hito 3)
```
measurement: machine_stats
tags:    device_id=machine-001
fields:  avg_temp_c=82.34, max_temp_c=87.12, count=28, alert=1
time:    1741687260  (inicio de ventana en segundos Unix)
```

### `datalake/clean/year=2026/month=03/day=11/hour=09/*.json` (RustFS)
```
Columnas: device_id, temperature_c, unit_original, ts, _ingested_at
Formato:  NDJSON (una línea JSON por registro, sin dependencias extra)
Escrito por: kafka_to_minio.py (ventana 60s, particionado Hive por event_time)
```

---

## Preguntas frecuentes

**¿Por qué hash-chaining en el simulador y no en el bridge?**
El hash debe generarse en el origen del dato (Edge), no en el transporte. Si el bridge firmara los mensajes, no podría detectar manipulaciones ocurridas entre el sensor y el bridge. La firma en el sensor garantiza autenticidad desde el origen.

**¿Por qué Flink usa `ValueState` keyed por `device_id` para el hash verifier?**
Cada máquina tiene su propia cadena de hashes independiente. Si usáramos estado global, el mensaje 2 de machine-001 dependería del hash de machine-002, lo que es incorrecto. El estado keyed garantiza que cada cadena se verifica de forma aislada.

**¿Por qué dos jobs para hot/cold path en lugar de uno?**
El hot path (InfluxDB) necesita baja latencia y escribe registro por registro. El cold path (RustFS/NDJSON) necesita alta eficiencia y escribe por lotes/particiones. Mezclarlos en un job crearía presión entre ambas latencias. La separación permite escalar cada path independientemente.

**¿Por qué IsolationForest y no un umbral fijo en la API?**
Un umbral fijo (>80°C) no captura contexto histórico de cada máquina. machine-003 normalmente va a 55°C; si sube a 70°C, no supera el umbral pero es estadísticamente anómalo para esa máquina específica. IsolationForest aprende el "rango normal" de cada dispositivo, y el modelo entrenado sobre todos los datos lo captura globalmente.

**¿Qué pasa si el JSON está malformado?**
Primera línea de defensa: el bridge lo descarta con `WARNING JSON inválido`.
Segunda línea: Flink normalization tiene `json.ignore-parse-errors=true` → campos NULL → DLQ con `reason`.
Tercera línea: el hash verifier detecta cualquier manipulación post-ingesta.

**¿Por qué DuckDB en lugar de Spark para el análisis histórico?**
DuckDB es un motor OLAP embebido (sin servidor), perfecto para análisis interactivos desde Streamlit. Lee NDJSON desde S3/RustFS directamente con `httpfs` y `read_json()` sin descargar los ficheros completos. Para datasets > 100 GB o procesamiento distribuido se usaría Spark o Trino.

---

## Dependencias Python

| Módulo | Librería | Versión | Uso |
|--------|---------|---------|-----|
| `sensor_simulator.py` | `paho-mqtt` | ≥2.0 | MQTT con CallbackAPIVersion.VERSION2 |
| `sensor_simulator.py` | `hashlib` | stdlib | SHA256 hash-chaining |
| `mqtt_to_redpanda_bridge.py` | `paho-mqtt` | ≥2.0 | Suscripción MQTT |
| `mqtt_to_redpanda_bridge.py` | `confluent-kafka` | latest | Productor Kafka idempotente |
| `flink_normalization_job.py` | `apache-flink` | 1.18.0 | PyFlink Table API + UDF |
| `flink_analytics_job.py` | `apache-flink` | 1.18.0 | Tumble Window + sink InfluxDB |
| `flink_analytics_job.py` | `urllib.request` | stdlib | HTTP Line Protocol a InfluxDB (sin deps extras) |
| `flink_hash_verifier_job.py` | `apache-flink` | 1.18.0 | DataStream KeyedProcessFunction |
| `kafka_to_minio.py` | `confluent-kafka`, `minio` | latest | Consumidor Kafka → RustFS NDJSON (cold path) |
| `src/api/main.py` | `fastapi`, `uvicorn` | latest | API REST asíncrona |
| `src/api/main.py` | `influxdb-client` | latest | Consultas Flux a InfluxDB |
| `src/api/main.py` | `paho-mqtt` | ≥2.0 | Publicación manual vía MQTT |
| `src/api/anomaly_model.py` | `scikit-learn` | latest | IsolationForest |
| `src/api/anomaly_model.py` | `numpy` | ≥1.24 | Arrays para ML |
| `src/05_ui/app.py` | `streamlit` | latest | Dashboard interactivo |
| `src/05_ui/app.py` | `duckdb` | latest | Query federada Lambda (httpfs) |
| `src/05_ui/app.py` | `plotly` | latest | Gráficas interactivas |
| `src/05_ui/app.py` | `minio` | latest | Listado Parquet cold path |
| `notebooks/*.ipynb` | `jupyterlab` | latest | Exploración de datos |
| `notebooks/*.ipynb` | `duckdb` | latest | Query RustFS Parquet con httpfs |
| `notebooks/*.ipynb` | `plotly` | latest | Visualización interactiva |
