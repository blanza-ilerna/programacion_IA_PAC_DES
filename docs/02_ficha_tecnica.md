# Ficha Técnica — ILERNA Smart-Industry PAC DES

## Servicios, Tecnologías y Puertos

| Capa          | Servicio            | Tecnología              | Puerto Externo | Puerto Interno | Descripción                                                   |
|---------------|---------------------|-------------------------|:--------------:|:--------------:|---------------------------------------------------------------|
| Generación    | `sensor_simulator`  | Python + Paho-MQTT      | N/A            | N/A            | Emula 5 máquinas enviando temperatura en C, F y K con fallos simulados y hash-chain SHA256. |
| Ingesta       | `mosquitto`         | MQTT Broker v2          | **11883**      | 1883           | Punto de entrada de telemetría industrial. Topic: `sensors/telemetry`. |
| Ingesta       | `mqtt_bridge`       | Python + Confluent-Kafka| N/A            | N/A            | Valida JSON, filtra esquema y publica en Redpanda. Clave Kafka = `device_id`. |
| Streaming     | `redpanda`          | Kafka API compatible    | **19092**      | 9092           | Motor de streaming de baja latencia. Topics: `sensors_raw`, `sensors_clean`, `sensors_verified`, `sensors_invalid`. |
| Streaming     | `redpanda-console`  | Web UI                  | **18080**      | 8080           | Inspección de tópicos y mensajes JSON en tiempo real.         |
| Proceso       | `jobmanager`        | Apache Flink 1.18.1     | **18081**      | 8081           | Orquestador Flink. REST API + UI. Ejecuta los 3 jobs Python.  |
| Proceso       | `taskmanager`       | Apache Flink 1.18.1     | N/A            | N/A            | Worker Flink. Ejecuta las UDFs Python (2 task slots).         |
| Proceso       | `flink_hash_verifier_job` | PyFlink DataStream API | N/A      | N/A            | **A1**: Verifica SHA256 hash-chain por dispositivo. sensors_raw → sensors_verified / sensors_invalid. |
| Proceso       | `flink_normalization_job` | PyFlink Table API  | N/A            | N/A            | **Hito 2**: Normaliza temperatura a °C (UDF to_celsius). sensors_raw → sensors_clean / sensors_invalid. |
| Proceso       | `flink_analytics_job`     | PyFlink Table API  | N/A            | N/A            | **Hito 3**: Tumble Window 1 min. AVG/MAX + alertas > 80°C → InfluxDB. |
| Proceso       | `kafka_to_minio`    | Python + confluent-kafka| N/A            | N/A            | **A2**: Cold path Lambda. sensors_clean → RustFS NDJSON particionado por año/mes/día/hora. Arranca como proceso Python (no Flink). |
| Storage       | `influxdb`          | InfluxDB 2.x            | **18086**      | 8086           | Series temporales. Measurement `machine_stats` (Hot Path).    |
| Storage       | `minio`             | S3 Object Storage       | **19000**      | 9000           | S3 API. Bucket `datalake/clean/` con archivos NDJSON (Cold Path). |
| Storage       | `minio-console`     | RustFS Web UI            | **19001**      | 9001           | Consola de administración de buckets y objetos.               |
| Servicio      | `fastapi`           | Python + Uvicorn        | **18000**      | 8000           | API REST: estado de máquinas, alertas, historial, predict (IsolationForest), model/train. |
| Analítica     | `streamlit`         | Python Framework        | **18501**      | 8501           | Dashboard: tiempo real, historial, alertas, Lambda Query, IA Anomalías, Pipeline Health, Hash Chain, Modelos Avanzados, Ayuda. |
| Analítica     | `duckdb`            | OLAP In-process         | N/A            | N/A            | Motor SQL embebido. Lee NDJSON desde RustFS vía extensión `httpfs` con `hive_partitioning=true`. |
| Visualización | `grafana`           | Grafana 12.x            | **13000**      | 3000           | Dashboards provisioned: Telemetría IoT (InfluxDB) + Estado Pipeline (FastAPI vía Infinity plugin). |
| IDE           | `jupyterlab`        | Jupyter                 | **18888**      | 8888           | Notebooks para exploración de datos y prototipado de IA.      |

---

## Topics Kafka (Redpanda)

| Topic              | Productor                   | Consumidor(es)                                | Descripción                                         |
|--------------------|-----------------------------|-----------------------------------------------|-----------------------------------------------------|
| `sensors/telemetry`| `sensor_simulator`          | `mqtt_bridge`                                 | Telemetría MQTT cruda (C/F/K, fallos posibles). Incluye `hash` y `prev_hash` SHA256. |
| `sensors_raw`      | `mqtt_bridge`               | `flink_hash_verifier_job`, `flink_normalization_job` | JSON validado + metadatos de ingesta. Con campos hash-chain. |
| `sensors_clean`    | `flink_normalization_job`   | `flink_analytics_job`, `kafka_to_minio`       | Temperatura normalizada a °C, sin corruptos. Hot+Cold path. |
| `sensors_invalid`  | `flink_normalization_job`, `flink_hash_verifier_job` | *(monitorización / alertas de calidad)* | **DLQ**: mensajes rechazados con campo `reason`. |
| `sensors_verified` | `flink_hash_verifier_job`   | *(consumidores downstream que requieren integridad)* | **A1**: Mensajes con hash-chain SHA256 verificado. |

---

## Stack Tecnológico

| Categoría        | Tecnología / Librería         | Versión     | Uso en el proyecto                                    |
|------------------|-------------------------------|-------------|-------------------------------------------------------|
| Lenguaje         | Python                        | 3.11+       | Todos los scripts y jobs                              |
| Protocolo IoT    | MQTT v5                       | —           | Ingesta de telemetría de sensores                     |
| Protocolo Stream | Kafka Protocol                | —           | Transporte de mensajes entre etapas                   |
| Stream Engine    | Apache Flink (PyFlink)        | 1.18.1      | Normalización (UDF), ventanas, alertas. 3 jobs Python.|
| Broker Kafka     | Redpanda                      | v25.3.x     | Sustituto de Kafka, compatible con API                |
| Time Series DB   | InfluxDB                      | 2.x         | Almacenamiento de métricas en tiempo real (hot path)  |
| Object Storage   | RustFS                         | latest      | Histórico en NDJSON via S3 API (cold path)            |
| OLAP Engine      | DuckDB                        | latest      | Análisis batch sobre NDJSON desde RustFS con `httpfs`  |
| Web Framework    | FastAPI + Uvicorn             | latest      | API REST de servicio de datos                         |
| Dashboard        | Streamlit                     | latest      | Interfaz de usuario interactiva (9 pestañas)          |
| Visualización    | Plotly                        | latest      | Gráficas interactivas en Streamlit y notebooks        |
| Dashboards Ops   | Grafana                       | 12.x        | Monitorización de infraestructura (provisioned)       |
| Notebooks        | JupyterLab                    | latest      | Exploración y prototipado                             |
| Cliente MQTT     | paho-mqtt                     | ≥ 2.0       | `CallbackAPIVersion.VERSION2` en bridge y simulador   |
| Cliente Kafka    | confluent-kafka               | latest      | Productor/consumidor Python con idempotencia          |
| Cliente InfluxDB | influxdb-client               | latest      | Consultas Flux desde FastAPI y Streamlit              |
| Cliente RustFS    | minio                         | latest      | Operaciones S3 desde `kafka_to_minio.py`              |
| Data Analysis    | pandas                        | latest      | Manipulación de DataFrames en Streamlit y notebooks   |
| ML               | scikit-learn (IsolationForest)| latest      | **A3**: Detección de anomalías no supervisada en FastAPI. `contamination=0.1`, pickle persistence. |
| Infraestructura  | Docker + Docker Compose       | —           | Orquestación de todos los servicios                   |
| Dev Environment  | GitHub Codespaces / DevContainers | —       | Entorno de desarrollo reproducible en la nube         |

---

## Credenciales de acceso

| Servicio  | Usuario | Contraseña      | Token                  |
|-----------|---------|-----------------|------------------------|
| InfluxDB  | admin   | Ilerna_Programaci0n | `supersecrettoken`     |
| RustFS     | admin   | Ilerna_Programaci0n | —                      |
| Grafana   | admin   | Ilerna_Programaci0n | —                      |
| MQTT      | —       | —               | anónimo (sin auth)     |

---

## Descripción Extensa de Tecnologías

---

### Python 3.11

Lenguaje principal del proyecto. Se usa en todos los componentes: simulador, bridge, jobs Flink, API, dashboard y scripts de almacenamiento. Python 3.11 aporta mejoras de rendimiento notables respecto a 3.10 (hasta un 60% más rápido en benchmarks de CPython) y mensajes de error más descriptivos. En este proyecto se usa su tipado estático opcional (`dict`, `list`, `tuple`, `Optional`) para mayor legibilidad sin añadir dependencias externas. La compatibilidad con PyFlink 1.18 requiere exactamente Python 3.x (no 3.12+) dentro del contenedor Flink.

---

### MQTT — Message Queuing Telemetry Transport

Protocolo de mensajería pub/sub diseñado para dispositivos de bajo consumo (IoT). Opera sobre TCP con tres niveles de calidad de servicio:

- **QoS 0** — at-most-once (sin confirmación, máxima velocidad).
- **QoS 1** — at-least-once (confirmación del broker, posibles duplicados). Usado en este proyecto.
- **QoS 2** — exactly-once (handshake de 4 pasos, mayor latencia).

Se eligió **QoS 1** porque garantiza que ningún mensaje de sensor se pierde en tránsito, y los posibles duplicados se eliminan en el lado Kafka con `enable.idempotence=True`. El topic MQTT utilizado es `sensors/telemetry`. Cada mensaje tiene un payload JSON con los campos `device_id`, `temperature`, `unit`, `ts`, `hash`, `prev_hash`.

**Paho-MQTT ≥ 2.0**: cliente Python oficial de la fundación Eclipse. La versión 2.0 introduce `CallbackAPIVersion.VERSION2`, que unifica las firmas de los callbacks (`on_connect`, `on_message`) y elimina comportamientos ambiguos de versiones anteriores. El bridge y el simulador usan esta API explícitamente.

---

### Mosquitto

Broker MQTT open-source de Eclipse, referencia de implementación del protocolo. Configurado sin autenticación (`allow_anonymous true`) y con listener en el puerto 1883 (interno) / 11883 (externo). Mosquitto actúa como intermediario entre los sensores simulados y el bridge Python: el simulador publica en `sensors/telemetry` y el bridge está suscrito a ese topic. Configurado con `persistence false` para maximizar throughput en entorno de desarrollo (los mensajes no sobreviven a un reinicio del broker, lo cual es aceptable porque Redpanda es el sistema de persistencia).

---

### Apache Kafka (protocolo) — Redpanda v25.3.x

**Apache Kafka** es el estándar industrial para la ingesta y distribución de eventos en pipelines de datos en tiempo real. Su modelo es un log distribuido y tolerante a fallos: los mensajes se almacenan en topics con particiones, y los consumidores avanzan por un puntero (offset) sin borrar los mensajes inmediatamente.

**Redpanda** es un broker 100% compatible con la API de Kafka, reescrito en C++ (sin JVM ni ZooKeeper). Ventajas en el contexto de este proyecto:
- **Un solo binario**: sin ZooKeeper ni KRaft aparte, simplifica el despliegue Docker.
- **Menor consumo de RAM**: ~200 MB frente a 512 MB+ de Kafka en mínimos.
- **Misma API**: las librerías `confluent-kafka` y `rpk` funcionan sin cambios.
- **Admin API v1**: endpoint REST en `:9644` para monitorización de brokers, topics y watermarks (usado por el dashboard Grafana vía Infinity plugin).

Configuración relevante en este proyecto:
- Broker interno: `redpanda:29092` (para jobs Flink dentro del contenedor)
- Broker externo: `localhost:19092` (para procesos Python en el workspace)
- Un único nodo (`--smp 1`), un único broker (desarrollo)
- Factor de replicación 1, particiones 1 por topic

---

### Apache Flink 1.18.1 (PyFlink)

Motor de procesamiento de streams distribuido. Flink procesa eventos uno a uno (o por micro-lotes) con garantías de exactamente-una-vez y estado persistente (checkpoints). En este proyecto se usan tres jobs Python:

**PyFlink Table API** (`flink_normalization_job.py`, `flink_analytics_job.py`):
La Table API expone una capa SQL sobre streams, permitiendo escribir transformaciones declarativas. Las UDFs (User-Defined Functions) extienden este SQL con lógica Python personalizada. `to_celsius` es un `ScalarFunction` registrado temporalmente (`create_temporary_function`) que convierte F y K a C.

`StatementSet` permite ejecutar múltiples `INSERT INTO` dentro del mismo job (mismo grafo de ejecución), evitando crear dos jobs separados que consumirían el topic por duplicado.

**PyFlink DataStream API** (`flink_hash_verifier_job.py`):
La DataStream API da acceso de bajo nivel al modelo de operadores. `KeyedProcessFunction` permite procesar elementos con estado keyed: el estado (`ValueState`) se particiona automáticamente por la clave (`device_id`), garantizando que cada cadena de hashes se verifica de forma aislada. Sin este estado keyed, el verifier no sabría cuál fue el hash anterior de cada máquina.

**Tumble Windows** (ventanas de tiempo fijo no solapadas):
En `flink_analytics_job.py`, todos los eventos entre `T` y `T+60s` se agregan y emiten al cerrar la ventana. El watermark de 10 segundos tolera mensajes tardíos: Flink espera hasta 10s de retraso antes de declarar cerrada una ventana.

**Infraestructura Flink en Docker**:
- `jobmanager`: orquestador (REST API en `:8081`, UI web). Distribuye slots de ejecución.
- `taskmanager`: worker con 2 task slots. Ejecuta los operadores de los jobs.
- El `Dockerfile.jobmanager` personalizado pre-instala Python 3, el JAR `flink-sql-connector-kafka`, y extrae `pyflink.zip` en el classpath para que las UDFs Python sean accesibles desde el TaskManager.

**Nota de limitación**: El conector FileSystem de Flink para S3/RustFS requería JARs de Hadoop (`hadoop-aws`, `aws-java-sdk`) que producían conflictos de classpath con Flink 1.18 + Java 17. El cold path se reemplazó por un proceso Python independiente (`kafka_to_minio.py`), obteniendo el mismo resultado funcional sin dependencias de Hadoop.

---

### InfluxDB 2.x

Base de datos de series temporales (TSDB) optimizada para escrituras de alta frecuencia con timestamps. Diferencias clave respecto a una BD relacional:

- **Modelo columnar comprimido**: almacena campos numéricos con compresión delta + Gorilla (float), reduciendo el espacio hasta 10×.
- **Tags vs Fields**: los `tags` son strings indexados (usados en filtros frecuentes, como `device_id`); los `fields` son valores numéricos no indexados. En `machine_stats`: `device_id` es tag, `avg_temp_c`, `max_temp_c`, `count`, `alert` son fields.
- **Line Protocol**: formato de escritura en texto plano eficiente: `measurement,tag=val field=val timestamp_ns`.
- **Lenguaje Flux**: DSL funcional propio de InfluxDB 2.x para consultas. Más expresivo que InfluxQL: permite `pivot()`, `join()`, funciones de ventana y agregaciones complejas. En este proyecto Flink escribe vía HTTP Line Protocol usando `urllib.request` (sin dependencias externas en el contenedor Flink).

Organización: bucket `sensores`, org `ilerna`, token `supersecrettoken`. Retención: infinita (configuración de desarrollo).

---

### RustFS

Servidor de object storage compatible con la API S3 de Amazon. Almacena los archivos del cold path (NDJSON particionados). Ventajas en este contexto:

- **API S3 estándar**: cualquier librería que soporte S3 (boto3, minio-py, DuckDB httpfs) se conecta sin cambios.
- **Despliegue ligero**: binario único en Docker, sin dependencias externas.
- **Particionado Hive**: los archivos se organizan en rutas como `clean/year=2026/month=03/day=11/hour=14/1741693200.json`. Este esquema permite a DuckDB (y herramientas como Spark o Athena) hacer *partition pruning* — leer solo los directorios que coincidan con el filtro de la query, evitando escanear todo el dataset.

El bucket `datalake` se crea automáticamente por `init_pipeline.sh`. Los archivos NDJSON aparecen cada 60 segundos a medida que `kafka_to_minio.py` cierra su ventana de acumulación.

---

### DuckDB

Motor SQL analítico embebido (in-process). No requiere servidor: se instancia como librería Python en el mismo proceso de Streamlit o en los notebooks. Rol clave en la Arquitectura Lambda:

- **Extensión `httpfs`**: permite acceder a archivos en S3/RustFS directamente via HTTP sin descargarlos a disco. Configurado con el endpoint interno `minio:9000`.
- **`read_json()`**: lectura directa de NDJSON con esquema explícito (`columns = {device_id: 'VARCHAR', ...}`) para evitar inferencia errónea cuando los archivos históricos tienen estructuras ligeramente distintas.
- **`hive_partitioning=true`**: DuckDB detecta automáticamente las columnas de partición (`year`, `month`, `day`, `hour`) a partir de los nombres de directorio y las expone como columnas filtrables.
- **Query federada**: `conn.register("hot", df_influx)` registra un DataFrame pandas como tabla virtual. El `UNION ALL` combina datos de InfluxDB y RustFS en memoria sin copiar datos a ningún sistema externo.

---

### FastAPI + Uvicorn

**FastAPI** es un framework web Python asíncrono basado en ASGI. Características relevantes:

- **Pydantic v2**: validación automática de tipos en los modelos de entrada/salida. Los schemas JSON de los endpoints se generan automáticamente.
- **OpenAPI / Swagger**: documentación interactiva disponible en `/docs` y `/redoc` sin configuración adicional.
- **Async/await**: los handlers son funciones `async def`, permitiendo manejar múltiples requests concurrentes en el mismo proceso sin bloquear el event loop durante las queries a InfluxDB.
- **Startup events**: el modelo IsolationForest se carga desde pickle (`/tmp/isolation_forest.pkl`) en el evento `startup`, manteniéndolo en memoria para predicciones de baja latencia.

**Uvicorn** es el servidor ASGI de referencia. Configurado con `--reload` en desarrollo (hot-reload al editar `main.py`) y `--host 0.0.0.0` para aceptar conexiones desde otros contenedores y desde el host en Codespaces.

Endpoints implementados: `GET /health`, `GET /machines/status`, `GET /machines/{id}`, `GET /machines/{id}/predict`, `GET /alerts`, `GET /stats`, `POST /model/train`, `GET /model/status`, `POST /machines/publish`.

---

### Streamlit

Framework Python para crear aplicaciones web de datos sin HTML/CSS/JS. Cada vez que el usuario interactúa, el script Python se re-ejecuta de arriba a abajo (modelo reactivo). El estado entre ejecuciones se gestiona con `st.session_state`.

El dashboard tiene **9 pestañas**:
1. **Tiempo Real** — gauges con `st.metric` + refresco automático cada 5s via `st_autorefresh`.
2. **Historial** — serie temporal de `avg_temp_c` por `device_id` con Plotly `go.Scatter`.
3. **Alertas** — scatter plot de eventos `alert=1` con `plotly.express.scatter`.
4. **Lambda Query** — DuckDB UNION ALL entre InfluxDB y RustFS, con botón de ejecución manual.
5. **IA Anomalías** — entrenamiento de IsolationForest vía `POST /model/train`, predictor interactivo con `st.slider`.
6. **Pipeline Health** — estado de jobs Flink (API Flink REST), topics Kafka y servicios.
7. **Hash Chain** — mensajes verificados/inválidos del DLQ con estadísticas de integridad.
8. **Modelos Avanzados** — Prophet (predicción temporal), RandomForest, CUSUM, K-Means.
9. **Ayuda** — arquitectura del sistema, endpoints, comandos de arranque, tabla de puertos.

---

### Plotly

Librería de visualización interactiva para Python. Genera gráficas en HTML/JavaScript (Plotly.js) renderizadas en el navegador. En este proyecto se usa `plotly.graph_objects` (API de bajo nivel, máximo control) y `plotly.express` (API de alto nivel, creación rápida). Ventaja sobre Matplotlib: las gráficas son interactivas (zoom, hover, selección de series) sin código adicional, lo que es especialmente útil en Streamlit.

---

### Grafana 12.x

Plataforma de observabilidad open-source. En este proyecto se usan dos dashboards provisioned (cargados automáticamente desde `config/grafana/provisioning/`):

**Telemetría IoT** (pipeline.json):
- 7 paneles conectados a InfluxDB via datasource `influxdb` (Flux mode).
- Queries Flux con rangos literales (`-3h`) en lugar de variables de dashboard (`v.timeRangeStart`), ya que las variables de template no se inyectan en dashboards provisioned.
- Cada target requiere `"queryType": "flux"` explícito en Grafana 12 para que el plugin no intente usar InfluxQL (modo legacy).
- Paneles: stat (máquinas activas, alertas, temp media/máxima), timeseries (evolución temporal), gauge (temperatura actual por máquina), table (estado con alerta coloreada).

**Redpanda / Kafka** (redpanda.json):
- 6 paneles conectados al Admin API de Redpanda (`:9644`) vía **Infinity plugin** (`yesoreyeram-infinity-datasource`).
- `parser: backend` fuerza que Grafana fetchee las URLs server-side (desde el contenedor Grafana hacia `redpanda:9644` en la red Docker interna), necesario porque las URLs no son públicas.
- Paneles: brokers activos, topics del pipeline, estado del cluster, detalle de brokers, lista de topics, watermark de mensajes.

**FastAPI** (fastapi.json):
- 6 paneles conectados a `http://workspace:8000/...` vía Infinity plugin.
- Muestra estado de máquinas, alertas recientes y estadísticas en tiempo real.

**Provider**: `updateIntervalSeconds: 30` — Grafana recarga los archivos JSON de disco cada 30 segundos. No requiere reinicio para cambios en los dashboards.

---

### JupyterLab

Entorno de notebooks interactivos. Permite mezclar código Python, visualizaciones Plotly y texto Markdown en un único documento. En este proyecto se usan para exploración ad-hoc y para demostrar el pipeline completo sin necesitar desplegar la UI de Streamlit. El notebook `01_exploracion_datos.ipynb` cubre: hot path (InfluxDB), cold path (RustFS NDJSON + DuckDB), Lambda Query, IsolationForest y verificación de hash-chain. El notebook `02_analisis_calidad.ipynb` cubre análisis de calidad de datos: distribución de unidades, integridad de hash-chain, completitud del cold path y comparación local vs DLQ de Flink.

---

### confluent-kafka (Python)

Cliente oficial de Confluent para la API de Kafka, implementado en C (librdkafka) con bindings Python. Es el cliente de referencia por rendimiento y por soportar todas las funcionalidades de la API Kafka:

- **Productor** (`mqtt_to_redpanda_bridge.py`): `enable.idempotence=True` garantiza entrega exactly-once desde el bridge. La clave Kafka es `device_id`, lo que concentra los mensajes de cada máquina en la misma partición y preserva el orden de llegada por dispositivo.
- **Consumidor** (`kafka_to_minio.py`): `group.id` único, `auto.offset.reset=earliest` para no perder mensajes históricos, commit manual de offsets solo tras escritura exitosa en RustFS (semántica at-least-once con idempotencia en RustFS).

---

### influxdb-client (Python)

SDK oficial de InfluxDB 2.x para Python. En `src/api/main.py` y `src/05_ui/app.py` se usa para ejecutar queries Flux y recibir los resultados como `pandas.DataFrame` (via `query_data_frame()`). Cada request a la API crea un `InfluxDBClient` efímero que se cierra en el bloque `finally`, evitando conexiones colgadas.

**Nota**: El job Flink de analytics **no usa** esta librería — escribe directamente con `urllib.request` de la stdlib Python para evitar dependencias externas en el entorno Flink (classpath controlado, sin pip). El endpoint es `POST /api/v2/write` con body en formato Line Protocol.

---

### minio (Python client)

Cliente oficial de RustFS para Python, compatible con AWS S3. En `kafka_to_minio.py` se usa para subir archivos NDJSON con `put_object()`. En los notebooks se usa para listar objetos con `list_objects()` y verificar la existencia del bucket. La librería maneja internamente la autenticación por firma HMAC-SHA256 (compatible con AWS Signature V4).

---

### pandas

Librería de análisis de datos estructurados en Python. DataFrames bidimensionales con tipos heterogéneos. En este proyecto:
- `influxdb-client` devuelve resultados de Flux como `pd.DataFrame`.
- DuckDB puede registrar DataFrames pandas como tablas virtuales (`conn.register()`).
- Streamlit renderiza tablas directamente desde DataFrames.
- Los notebooks usan pandas para manipulación, agrupación y estadísticas sobre los datos del pipeline.

---

### scikit-learn — IsolationForest

Librería de Machine Learning para Python, referencia en algoritmos clásicos (supervisados y no supervisados). **IsolationForest** es el algoritmo usado para la detección de anomalías:

**Funcionamiento**: construye un ensemble de árboles de decisión aleatorios. En cada árbol, selecciona aleatoriamente un feature y un valor de split. Los puntos anómalos (estadísticamente raros) requieren pocas particiones para ser aislados completamente — su "longitud de camino" en el árbol es corta. Los puntos normales necesitan más splits. El `anomaly_score` normalizado es más negativo cuanto más anómalo es el punto.

**Parámetros usados**:
- `n_estimators=100`: 100 árboles en el ensemble.
- `contamination=0.1`: se asume que el 10% de los datos de entrenamiento son anómalos. Esto calibra el umbral de decisión.
- `random_state=42`: reproducibilidad del modelo.

**Flujo en este proyecto**:
1. `POST /model/train?range_minutes=60` — lee histórico de InfluxDB, entrena el modelo, serializa a `/tmp/isolation_forest.pkl`.
2. `GET /machines/{id}/predict?temperature_c=X` — carga el modelo, calcula `score_samples([[X]])`, normaliza a `failure_prob ∈ [0,1]`, devuelve `is_anomaly` + `interpretation`.

**Diferencia con la detección Edge de Flink**: Flink detecta `avg_temp_c > 80°C` (regla fija, determinista, milisegundos). IsolationForest detecta comportamiento estadísticamente raro para cada máquina, sin umbral fijo, aprendiendo del histórico real.

---

### Docker + Docker Compose

Docker encapsula cada servicio en un contenedor con sus dependencias, garantizando reproducibilidad independientemente del sistema operativo del host. Docker Compose orquesta los múltiples contenedores del stack, definiendo redes, volúmenes, healthchecks y orden de arranque.

Red interna `ilerna_ia_big_data`: todos los servicios se comunican por nombre de hostname (`mosquitto`, `redpanda`, `influxdb`, `minio`, `jobmanager`, `workspace`). Los puertos externos en rango `10000+` evitan conflictos con servicios del host en Codespaces.

Volúmenes nombrados para persistencia: `redpanda-data`, `influxdb-data`, `minio-data`, `grafana-data`. Los datos sobreviven a reinicios de contenedores pero se pierden con `docker volume prune`.

Healthchecks: cada servicio crítico tiene un `healthcheck` configurado. Docker Compose usa `depends_on: condition: service_healthy` para arrancar servicios en el orden correcto (por ejemplo, Flink espera a que Redpanda esté healthy antes de lanzar jobs).

---

### GitHub Codespaces / DevContainers

GitHub Codespaces proporciona una máquina virtual en la nube (2 cores / 8 GB RAM en plan gratuito) con VS Code en el navegador. El entorno se define completamente en `.devcontainer/devcontainer.json`:

- `"postStartCommand": "bash .devcontainer/start.sh"` — levanta todos los contenedores Docker automáticamente al abrir el Codespace.
- `"forwardPorts"`: declara explícitamente qué puertos exponer, con visibilidad pública o privada.
- `"features"`: instala herramientas adicionales (Docker-in-Docker) sin modificar la imagen base.

La especificación DevContainers es un estándar abierto (también soportado por VS Code local con Docker Desktop), lo que hace el entorno reproducible en cualquier máquina, no solo en Codespaces.

**Limitación de recursos**: con 2 cores y 8 GB RAM compartidos entre todos los servicios (jobmanager 560 MB, taskmanager 400 MB, redpanda 300 MB, influxdb 400 MB, workspace 2 GB+, grafana, minio...), la memoria es el recurso crítico. Los 3 jobs PyFlink lanzan procesos Python UDF adicionales al ejecutarse. Si el sistema queda sin memoria, el OOM killer puede terminar el TaskManager. La solución es ejecutar `flink-restart` (alias definido por `init_pipeline.sh`) para relanzar los jobs tras el reinicio automático del TaskManager.

---

### SHA256 Hash-Chaining

No es una librería externa: usa `hashlib` de la stdlib Python. La cadena de hashes funciona de forma similar a la estructura de bloques en blockchain:

```
hash_0 = "0" × 64  (génesis)
hash_1 = SHA256(json(msg_1, sort_keys=True) + hash_0)
hash_2 = SHA256(json(msg_2, sort_keys=True) + hash_1)
hash_n = SHA256(json(msg_n, sort_keys=True) + hash_{n-1})
```

Si cualquier mensaje intermedio se modifica, su hash recalculado difiere del que declaró, y todos los mensajes posteriores fallan la verificación de `prev_hash`. El campo `prev_hash` en cada mensaje es el `hash` del mensaje anterior de la **misma máquina** (cadena independiente por `device_id`). El verifier Flink mantiene el último hash válido en `ValueState` keyed por `device_id`.

---

## Notas de configuración para Codespaces

**Red interna Docker**: Los servicios se comunican entre sí por nombre de servicio:
- `mosquitto:1883`, `redpanda:29092`, `influxdb:8086`, `minio:9000`, `jobmanager:8081`

**Acceso externo desde el host/Codespaces**: Puertos mapeados al rango `10000+` para evitar conflictos:
- Los scripts Python ejecutados en el `workspace` deben usar `localhost:<PUERTO_EXTERNO>`
- Los jobs Flink ejecutados en el contenedor `jobmanager` deben usar los nombres internos

**Autodetección de puertos en Codespaces**: Configurado `"autoForwardPorts": false` en `devcontainer.json`. Solo se exponen los puertos declarados explícitamente en `forwardPorts`.

**Persistencia de datos**: Volúmenes Docker nombrados para Redpanda, InfluxDB, RustFS y Grafana. Los datos sobreviven reinicios de contenedores pero se pierden si se ejecuta `docker volume prune`.

**Flink y Python**: El contenedor `flink:1.18.1-java17` necesita Python 3 y el JAR Kafka SQL Connector para ejecutar jobs PyFlink. `Dockerfile.jobmanager` pre-instala ambos, además de extraer `pyflink.zip` para que los UDFs Python puedan ejecutarse desde el TaskManager.

**Cold path (Python)**: `kafka_to_minio.py` se ejecuta como proceso Python independiente (no Flink). Arranca automáticamente con `init_pipeline.sh`. Escribe un archivo NDJSON cada 60 segundos en `s3://datalake/clean/year=.../month=.../day=.../hour=.../`.

**Grafana dashboards**: Provisioned automáticamente desde `config/grafana/provisioning/`. Las queries Flux usan rangos literales (`-3h`) en lugar de variables de Grafana (`v.timeRangeStart`) que no se inyectan en dashboards provisioned.

---

## Aportaciones Avanzadas (resumen técnico)

| ID | Nombre                     | Archivos clave                                               | Descripción técnica                                                                                  |
|----|----------------------------|--------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| A1 | Hash-Chain SHA256          | `sensor_simulator.py`, `flink_hash_verifier_job.py`, `src/processing/hash_chain.py` | El simulador encadena mensajes con SHA256 (prev_hash→hash). Flink verifica la cadena con `ValueState` keyed por device_id. Mensajes íntegros → `sensors_verified`, tampered → `sensors_invalid`. |
| A2 | Lambda Architecture JSON   | `src/03_storage/kafka_to_minio.py`, `src/05_ui/app.py`      | Cold path: proceso Python consume `sensors_clean` y escribe NDJSON con particionado Hive en RustFS. DuckDB en Streamlit consulta ambos paths con `UNION ALL`. Ventana de escritura: 60 segundos. |
| A3 | IsolationForest ML         | `src/api/main.py` (FastAPI), `src/05_ui/app.py`             | Singleton `AnomalyDetector` con `IsolationForest(contamination=0.1)`. Entrena con histórico de InfluxDB. Devuelve `is_anomaly`, `failure_prob ∈ [0,1]`, `interpretation`. Pickle persistence en `/tmp/`. |
