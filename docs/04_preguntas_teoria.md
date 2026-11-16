# Preguntas de Teoría — ILERNA Smart-Industry PAC DES

Preguntas y respuestas organizadas por bloque temático, cubriendo los conceptos clave del pipeline.

---

## 1. Arquitectura general

**¿Qué es la Arquitectura Lambda y cómo se aplica en este proyecto?**

La Arquitectura Lambda combina dos paths de procesamiento:
- **Hot path** (baja latencia): Flink → InfluxDB → consultas en segundos. Sirve para monitorización en tiempo real y alertas.
- **Cold path** (alta escala): Flink/Python → RustFS JSON → consultas históricas. Sirve para análisis mensual y entrenamiento ML.
- **Serving layer**: DuckDB ejecuta un `UNION ALL` en memoria combinando ambas fuentes sin mover datos.

---

**¿Por qué se usa Redpanda en lugar de Kafka?**

Redpanda es compatible con la API de Kafka pero es más ligero (binario único en C++, sin JVM ni ZooKeeper). Para un entorno de desarrollo como Codespaces reduce el consumo de memoria y simplifica el despliegue, manteniendo la misma interfaz (`rpk`, `confluent_kafka`).

---

**¿Qué rol cumple cada topic Kafka?**

| Topic | Contenido |
|---|---|
| `sensors_raw` | Mensajes crudos validados por el bridge (temperature, unit, ts) |
| `sensors_clean` | Datos normalizados a Celsius por Flink (temperature_c, unit_original) |
| `sensors_invalid` | DLQ: mensajes con schema inválido o hash roto |
| `sensors_verified` | Mensajes con hash-chain verificado (integridad confirmada) |

---

## 2. Ingesta (Hito 1)

**¿Qué validaciones hace `mqtt_to_redpanda_bridge.py` antes de publicar en Kafka?**

1. Parsea JSON del payload MQTT.
2. Verifica que existan los campos `device_id`, `temperature`, `unit`, `ts`.
3. Valida que `unit` sea uno de `{C, F, K}`.
4. Enriquece el mensaje con `_mqtt_topic` e `_ingested_at`.
5. Publica en `sensors_raw` con `enable.idempotence=True` y clave Kafka = `device_id`.

---

**¿Para qué sirve `enable.idempotence=True` en el productor Kafka?**

Garantiza que en caso de reintento por fallo de red el broker no registre el mismo mensaje dos veces. El productor asigna un ID de secuencia; si el broker ya recibió ese ID, descarta el duplicado. Evita que un reintento ante un ACK perdido genere mensajes duplicados en el topic.

---

**¿Por qué se usa QoS 1 en MQTT y cómo se relaciona con la idempotencia de Kafka?**

MQTT QoS 1 garantiza entrega *at-least-once*: el broker confirma la recepción pero puede haber reenvíos. Combinado con `enable.idempotence=True` en el lado Kafka se consigue un pipeline *effectively-once*: MQTT puede reenviar pero Kafka no duplica.

---

## 3. Normalización con Flink (Hito 2)

**¿Qué hace `flink_normalization_job.py`?**

Consume `sensors_raw`, aplica una UDF `to_celsius` para convertir F→C y K→C, añade el campo `unit_original` y envía los resultados a `sensors_clean`. Los mensajes con campos inválidos se desvían al DLQ `sensors_invalid` mediante `StatementSet` (dos sinks en el mismo job).

---

**¿Cuáles son las fórmulas de conversión de temperatura?**

- Fahrenheit a Celsius: `(F - 32) × 5/9`
- Kelvin a Celsius: `K - 273.15`
- Celsius: sin cambio.

---

**¿Qué es una UDF en Flink Table API?**

Una *User-Defined Function* que extiende el SQL de Flink con lógica personalizada. Se registra con `table_env.create_temporary_function("nombre", ClasePython)`. En este proyecto `to_celsius` es un `ScalarFunction` que recibe `temperature` y `unit` y devuelve `temperature_c`.

---

**¿Qué es un `StatementSet` y para qué se usa aquí?**

Permite ejecutar varios `INSERT INTO` en el mismo pipeline Flink. En lugar de abrir dos jobs separados, el normalizador envía datos válidos a `sensors_clean` e inválidos a `sensors_invalid` en un único job con un solo plan de ejecución, reduciendo recursos.

---

## 4. Analítica con Flink (Hito 3)

**¿Qué es una Tumble Window y por qué se usa una de 1 minuto?**

Una ventana de tiempo fija no solapada. Todos los eventos que llegan entre `T` y `T+60s` se agrupan y se emite un resultado al cerrar la ventana. Se usa 1 minuto porque es un compromiso entre latencia (suficientemente rápido para detectar alertas) y agregación (suficientes muestras para calcular una media fiable).

---

**¿Qué es un watermark y para qué sirve el de 10 segundos?**

Un watermark es una marca de progreso temporal que indica al motor de streaming hasta qué punto del tiempo de evento puede considerar que ya han llegado todos los datos. Un watermark de 10s significa que Flink espera hasta 10 segundos de retraso antes de cerrar una ventana, tolerando mensajes desordenados hasta ese margen.

---

**¿Qué métricas calcula `flink_analytics_job.py` por ventana?**

Por cada (`device_id`, ventana de 1 min):
- `avg_temp_c` — temperatura media en Celsius
- `max_temp_c` — temperatura máxima
- `count` — número de lecturas
- `alert` — `1` si `avg_temp_c > 80°C`, `0` en caso contrario

Escribe en InfluxDB measurement `machine_stats`.

---

**¿Por qué el Flink UI muestra "Records Received: 0" aunque el job esté procesando datos?**

Es un bug conocido del Beam runner de PyFlink 1.18: las métricas de registros de la UI no se actualizan correctamente para jobs Python. No indica un problema real; para verificar el funcionamiento hay que consultar directamente InfluxDB o el topic `sensors_clean`.

---

## 5. Almacenamiento (InfluxDB y RustFS)

**¿Por qué se usa InfluxDB y no una base de datos relacional para el hot path?**

InfluxDB es una base de datos de series temporales optimizada para escrituras de alta frecuencia con timestamps. Ofrece retención automática, compresión nativa de datos temporales y el lenguaje Flux para consultas sobre rangos de tiempo. Una base relacional requeriría índices especiales y tendría peor rendimiento en consultas tipo `range(start: -5m)`.

---

**¿Qué es el Line Protocol de InfluxDB?**

Formato de texto plano para escribir datos:
```
measurement,tag1=v1,tag2=v2 field1=v1,field2=v2 timestamp_ns
```
Por ejemplo:
```
machine_stats,device_id=machine-001 avg_temp_c=72.3,alert=0i 1710768000000000000
```

---

**¿Qué es el particionado Hive en RustFS y para qué sirve?**

Organizar los ficheros en directorios con nombre `columna=valor`:
```
clean/year=2026/month=03/day=18/hour=14/archivo.json
```
DuckDB (y herramientas como Spark, Athena) pueden leer solo las particiones necesarias sin escanear todos los ficheros, reduciendo I/O. Se usa `hive_partitioning=true` en la query DuckDB.

---

**¿Por qué se eligió `kafka_to_minio.py` (Python) en lugar de un job Flink para el cold path?**

El conector FileSystem de Flink + plugin S3/Hadoop requiere JARs de Hadoop que presentaban conflictos de classpath en el entorno (Flink 1.18 + Java 17). El proceso Python alternativo (`kafka_to_minio.py`) consume `sensors_clean` directamente con `confluent_kafka`, acumula mensajes en ventanas de 60s y los escribe en RustFS como NDJSON con particionado Hive. DuckDB los lee con `read_json()` y `hive_partitioning=true`, obteniendo exactamente el mismo resultado funcional.

**¿Por qué se eligió NDJSON en lugar de Parquet para el cold path Python?**

NDJSON (un JSON por línea) no requiere dependencias externas para escribir: basta con `json.dumps()` por registro y la librería `minio` para subir el archivo. Parquet requeriría `pyarrow`, que añade complejidad y peso. DuckDB lee NDJSON directamente con `read_json()` con esquema explícito y particionado Hive, con rendimiento suficiente para el volumen de este proyecto.

---

## 6. API REST (Hito 4)

**¿Qué endpoints expone FastAPI y para qué sirve cada uno?**

| Endpoint | Descripción |
|---|---|
| `GET /health` | Estado de la API y conectividad InfluxDB |
| `GET /machines/status` | Última lectura normalizada de cada máquina + alerta |
| `GET /machines/{id}` | Historial de temperatura de una máquina |
| `GET /machines/{id}/predict` | Predicción de anomalía con IsolationForest |
| `GET /alerts` | Ventanas donde avg > umbral |
| `GET /stats` | Estadísticas globales (media, min, max) |
| `POST /model/train` | Entrena IsolationForest con datos históricos |
| `POST /machines/publish` | Publica lectura manual vía MQTT |

---

**¿Cómo consulta FastAPI los datos de InfluxDB?**

Usa `influxdb_client` (SDK oficial) con queries en lenguaje Flux. Cada request crea un cliente, ejecuta la query y cierra la conexión en el bloque `finally`. Ejemplo de query: `from(bucket:"sensores") |> range(start: -5m) |> filter(fn: (r) => r._measurement == "machine_stats")`.

---

## 7. Detección de Anomalías (IA)

**¿Cuál es la diferencia entre la detección de Flink y la del modelo ML?**

| | Flink (Edge) | IsolationForest (Cloud) |
|---|---|---|
| Tipo | Regla fija | Modelo estadístico |
| Criterio | `avg_temp_c > 80°C` | Comportamiento estadísticamente raro |
| Latencia | Segundos (streaming) | Bajo demanda (REST) |
| Entrenamiento | No necesita | Requiere datos históricos |
| Ventaja | Determinista, sin modelo | Detecta anomalías sutiles sin umbral fijo |

---

**¿Qué es IsolationForest y cómo funciona?**

Algoritmo de detección de outliers no supervisado. Construye árboles de decisión aleatorios y mide cuántas particiones necesita para aislar cada punto. Los puntos anómalos se aíslan con pocas particiones (caminos cortos), los normales requieren más. El `score` resultante es más negativo cuanto más anómalo es el punto.

---

**¿Qué significa el parámetro `contamination`?**

La fracción esperada de anomalías en el conjunto de entrenamiento. Con `contamination=0.1` se espera que el 10% de los datos sean anómalos. Ajusta el umbral de decisión del modelo: un valor más alto hace al modelo más permisivo (menos falsos positivos pero más falsos negativos).

---

## 8. Query Federada (DuckDB)

**¿Qué es DuckDB y qué rol tiene en la Arquitectura Lambda?**

DuckDB es un motor SQL analítico embebido (sin servidor). En este proyecto actúa como capa de consulta unificada: ejecuta un `UNION ALL` entre datos de InfluxDB (hot) y RustFS (cold) en memoria, sin necesidad de copiar datos a ningún lado. Usa la extensión `httpfs` para leer ficheros JSON directamente desde RustFS via S3.

---

**¿Qué ventaja tiene usar DuckDB para la query federada frente a copiar datos a una sola BD?**

- No hay movimiento de datos → menor latencia y sin duplicación.
- Funciona con fuentes heterogéneas (time-series + object storage).
- El `UNION ALL` se ejecuta en memoria en el servidor de la API.
- Escala horizontalmente: cada fuente puede crecer independientemente.

---

## 9. Seguridad e Integridad

**¿Qué es el hash-chaining SHA256 del simulador?**

Cada mensaje lleva un campo `hash` calculado como `SHA256(hash_anterior + payload_actual)`. Forma una cadena: si se modifica cualquier mensaje intermedio, el hash de todos los posteriores es inválido. `flink_hash_verifier_job.py` verifica la cadena por `device_id` y envía mensajes corruptos al DLQ `sensors_invalid`.

---

**¿Qué diferencia hay entre `sensors_invalid` y `sensors_verified`?**

- `sensors_invalid` (DLQ): mensajes rechazados — schema incorrecto o hash roto. Sirve para auditoría.
- `sensors_verified`: mensajes con hash-chain validado — integridad confirmada.

---

## 10. Infraestructura y DevOps

**¿Qué hace `init_pipeline.sh` al ejecutarlo?**

1. Crea los 4 topics Kafka en Redpanda (`sensors_raw`, `sensors_clean`, `sensors_verified`, `sensors_invalid`).
2. Crea el bucket `datalake` en RustFS.
3. Lanza los 3 jobs Flink (si no están ya corriendo): normalization, hash_verifier, analytics.
4. Arranca `kafka_to_minio.py` como proceso Python en background (cold path).
5. Añade aliases de desarrollo a `~/.bashrc` (`sim`, `bridge`, `api`, `ui`, `flink-jobs`, `minio-writer`, etc.).

---

**¿Por qué el TaskManager de Flink puede caerse en Codespaces?**

Codespaces de 2 cores / 8GB RAM ejecuta simultáneamente: jobmanager (560MB), taskmanager (400MB), workspace (2GB+), redpanda, influxdb, minio, grafana... Con memoria justa, el sistema puede OOM-killar el TaskManager. Cada job PyFlink lanza procesos Python UDF adicionales. Con 3 jobs (en lugar de 4) la presión de memoria es menor. La solución cuando caen es ejecutar `flink-restart` para relanzar los jobs tras el reinicio automático del TaskManager.

---

**¿Cuáles son los puertos de los servicios y cómo se accede a cada uno?**

| Puerto | Servicio | Uso |
|---|---|---|
| 11883 | Mosquitto (MQTT) | Publicar/suscribir mensajes de sensores |
| 13000 | Grafana | Dashboards de monitorización |
| 18000 | FastAPI | API REST del pipeline |
| 18080 | Redpanda Console | UI del broker Kafka |
| 18081 | Flink UI | Monitorización de jobs Flink |
| 18086 | InfluxDB | Base de datos de series temporales |
| 18501 | Streamlit | Dashboard interactivo |
| 18888 | JupyterLab | Notebooks de análisis |
| 19000 | RustFS S3 API | Acceso programático al datalake |
| 19001 | RustFS Console | UI web de RustFS |
| 19092 | Redpanda Kafka API | Producers/consumers Kafka |

---

## 11. Preguntas de concepto rápido

**¿Qué es un Consumer Group en Kafka?**


**¿Qué es el offset en Kafka?**


**¿Qué diferencia hay entre `earliest` y `latest` offset?**
- `earliest`: el consumidor empieza desde el primer mensaje disponible (procesa histórico).
- `latest`: solo procesa mensajes nuevos a partir del momento de suscripción.

**¿Qué es un DLQ (Dead Letter Queue)?**


**¿Qué es el watermark en streaming y por qué importa en ventanas?**


**¿Qué ventaja tiene el formato NDJSON frente a JSON array?**

---



**¿Por qué se usa `sort_keys=True` en `json.dumps()` al calcular el hash SHA256 en el simulador? ¿Qué ocurriría si se omite?**

---

**¿Qué ocurre si el simulador arranca antes que el hash verifier de Flink? ¿Por qué los mensajes van todos al DLQ y cómo se soluciona?**

---

**¿Cuál es la diferencia entre semántica `at-least-once` y `exactly-once` en este pipeline? ¿Qué componente aporta cada garantía?**

---

**¿Por qué `device_id` se usa como clave de partición en el productor Kafka del bridge? ¿Qué propiedad garantiza esto sobre el orden de mensajes?**

---

**¿Qué es un `KeyedProcessFunction` en la DataStream API de Flink y por qué es la abstracción correcta para el hash verifier en lugar de una función `map` simple?**

---

**¿Qué son los checkpoints de Flink y cómo permiten recuperar el estado del hash verifier si el TaskManager cae?**

---

**¿Por qué el job de analytics usa `urllib.request` (stdlib) para escribir en InfluxDB en lugar del SDK `influxdb-client`?**

---

**¿Qué diferencia hay entre `|> last()` y `|> mean()` en Flux? ¿Cuándo es apropiado cada uno para consultar `machine_stats`?**

---

**¿Qué impacto tiene `replication_factor=1` en Redpanda sobre la durabilidad de los datos? ¿Qué cambiaría en producción?**

---

**¿Por qué `queryType: "flux"` es necesario en cada target de los paneles de Grafana 12 aunque el datasource ya esté configurado con `version: Flux`?**

---

**¿Qué es `partition pruning` en DuckDB y cómo lo habilita el particionado Hive de RustFS? ¿Qué columnas de la query deben coincidir para aprovecharlo?**

---

**¿Qué significa `contamination=0.1` en IsolationForest? ¿Qué efecto tiene subir ese valor a `0.5` sobre los falsos positivos y falsos negativos?**

---

**¿Cuál es la diferencia entre el módulo `src/processing/validators.py` y las validaciones dentro de `flink_normalization_job.py`? ¿En qué contextos se usa cada uno?**

---

**¿Por qué `kafka_to_minio.py` hace commit de offsets solo tras una escritura exitosa en RustFS y no automáticamente? ¿Qué problema evita?**

---

**Si una máquina envía un mensaje con temperatura en Kelvin absoluto cero (0K), ¿en qué punto del pipeline se rechaza y por qué? Nombra el componente y la razón exacta.**

---


**¿Qué hace exactamente `source .devcontainer/init_pipeline.sh` que no haría `bash .devcontainer/init_pipeline.sh`? ¿Por qué los aliases no quedarían disponibles en el shell actual si se usa `bash`?**

---

**`kafka_to_minio.py` arranca automáticamente con `init_pipeline.sh` usando `nohup ... &`. ¿Qué significa cada una de esas dos partes y qué ocurre si se omite alguna de ellas?**

---

**¿Cómo verificarías desde la línea de comandos que `kafka_to_minio.py` está corriendo, cuántos archivos ha escrito en RustFS y qué decía su última línea de log? Escribe los tres comandos.**

---

**¿Por qué `init_pipeline.sh` comprueba si hay exactamente 3 jobs RUNNING antes de lanzarlos? ¿Qué problema evita esa comprobación si se ejecuta el script dos veces seguidas?**

---

**El `minio-writer` usa `consumer.commit()` manual. ¿Qué ocurriría si el proceso se mata justo después de subir un archivo a RustFS pero antes del commit? ¿Y si se mata justo después del commit pero antes de subir?**

---

**`nohup python kafka_to_minio.py > /tmp/minio_writer.log 2>&1 &` — ¿qué significa `2>&1`? ¿Dónde irían los mensajes de error sin esa redirección?**

---

**¿Qué diferencia hay entre `pgrep -f kafka_to_minio` y `pgrep kafka_to_minio`? ¿Por qué en este caso se necesita `-f`?**

---

**En el `flink_analytics_job.py`, la alerta se genera cuando `avg_temp_c > 80`. ¿Qué ventaja tiene calcular la alerta en Flink frente a calcularla en la query Flux de FastAPI o Grafana?**

---

**¿Qué es un `Tumble Window` en Flink y en qué se diferencia de un `Sliding Window`? ¿Podría un mensaje aparecer en dos ventanas Tumble distintas?**

---

**El bridge publica en Kafka con `key=device_id`. Si `sensors_raw` tuviera 4 particiones en lugar de 1, ¿qué garantía de orden se mantendría y cuál se perdería?**

---

**¿Qué es `enable.idempotence=True` en el productor Kafka y qué garantía exacta proporciona? ¿Es suficiente por sí solo para garantizar exactly-once end-to-end en el pipeline?**

---

**El dashboard Streamlit tiene `st.session_state` para mantener el modelo entrenado entre reruns. ¿Por qué Streamlit necesita este mecanismo? ¿Qué ocurre con las variables locales entre dos interacciones del usuario?**

---

**¿Qué es el `Line Protocol` de InfluxDB y qué ventaja tiene sobre enviar los datos en JSON? ¿Por qué `flink_analytics_job.py` lo usa en lugar del SDK oficial?**

---

**¿Por qué el `flink_hash_verifier_job.py` usa la DataStream API y no la Table API como los otros dos jobs? ¿Qué limitación de la Table API lo impide?**

---

**Si añadiéramos una sexta máquina al simulador (`--machines 6`), ¿qué cambios habría que hacer en el pipeline? ¿Qué componentes se adaptan automáticamente y cuáles requieren configuración manual?**

---

**¿Qué diferencia hay entre `KafkaSource` (Flink DataStream API) y `KafkaConsumer` (confluent_kafka)? ¿Cuándo se usa cada uno en este proyecto?**

---

**¿Por qué Redpanda expone el puerto 9644 solo dentro de la red Docker y no al exterior? ¿Qué riesgo habría si se expusiera?**

---

**En InfluxDB, ¿qué diferencia hay entre un `tag` y un `field`? ¿Por qué `device_id` es un tag y `avg_temp_c` es un field?**

---

**¿Qué es `hive_partitioning=true` en DuckDB? ¿Qué ocurre si se omite cuando los archivos están en carpetas `year=2026/month=03/...`?**

---

**¿Qué garantiza `DeliveryGuarantee.AT_LEAST_ONCE` en el `KafkaSink` de Flink? ¿Qué se necesitaría para tener `EXACTLY_ONCE`?**

---

**¿Qué es un `ValueState` en Flink y por qué es necesario para el hash verifier? ¿Qué pasaría si se usara una variable de instancia Python normal en su lugar?**

---

**¿Qué significa que Redpanda sea "Kafka-compatible"? ¿Puede usarse `confluent_kafka` sin modificaciones contra Redpanda?**

---

**¿Qué es `scan.startup.mode = 'earliest-offset'` en el conector Kafka de Flink Table API? ¿Qué implicación tiene si el job se reinicia?**

---

**¿Qué hace `json.ignore-parse-errors = 'true'` en el conector Kafka de Flink? ¿Qué alternativa habría si se necesita auditar esos mensajes?**

---

**¿Por qué `minio:9000` funciona como endpoint desde el devcontainer pero `localhost:19000` no? ¿Qué determina qué hostname usar en cada contexto?**

---

**¿Qué es el `group_id` en un consumidor Kafka y qué ocurre si dos procesos arrancan con el mismo `group_id` sobre el mismo topic?**

---

**En el Line Protocol de InfluxDB, ¿qué diferencia hay entre escribir `count=5i` y `count=5.0`? ¿Qué tipo de dato representa cada uno?**

---

**¿Qué es `auto.offset.reset: earliest` en `kafka_to_minio.py`? ¿Qué ocurre la primera vez que arranca con ese grupo y qué ocurre si el proceso se reinicia con el mismo grupo?**

---

**¿Qué es `proc_time AS PROCTIME()` en Flink Table API y en qué se diferencia del `event_time` usado en el analytics job? ¿Para qué sirve el processing time?**

---

**¿Por qué el hash verifier usa `key_by(device_id)` antes de `process(HashChainVerifier())`? ¿Qué ocurriría si se procesaran todos los mensajes sin keyear?**

---

**¿Qué es `enable.auto.commit: False` en `kafka_to_minio.py` y por qué es importante para la consistencia entre Kafka y RustFS?**

---

**¿Qué ventaja tiene NDJSON (una línea = un registro) frente a un JSON array para el cold path? ¿Qué otras herramientas además de DuckDB lo leen nativamente?**

---

**¿Qué ocurre con los mensajes de `sensors_invalid` una vez escritos? ¿Hay algún consumidor activo en este pipeline que los procese?**

---

**¿Qué es el `schemaVersion` en un JSON de dashboard de Grafana y por qué no se debe bajar su valor al editar manualmente el archivo?**

---

**El endpoint `/v1/cluster` de Redpanda devuelve 404 en v25. ¿Cuál es el endpoint correcto para obtener el estado de salud del cluster y qué campos devuelve?**

---

## Hot path vs Cold path — conceptos generales

**¿Cuál es la diferencia fundamental entre hot path y cold path en una arquitectura Lambda?**

---

**¿Por qué el hot path usa InfluxDB y no RustFS? ¿Qué características de InfluxDB lo hacen adecuado para datos en tiempo real?**

---

**¿Por qué el cold path no puede usarse para alertas en tiempo real aunque tenga más datos históricos?**

---

**¿Qué es la "serving layer" en la arquitectura Lambda y qué tecnología la implementa en este proyecto?**

---

**¿Qué ocurre con los datos del hot path pasadas 3 horas según la configuración de este proyecto? ¿Dónde quedarían esos datos si no se ha escrito el cold path?**

---

**¿Por qué en el cold path se usan ventanas de 60 segundos para escribir en RustFS en lugar de escribir registro a registro?**

---

**¿Qué es la latencia de procesamiento del cold path en este proyecto? ¿Qué factores la determinan?**

---

**Si el `kafka_to_minio.py` lleva parado 2 horas y se reinicia con `auto.offset.reset: earliest`, ¿qué datos recuperará? ¿Hay algún riesgo de pérdida?**

---

**¿Qué ventaja tiene consultar hot + cold con `UNION ALL` en DuckDB frente a replicar todos los datos a una única base de datos?**

---

## 16. Conceptos generales de streaming y mensajería

**¿Qué diferencia hay entre un sistema de mensajería (RabbitMQ) y un log distribuido (Kafka/Redpanda)? ¿Por qué importa la diferencia para el cold path?**

---

**¿Qué es la retención de mensajes en Kafka? Si `sensors_raw` tuviera retención de 1 hora y el hash verifier llevara 2 horas parado, ¿qué mensajes perdería al arrancar con `earliest`?**

---

**¿Qué es el `high watermark` de una partición Kafka y cómo se relaciona con el número de mensajes visibles para un consumidor?**

---

**¿Qué es el backpressure en Flink y cómo se detecta en la Flink UI?**

---

**¿Qué es event time vs processing time en streaming? ¿En qué situación pueden divergir significativamente en este pipeline?**

---

**¿Qué es exactamente un `topic` en Kafka y en qué se diferencia de una cola tradicional?**

---

**¿Qué es `parallelism=1` en Flink y qué implicación tiene sobre el rendimiento y la capacidad de escalar el job?**

---

## Conceptos generales de almacenamiento y consulta

**¿Qué es un object store (RustFS/S3) y en qué se diferencia de un sistema de ficheros tradicional?**

---

**¿Qué es una base de datos de series temporales (TSDB) y en qué se diferencia de una base de datos relacional para datos de sensores?**

---

**¿Qué es el `bucket` en InfluxDB y en RustFS? ¿Son el mismo concepto?**

---

**¿Qué es Flux y en qué se diferencia de SQL para consultar series temporales? ¿Por qué InfluxDB v2 abandonó InfluxQL?**

---

**¿Qué es un índice columnar y por qué formatos como Parquet son más eficientes que JSON para consultas analíticas sobre millones de filas?**

---

**¿Qué es `partition pruning` y por qué el particionado Hive por `year/month/day/hour` mejora el rendimiento de DuckDB al consultar un rango de fechas?**

---

**¿Qué diferencia hay entre `read_json` y `read_parquet` en DuckDB? ¿Qué ventajas e inconvenientes tiene cada uno para el cold path?**

---

## Conceptos generales de contenedores y DevOps

**¿Qué es un `healthcheck` en Docker Compose y por qué es importante para servicios como Redpanda o InfluxDB que tardan en estar listos?**

---

**¿Qué diferencia hay entre `depends_on: service_healthy` y `depends_on: service_started` en Docker Compose?**

---

**¿Qué es un volumen Docker y por qué los datos de InfluxDB y RustFS se guardan en volúmenes en lugar de en el sistema de ficheros del contenedor?**

---

**¿Qué es una red Docker bridge y por qué todos los servicios de este proyecto comparten la misma red `ilerna_ia_big_data`?**

---

**¿Qué es un devcontainer y qué ventaja tiene frente a instalar las dependencias directamente en la máquina local?**

---

**¿Qué es `restart: unless-stopped` en Docker Compose? ¿Qué diferencia hay con `always` y con `on-failure`?**

---

## Preguntas sobre los notebooks Jupyter

### Notebook 01 — Exploración de datos

**El notebook lee datos de InfluxDB con un query Flux `range(start: -3h)`. ¿Qué ocurre si se ejecuta el notebook justo después de arrancar el pipeline por primera vez? ¿Qué datos devuelve?**

---

**La sección 3 del notebook 01 usa DuckDB con `read_json('s3://datalake/clean/**/*.json', hive_partitioning=true)`. ¿Qué hace el patrón `**/*.json`? ¿Qué archivos excluiría si se pusiera `clean/year=2026/*.json` en su lugar?**

---

**El notebook distingue entre "Edge intelligence" (Flink, alerta `avg > 80°C`) y "Cloud intelligence" (IsolationForest, `failure_prob`). ¿En qué situación detectaría anomalías el modelo ML que la regla de Flink no detectaría?**

---

**La sección 5 del notebook 01 hace `UNION ALL` entre hot_df (InfluxDB) y cold_df (RustFS) con DuckDB. ¿Qué ocurre con los registros que están en ambas fuentes a la vez (solapamiento temporal)? ¿Se duplican?**

---

**El notebook entrena el IsolationForest con `POST /model/train?range_minutes=60&contamination=0.1`. ¿Qué pasa si se entrena con solo 5 minutos de datos? ¿Qué efecto tiene `contamination=0.1` sobre los resultados de predicción?**

---

**La sección 6 del notebook 01 verifica la hash-chain leyendo mensajes de `sensors_raw` con `confluent_kafka`. ¿Por qué se agrupa por `device_id` antes de verificar? ¿Qué ocurriría si se verificaran todos los mensajes juntos sin agrupar?**

---

### Notebook 02 — Análisis de calidad

**El notebook 02 calcula el ratio de rechazo del DLQ: `total_dlq / total_raw * 100`. En una ejecución normal sin fallos simulados, ¿debería ser 0%? ¿Qué causa que siempre haya algún mensaje en `sensors_invalid`?**

---

**La sección 2 del notebook 02 verifica la hash-chain de forma independiente al job Flink. ¿Cuál es el valor de hacer esta verificación en el notebook si Flink ya la hace en tiempo real?**

---

**La sección 4 del notebook 02 analiza la distribución de unidades (`C`, `F`, `K`) en `sensors_raw`. Si el simulador genera 5 máquinas con configuración por defecto, ¿qué distribución aproximada de unidades cabría esperar?**

---

**El resumen ejecutivo (sección 6 del notebook 02) muestra tres gauges: Hash Integrity, Cold Completitud y Aceptación DLQ. ¿Qué valor de "Aceptación DLQ" indicaría que el pipeline está sano? ¿Qué significaría un valor de 50%?**

---

### Notebook 03 — Bases de datos y cálculos

**La sección 2 del notebook 03 obtiene los `high watermark` de cada topic con `get_watermark_offsets()`. ¿Qué diferencia hay entre `low watermark` y `high watermark`? ¿Cuándo el `low watermark` sería mayor que 0?**

---

**La sección 2 del notebook 03 calcula el ratio `sensors_clean / sensors_raw`. En condiciones normales, ¿debería ser cercano a 1.0 o significativamente menor? ¿Qué lo haría bajar mucho?**

---

**La sección 3 del notebook 03 inventaría los archivos NDJSON en RustFS con DuckDB. Si hay 10 archivos de 60 segundos cada uno, ¿cuántos minutos de datos históricos cubre el cold path aproximadamente?**

---

**La sección 5 del notebook 03 analiza el solapamiento temporal entre hot path y cold path. ¿Por qué puede haber registros en el hot path que no están en el cold path y viceversa?**

---

**La sección 6 del notebook 03 calcula el Z-score por máquina: `(x - mean) / std`. ¿Qué valor de Z-score se considera anómalo convencionalmente? ¿En qué se diferencia este enfoque del IsolationForest del notebook 01?**

---

**El heatmap de correlación del notebook 03 muestra la correlación entre `avg_temp_c` de distintas máquinas. ¿Qué significaría una correlación alta (>0.9) entre dos máquinas? ¿Y una correlación negativa?**

---

**El notebook 03 calcula la tendencia de temperatura con una regresión lineal sobre el tiempo. ¿Qué indica una pendiente positiva sostenida en una máquina industrial? ¿Cómo se relaciona con el umbral de alerta de 80°C?**

---

**La sección 4 del notebook 03 consulta `GET /machines/status` de FastAPI. ¿Qué ocurre si FastAPI no está corriendo cuando se ejecuta esa celda? ¿Cómo debería gestionarlo el notebook para no interrumpir las secciones siguientes?**
