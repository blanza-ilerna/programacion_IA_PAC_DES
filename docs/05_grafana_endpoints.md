# Grafana — Datasources y Endpoints

## Qué es Infinity Datasource

**Infinity** es un plugin de Grafana que permite consultar **cualquier API HTTP** como fuente de datos: JSON, CSV, XML o GraphQL. No requiere que el servicio tenga un datasource oficial en Grafana.

En este proyecto lo usamos para los servicios que no tienen datasource nativo:

| Servicio   | Datasource usado | Por qué                                      |
|------------|-----------------|----------------------------------------------|
| InfluxDB   | InfluxDB nativo | Plugin oficial, soporta Flux directamente    |
| Redpanda   | Infinity        | No hay datasource Kafka/Redpanda en Grafana  |
| Flink      | Infinity        | No hay datasource Flink en Grafana           |
| FastAPI    | Infinity        | API REST propia, se consulta como JSON       |

---

## Endpoints disponibles por servicio

Todos los hostnames son internos a la red Docker (`ilerna_ia_big_data`). Grafana accede a ellos desde dentro del contenedor.

### Redpanda — Admin API (puerto 9644)

| Endpoint | Descripción | Formato respuesta |
|---|---|---|
| `http://redpanda:9644/v1/brokers` | Lista de brokers del cluster | Array de objetos |
| `http://redpanda:9644/v1/cluster/health_overview` | Estado de salud del cluster (`is_healthy`, nodos caídos, particiones sin líder) | Objeto |

> **Notas:**
> - `GET /v1/cluster` devuelve **404 en Redpanda v25** — usar `/v1/cluster/health_overview`.
> - `GET /v1/topics` (listado de todos los topics) también devuelve 404. La tabla de topics del dashboard usa datos inline estáticos.
> - La Admin API **no** da el número de mensajes por topic (`/v1/partitions/kafka/{topic}/0` no incluye `high_watermark`). Para eso se usa la API REST de Redpanda Console (ver más abajo).

### Redpanda — Pandaproxy HTTP (puerto 8082)

| Endpoint | Descripción | Formato respuesta |
|---|---|---|
| `http://redpanda:8082/topics` | Lista de nombres de topics | Array de strings |

> En Redpanda v25.3 la ruta `/topics/{topic}/partitions/0/offsets` (y `/topics/{topic}`) devuelve **404** (`{"message": "Not found"}`). Pandaproxy solo se usa para listar topics.

### Redpanda Console — API REST (puerto 8080)

| Endpoint | Descripción | Formato respuesta |
|---|---|---|
| `http://redpanda-console:8080/api/topics` | Topics con particiones, réplicas y tamaño en disco | `{"topics": [{"topicName", "partitionCount", "logDirSummary": {"totalSizeBytes"}, ...}]}` |
| `http://redpanda-console:8080/api/topics/{topic}/partitions` | Particiones con offsets | `{"partitions": [{"id", "waterMarkLow", "waterMarkHigh", ...}]}` — `waterMarkHigh` = mensajes acumulados |

> Comprobado en el Codespace: `waterMarkHigh` de `sensors_raw` coincide con la métrica `redpanda_kafka_max_offset` de `http://redpanda:9644/public_metrics`.
> El host `http://redpanda-console:8080` tiene que estar en `allowedHosts` del datasource Infinity (`datasources.yml`); los datasources solo se releen al reiniciar Grafana.

### Flink — REST API (puerto 8081)

| Endpoint | Descripción | Formato respuesta |
|---|---|---|
| `http://jobmanager:8081/jobs/overview` | Resumen de todos los jobs con estado | Objeto con array `jobs` |
| `http://jobmanager:8081/jobs` | Lista de job IDs | Objeto con array `jobs` |
| `http://jobmanager:8081/taskmanagers` | Info de los task managers | Objeto con array `taskmanagers` |

### FastAPI (puerto 8000)

| Endpoint | Descripción | Formato respuesta |
|---|---|---|
| `http://workspace:8000/machines/status` | Estado actual de todas las máquinas | Array de objetos |
| `http://workspace:8000/machines/{id}` | Detalle de una máquina concreta | Objeto |
| `http://workspace:8000/alerts` | Alertas activas | Array de objetos |

> **Nota:** El hostname `workspace` puede variar. Si no resuelve, prueba `api` o `localhost`.

### InfluxDB — API Flux (puerto 8086)

Aunque se usa el datasource nativo, el endpoint subyacente es:

```
POST http://influxdb:8086/api/v2/query?org=ilerna
Authorization: Token supersecrettoken
Content-Type: application/vnd.flux
```

---

## Dashboards provisionados

Los dashboards están definidos en `config/grafana/provisioning/dashboards/` y Grafana los recarga cada 30 segundos. El provider tiene `allowUiUpdates: true`, así que se pueden editar y guardar desde la UI, pero **la versión del archivo JSON es la que manda**: para que un cambio sea permanente hay que llevarlo al JSON (Dashboard settings → JSON Model) y subir su `version`.

| Archivo | UID | Título | Datasource |
|---|---|---|---|
| `telemetria.json` | `ilerna-telemetria` | ILERNA Smart-Industry — Telemetría IoT | InfluxDB (Flux) |
| `redpanda.json` | `ilerna-redpanda` | ILERNA Smart-Industry — Redpanda / Kafka | Infinity |

**Telemetría IoT** (`machine_stats`, variable `$device` multiselección, refresco 10 s, rango 30 min): KPIs (máquinas con datos, en alerta, media y máximo, *último dato hace*), temperatura por máquina con la zona de umbral a 80 °C, temperatura actual (bargauge), línea de estado de alerta por máquina, ventanas en alerta cada 5 min, lecturas procesadas por minuto y tabla de estado.

**Redpanda / Kafka**: brokers, salud del cluster, definición de topics (datos fijos) y mensajes acumulados por topic vía la API REST de Redpanda Console (el del DLQ se pone naranja/rojo al crecer).

### Comprobar qué paneles tendrán datos

Sin `curl`, desde el workspace con el pipeline levantado:

```bash
python tests/check_grafana_endpoints.py
```

Valida cada endpoint y el campo que lee el panel (`node_id`, `is_healthy`, `end_offset`…), la consulta Flux de la variable `$device` y que haya `machine_stats` en los últimos 10 min.

Para ver el JSON actual de un dashboard desde la API de Grafana:

```
http://localhost:13000/api/dashboards/uid/ilerna-redpanda
```

### Otros endpoints de la API de Grafana

| Endpoint | Descripción |
|---|---|
| `http://localhost:13000/api/health` | Estado de Grafana |
| `http://localhost:13000/api/datasources` | Datasources configurados |
| `http://localhost:13000/api/search` | Lista de dashboards |
| `http://localhost:13000/api/alerts` | Alertas activas |

---

## Configuración de un panel Infinity — referencia rápida

Para añadir un nuevo panel que consuma una API REST:

1. Datasource: **Infinity**
2. Type: **JSON**
3. Parser: **Backend**
4. Source: **URL**
5. Method: **GET**
6. URL: el endpoint deseado (ver tablas arriba)
7. En _Parsing options_: añadir columnas con `selector` = nombre del campo JSON

Para datos estáticos (sin llamada HTTP):
- Source: **Inline**
- Data: JSON array como string, p.ej. `[{"topic":"sensors_raw"},...]`
