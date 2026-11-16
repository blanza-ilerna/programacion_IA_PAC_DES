# Ejercicios Prácticos — ILERNA Smart-Industry PAC DES

Ejercicios hands-on sobre el pipeline real. Cada ejercicio indica los ficheros involucrados y cómo verificar la solución.

---

## Bloque 1 — Kafka / Redpanda

### Ejercicio 1.1 — Listar topics y contar mensajes

Sin usar ningún alias, escribe los comandos para:
1. Listar todos los topics existentes en Redpanda.
2. Obtener el número de mensajes (high-watermark) del topic `sensors_clean`.

<details>
<summary>Solución</summary>

```bash
# 1. Listar topics
RP=$(docker ps -qf "label=com.docker.compose.service=redpanda")
docker exec $RP rpk topic list

# 2. Contar mensajes en sensors_clean
docker exec $RP rpk topic describe sensors_clean --print-partitions \
  | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print "Mensajes:", sum+0}'
```
</details>

---

### Ejercicio 1.2 — Consumir mensajes en tiempo real

Escribe el comando para ver los últimos 5 mensajes del topic `sensors_raw` en formato JSON legible.

<details>
<summary>Solución</summary>

```bash
RP=$(docker ps -qf "label=com.docker.compose.service=redpanda")
docker exec $RP rpk topic consume sensors_raw --num 5 | python3 -m json.tool
```
</details>

---

### Ejercicio 1.3 — Verificar el DLQ

Escribe el comando para ver cuántos mensajes hay en el topic `sensors_invalid` y muestra uno de ellos para ver el campo `reason`.

<details>
<summary>Solución</summary>

```bash
RP=$(docker ps -qf "label=com.docker.compose.service=redpanda")

# Contar
docker exec $RP rpk topic describe sensors_invalid --print-partitions \
  | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print "Mensajes DLQ:", sum+0}'

# Ver uno
docker exec $RP rpk topic consume sensors_invalid --num 1 \
  | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print(json.dumps(d, indent=2))"
```
</details>

---

## Bloque 2 — Flink

### Ejercicio 2.1 — Estado de los jobs Flink

Escribe una query `curl` a la REST API de Flink que muestre el nombre, estado y duración de todos los jobs. Sin usar el alias `flink-jobs`.

<details>
<summary>Solución</summary>

```bash
curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys, json
jobs = json.load(sys.stdin)['jobs']
for j in jobs:
    dur = j.get('duration', 0) // 1000
    print(f\"{j['jid'][:8]}  {j['state']:<10}  {dur:>5}s  {j['name'][:55]}\")
"
```
</details>

---

### Ejercicio 2.2 — Cancelar un job por nombre

Escribe los comandos para cancelar el job llamado `sensor-analytics-tumble-1min` usando la REST API de Flink. No uses el alias `flink-restart`.

<details>
<summary>Solución</summary>

```bash
# 1. Obtener el JID del job por nombre
JID=$(curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys,json
jobs=json.load(sys.stdin)['jobs']
j=[x for x in jobs if 'analytics' in x['name'] and x['state']=='RUNNING']
print(j[0]['jid'] if j else '')
")

# 2. Cancelar
curl -s -X PATCH "http://jobmanager:8081/jobs/${JID}?mode=cancel"
echo "Job $JID cancelado"
```
</details>

---

### Ejercicio 2.3 — Ver la excepción de un job fallido

Dado un job en estado FAILED, escribe el comando para obtener la causa raíz del error.

<details>
<summary>Solución</summary>

```bash
JID=$(curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys,json
jobs=json.load(sys.stdin)['jobs']
failed=[j for j in jobs if j['state']=='FAILED']
print(failed[0]['jid'] if failed else '')
")

curl -s "http://jobmanager:8081/jobs/${JID}/exceptions" | python3 -c "
import sys,json
d=json.load(sys.stdin)
root=d.get('root-exception','')
# Mostrar solo las primeras 5 líneas
print('\n'.join(root.split('\n')[:5]))
"
```
</details>

---

## Bloque 3 — InfluxDB (Flux)

### Ejercicio 3.1 — Última lectura por máquina

Escribe una query Flux que devuelva la última temperatura media (`avg_temp_c`) de cada máquina en los últimos 10 minutos.

<details>
<summary>Solución</summary>

```bash
curl -s -X POST "http://influxdb:8086/api/v2/query?org=ilerna" \
  -H "Authorization: Token supersecrettoken" \
  -H "Content-Type: application/vnd.flux" \
  --data '
from(bucket: "sensores")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "machine_stats")
  |> filter(fn: (r) => r._field == "avg_temp_c")
  |> group(columns: ["device_id"])
  |> last()
'
```
</details>

---

### Ejercicio 3.2 — Máquinas en alerta

Escribe una query Flux que devuelva solo las máquinas con `avg_temp_c > 80°C` en la última hora.

<details>
<summary>Solución</summary>

```bash
curl -s -X POST "http://influxdb:8086/api/v2/query?org=ilerna" \
  -H "Authorization: Token supersecrettoken" \
  -H "Content-Type: application/vnd.flux" \
  --data '
from(bucket: "sensores")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "machine_stats")
  |> filter(fn: (r) => r._field == "avg_temp_c")
  |> filter(fn: (r) => r._value > 80.0)
  |> group(columns: ["device_id"])
  |> last()
'
```
</details>

---

### Ejercicio 3.3 — Contar registros totales

Escribe una query Flux que cuente el total de registros `machine_stats` almacenados en el bucket en las últimas 24 horas.

<details>
<summary>Solución</summary>

```bash
curl -s -X POST "http://influxdb:8086/api/v2/query?org=ilerna" \
  -H "Authorization: Token supersecrettoken" \
  -H "Content-Type: application/vnd.flux" \
  --data '
from(bucket: "sensores")
  |> range(start: -24h)
  |> filter(fn: (r) => r._measurement == "machine_stats")
  |> filter(fn: (r) => r._field == "avg_temp_c")
  |> count()
'
```
</details>

---

## Bloque 4 — FastAPI

### Ejercicio 4.1 — Estado de las máquinas vía curl

Escribe el comando `curl` para obtener el estado actual de todas las máquinas con una ventana de 10 minutos y mostrarlo formateado.

<details>
<summary>Solución</summary>

```bash
curl -s "http://localhost:8000/machines/status?range_minutes=10" | python3 -m json.tool
```
</details>

---

### Ejercicio 4.2 — Historial de una máquina

Escribe el comando para obtener el historial de temperatura de `machine-004` en los últimos 30 minutos, limitado a 50 registros.

<details>
<summary>Solución</summary>

```bash
curl -s "http://localhost:8000/machines/machine-004?range_minutes=30&limit=50" | python3 -m json.tool
```
</details>

---

### Ejercicio 4.3 — Entrenar el modelo y predecir

Escribe los comandos para:
1. Entrenar el modelo IsolationForest con los últimos 60 minutos de datos.
2. Predecir si 95°C es una anomalía para `machine-002`.

<details>
<summary>Solución</summary>

```bash
# 1. Entrenar
curl -s -X POST "http://localhost:8000/model/train?range_minutes=60&contamination=0.1" \
  | python3 -m json.tool

# 2. Predecir
curl -s "http://localhost:8000/machines/machine-002/predict?temperature_c=95.0" \
  | python3 -m json.tool
```
</details>

---

### Ejercicio 4.4 — Publicar una lectura manual

Escribe el comando para publicar una lectura de `machine-001` a 212°F vía el endpoint `/machines/publish`.

<details>
<summary>Solución</summary>

```bash
curl -s -X POST "http://localhost:8000/machines/publish" \
  -H "Content-Type: application/json" \
  -d '{"device_id": "machine-001", "temperature": 212, "unit": "F"}' \
  | python3 -m json.tool
```

Nota: el bridge convierte F a C antes de publicar en Kafka → Flink normalizará a 100°C → activará alerta.
</details>

---

## Bloque 5 — RustFS

### Ejercicio 5.1 — Listar ficheros del cold path

Escribe el comando Python para listar todos los objetos del bucket `datalake` con prefijo `clean/` y mostrar nombre y tamaño.

<details>
<summary>Solución</summary>

```python
from minio import Minio

c = Minio("minio:9000", access_key="admin", secret_key="Ilerna_Programaci0n", secure=False)
for obj in c.list_objects("datalake", prefix="clean/", recursive=True):
    print(f"{obj.object_name}  {obj.size/1024:.1f} KB")
```
</details>

---

### Ejercicio 5.2 — Leer un fichero JSON con DuckDB

Escribe la query DuckDB para leer todos los ficheros JSON del cold path desde RustFS y mostrar las 5 primeras filas de `machine-003`.

<details>
<summary>Solución</summary>

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("""
    SET s3_endpoint          = 'minio:9000';
    SET s3_access_key_id     = 'admin';
    SET s3_secret_access_key = 'Ilerna_Programaci0n';
    SET s3_use_ssl           = false;
    SET s3_url_style         = 'path';
""")

df = conn.execute("""
    SELECT device_id, temperature_c, unit_original, ts
    FROM read_json(
        's3://datalake/clean/**/*.json',
        hive_partitioning = true,
        auto_detect = true
    )
    WHERE device_id = 'machine-003'
    ORDER BY ts DESC
    LIMIT 5
""").df()

print(df)
conn.close()
```
</details>

---

### Ejercicio 5.3 — Query federada Lambda manual

Escribe un script Python que:
1. Consulte InfluxDB las últimas temperaturas (hot path).
2. Consulte RustFS los datos históricos (cold path).
3. Una ambas con DuckDB `UNION ALL` y muestre el total por fuente.

<details>
<summary>Solución</summary>

```python
import duckdb
import pandas as pd
from influxdb_client import InfluxDBClient

# --- Hot path: InfluxDB ---
client = InfluxDBClient(url="http://influxdb:8086", token="supersecrettoken", org="ilerna")
query_api = client.query_api()
tables = query_api.query("""
    from(bucket: "sensores")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "machine_stats")
      |> filter(fn: (r) => r._field == "avg_temp_c")
      |> last()
""")
hot_rows = [{"device_id": r.values["device_id"], "value": r.get_value(),
             "ts": str(r.get_time()), "source": "InfluxDB (hot)"}
            for table in tables for r in table.records]
hot_df = pd.DataFrame(hot_rows)
client.close()

# --- Cold path: RustFS via DuckDB ---
conn = duckdb.connect()
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("""
    SET s3_endpoint='minio:9000'; SET s3_access_key_id='admin';
    SET s3_secret_access_key='Ilerna_Programaci0n';
    SET s3_use_ssl=false; SET s3_url_style='path';
""")
try:
    cold_df = conn.execute("""
        SELECT device_id, CAST(temperature_c AS DOUBLE) AS value, ts, 'RustFS (cold)' AS source
        FROM read_json('s3://datalake/clean/**/*.json', hive_partitioning=true, auto_detect=true)
        WHERE temperature_c IS NOT NULL
        LIMIT 1000
    """).df()
except Exception:
    cold_df = pd.DataFrame()

# --- UNION ALL ---
if not hot_df.empty:
    conn.register("hot_data", hot_df)
if not cold_df.empty:
    conn.register("cold_data", cold_df)

result = conn.execute("""
    SELECT source, COUNT(*) AS registros
    FROM (
        SELECT source FROM hot_data
        UNION ALL
        SELECT source FROM cold_data
    )
    GROUP BY source
""").df()

print(result)
conn.close()
```
</details>

---

## Bloque 6 — Normalización (código)

### Ejercicio 6.1 — Implementar la UDF de conversión

Implementa en Python la función `to_celsius(temperature, unit)` que el job Flink usa como UDF. Debe:
- Retornar `float` en todos los casos.
- Lanzar `ValueError` si `unit` no es C, F o K.
- Manejar el caso en que `temperature` sea `None`.

<details>
<summary>Solución</summary>

```python
def to_celsius(temperature: float, unit: str) -> float:
    if temperature is None:
        return None
    unit = unit.upper().strip()
    if unit == "C":
        return float(temperature)
    elif unit == "F":
        return (float(temperature) - 32) * 5 / 9
    elif unit == "K":
        return float(temperature) - 273.15
    else:
        raise ValueError(f"Unidad desconocida: {unit}")

# Tests rápidos
assert round(to_celsius(212, "F"), 2) == 100.0
assert round(to_celsius(373.15, "K"), 2) == 100.0
assert to_celsius(25, "C") == 25.0
print("OK")
```
</details>

---

### Ejercicio 6.2 — Verificar hash-chain

Dado el siguiente par de mensajes consecutivos de `machine-001`, escribe el código Python para verificar si el `hash` del segundo es correcto:

```python
msg1 = {"device_id": "machine-001", "temperature": 72.3, "unit": "C",
        "ts": "2026-03-18T14:00:00Z", "hash": "abc123..."}
msg2 = {"device_id": "machine-001", "temperature": 74.1, "unit": "C",
        "ts": "2026-03-18T14:01:00Z", "prev_hash": "abc123...", "hash": "???"}
```

<details>
<summary>Solución</summary>

```python
import hashlib, json

def compute_hash(prev_hash: str, payload: dict) -> str:
    # Excluir campos de hash del payload antes de calcular
    clean = {k: v for k, v in payload.items() if k not in ("hash", "prev_hash")}
    content = prev_hash + json.dumps(clean, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()

def verify_chain(prev_msg: dict, current_msg: dict) -> bool:
    expected = compute_hash(prev_msg["hash"], current_msg)
    return expected == current_msg["hash"]

# Uso
is_valid = verify_chain(msg1, msg2)
print("Hash válido:", is_valid)
```
</details>

---

## Bloque 7 — Diagnóstico del pipeline

### Ejercicio 7.1 — Diagnóstico completo en un comando

Escribe un script bash que compruebe en orden:
1. Topics Kafka y número de mensajes.
2. Jobs Flink RUNNING.
3. Datos en InfluxDB (últimos 5 min).
4. Ficheros en RustFS cold path.

<details>
<summary>Solución</summary>

```bash
#!/usr/bin/env bash
RP=$(docker ps -qf 'label=com.docker.compose.service=redpanda')

echo "=== 1. KAFKA TOPICS ==="
for T in sensors_raw sensors_clean sensors_invalid sensors_verified; do
  N=$(docker exec $RP rpk topic describe $T --print-partitions 2>/dev/null \
      | awk 'NR>1 && /^[0-9]/{sum+=$NF} END{print sum+0}')
  echo "  $T: $N mensajes"
done

echo ""
echo "=== 2. FLINK JOBS ==="
curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys,json
jobs=json.load(sys.stdin)['jobs']
running=[j for j in jobs if j['state']=='RUNNING']
print(f'  {len(running)}/4 jobs RUNNING')
for j in running:
    print(f\"  ✅ {j['name'][:50]}\")
"

echo ""
echo "=== 3. INFLUXDB (últimos 5 min) ==="
curl -s -X POST "http://influxdb:8086/api/v2/query?org=ilerna" \
  -H "Authorization: Token supersecrettoken" \
  -H "Content-Type: application/vnd.flux" \
  --data 'from(bucket:"sensores")|>range(start:-5m)|>filter(fn:(r)=>r._measurement=="machine_stats")|>filter(fn:(r)=>r._field=="avg_temp_c")|>count()' \
  | python3 -c "
import sys
lines=[l for l in sys.stdin if '_result' in l]
total=sum(int(l.split(',')[-1]) for l in lines if l.strip())
print(f'  Registros: {total}')
"

echo ""
echo "=== 4. MINIO COLD PATH ==="
python3 -c "
from minio import Minio
c=Minio('minio:9000',access_key='admin',secret_key='Ilerna_Programaci0n',secure=False)
objs=list(c.list_objects('datalake',prefix='clean/',recursive=True))
total_kb=sum(o.size for o in objs)/1024
print(f'  Ficheros: {len(objs)}  |  Tamaño total: {total_kb:.0f} KB')
" 2>/dev/null || echo "  RustFS no accesible"
```
</details>

---

### Ejercicio 7.2 — Identificar el cuello de botella

El sistema muestra:
- `sensors_raw`: 5000 mensajes
- `sensors_clean`: 120 mensajes
- InfluxDB: 0 registros en últimos 5 min

¿Cuál es el componente que falla y cómo lo confirmarías?

<details>
<summary>Solución</summary>

**Componente que falla:** `flink_normalization_job` — solo procesó 120 de 5000 mensajes (tasa de conversión muy baja) o está caído.

**Confirmación:**

```bash
# 1. Ver si el job de normalización está RUNNING
flink-jobs

# 2. Si FAILED, ver la excepción
JID=$(curl -s http://jobmanager:8081/jobs/overview | python3 -c "
import sys,json
jobs=json.load(sys.stdin)['jobs']
j=[x for x in jobs if 'clean' in x['name'].lower() or 'normaliz' in x['name'].lower()]
print(j[0]['jid'] if j else '')
")
curl -s "http://jobmanager:8081/jobs/${JID}/exceptions" | python3 -c "
import sys,json; d=json.load(sys.stdin)
print(d.get('root-exception','')[:500])
"

# 3. Si el job está RUNNING pero sensors_clean no crece:
# → El topic sensors_raw puede estar vacío o el consumer group está en lag
docker exec $RP rpk topic describe sensors_clean --print-partitions
```

**Causa probable:** job de normalización caído o consumer group con offset al final.

**Solución:** `flink-restart`
</details>

---

### Ejercicio 7.3 — Grafana no muestra datos

Grafana muestra "No data" en el dashboard Telemetría IoT. InfluxDB tiene datos recientes. ¿Cuáles son las 3 causas más probables y cómo verificarías cada una?

<details>
<summary>Solución</summary>

**Causa 1 — Rango de tiempo de Grafana demasiado estrecho**
Los datos de InfluxDB pueden tener timestamps del procesamiento de backlog (histórico), fuera del rango "now-30m" de Grafana.
```
Solución: cambiar el rango en Grafana a "Last 3 hours" o "Last 6 hours".
```

**Causa 2 — Datasource mal configurado**
```bash
# Verificar que InfluxDB responde desde Grafana
curl -s -u admin:admin123 \
  "http://grafana:3000/api/datasources" | python3 -m json.tool | grep -E "name|url|type"

# Health del datasource (ID 1 = InfluxDB)
curl -s -u admin:admin123 "http://grafana:3000/api/datasources/1/health"
```

**Causa 3 — Query Flux con bucket o measurement incorrecto**
```bash
# Verificar directamente que el measurement existe
curl -s -X POST "http://influxdb:8086/api/v2/query?org=ilerna" \
  -H "Authorization: Token supersecrettoken" \
  -H "Content-Type: application/vnd.flux" \
  --data 'from(bucket:"sensores")|>range(start:-1h)|>filter(fn:(r)=>r._measurement=="machine_stats")|>count()'
# Si devuelve filas → los datos están ahí, el problema es Grafana.
```
</details>

---

## Bloque 8 — Código Python

### Ejercicio 8.1 — Consumidor Kafka básico

Escribe un consumidor Python con `confluent_kafka` que lea del topic `sensors_clean`, imprima `device_id` y `temperature_c` de cada mensaje, y se detenga tras 10 mensajes.

<details>
<summary>Solución</summary>

```python
import json
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "redpanda:29092",
    "group.id":          "ejercicio-consumidor",
    "auto.offset.reset": "earliest",
})
c.subscribe(["sensors_clean"])

count = 0
while count < 10:
    msg = c.poll(timeout=2.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue
    data = json.loads(msg.value())
    print(f"{data['device_id']}: {data['temperature_c']:.2f}°C")
    count += 1

c.close()
```
</details>

---

### Ejercicio 8.2 — Escribir en InfluxDB vía Line Protocol

Escribe la función Python que construye una línea en Line Protocol para el measurement `machine_stats` dado un diccionario de resultados de ventana Flink.

<details>
<summary>Solución</summary>

```python
import time

def to_line_protocol(record: dict) -> str:
    """
    record = {
        "device_id": "machine-001",
        "avg_temp_c": 72.3,
        "max_temp_c": 78.1,
        "count": 12,
        "alert": 0,
        "window_end": 1710768060  # epoch segundos
    }
    """
    tags   = f"device_id={record['device_id']}"
    fields = (
        f"avg_temp_c={record['avg_temp_c']},"
        f"max_temp_c={record['max_temp_c']},"
        f"count={record['count']}i,"
        f"alert={record['alert']}i"
    )
    ts_ns  = int(record.get("window_end", time.time())) * 1_000_000_000
    return f"machine_stats,{tags} {fields} {ts_ns}"

# Test
r = {"device_id": "machine-001", "avg_temp_c": 72.3,
     "max_temp_c": 78.1, "count": 12, "alert": 0, "window_end": 1710768060}
print(to_line_protocol(r))
```
</details>

---

### Ejercicio 8.3 — Endpoint FastAPI nuevo

Añade un nuevo endpoint `GET /machines/summary` que devuelva para cada máquina: `device_id`, `avg_temp_c` de la última lectura y `status` (`"OK"`, `"ALERT"` o `"SIN DATOS"`).

<details>
<summary>Solución</summary>

```python
@app.get("/machines/summary", tags=["Máquinas"])
def machines_summary():
    """Resumen de estado de todas las máquinas."""
    client = get_influx_client()
    query_api = client.query_api()
    query = f"""
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -10m)
      |> filter(fn: (r) => r._measurement == "machine_stats")
      |> filter(fn: (r) => r._field == "avg_temp_c")
      |> group(columns: ["device_id"])
      |> last()
    """
    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        seen = {}
        for table in tables:
            for r in table.records:
                dev = r.values.get("device_id")
                val = r.get_value()
                seen[dev] = val

        all_devices = [f"machine-{str(i).zfill(3)}" for i in range(1, 6)]
        summary = []
        for dev in all_devices:
            if dev not in seen:
                summary.append({"device_id": dev, "avg_temp_c": None, "status": "SIN DATOS"})
            else:
                temp = seen[dev]
                summary.append({
                    "device_id":  dev,
                    "avg_temp_c": round(temp, 2),
                    "status":     "ALERT" if temp > ALERT_THRESHOLD else "OK",
                })
        return {"machines": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()
```
</details>

---

## Puntuación orientativa

| Bloque | Ejercicios | Peso |
|---|---|---|
| Kafka / Redpanda | 1.1 – 1.3 | 15% |
| Flink REST API | 2.1 – 2.3 | 15% |
| InfluxDB Flux | 3.1 – 3.3 | 15% |
| FastAPI | 4.1 – 4.4 | 15% |
| RustFS / DuckDB | 5.1 – 5.3 | 15% |
| Código Python | 6.1 – 6.2, 8.1 – 8.3 | 15% |
| Diagnóstico | 7.1 – 7.3 | 10% |
