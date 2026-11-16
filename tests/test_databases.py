"""
test_databases.py — Verificación de datos en bases de datos y Grafana.

Comprueba:
  1. Redpanda  — mensajes en cada topic del pipeline
  2. InfluxDB  — datos recientes en machine_stats (últimos 30 min y 3 h)
  3. RustFS     — archivos JSON en el datalake (cold path)
  4. Grafana   — datasource health + queries de paneles del dashboard

Uso:
  python tests/test_databases.py
"""

import json
import sys
import urllib.request
import urllib.error
import base64
import subprocess
import time

# ── Configuración ─────────────────────────────────────────────
INFLUX_URL    = "http://influxdb:8086"
INFLUX_TOKEN  = "supersecrettoken"
INFLUX_ORG    = "ilerna"
INFLUX_BUCKET = "sensores"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS   = "admin"
MINIO_SECRET   = "Ilerna_Programaci0n"
MINIO_BUCKET   = "datalake"

GRAFANA_URL    = "http://grafana:3000"
GRAFANA_USER   = "admin"
GRAFANA_PASS   = "Ilerna_Programaci0n"

REDPANDA_TOPICS = ["sensors_raw", "sensors_clean", "sensors_invalid", "sensors_verified"]

# ── Helpers ───────────────────────────────────────────────────
PASS = FAIL = WARN = 0
GREEN  = "\033[0;32m"
RED    = "\033[0;31m"
YELLOW = "\033[1;33m"
CYAN   = "\033[0;36m"
NC     = "\033[0m"


def ok(name, detail=""):
    global PASS; PASS += 1
    print(f"  {GREEN}✅ {name}{NC}" + (f" — {detail}" if detail else ""))


def fail(name, detail=""):
    global FAIL; FAIL += 1
    print(f"  {RED}❌ {name}{NC}" + (f" — {detail}" if detail else ""))


def warn(name, detail=""):
    global WARN; WARN += 1
    print(f"  {YELLOW}⚠️  {name}{NC}" + (f" — {detail}" if detail else ""))


def section(title):
    pad = "─" * max(1, 52 - len(title))
    print(f"\n{CYAN}── {title} {pad}{NC}")


def influx_query(flux: str) -> list[dict]:
    """Ejecuta una query Flux y devuelve filas como lista de dicts."""
    url = f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}"
    req = urllib.request.Request(
        url,
        data=flux.encode("utf-8"),
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            lines = r.read().decode("utf-8").splitlines()
        rows = []
        header = None
        for line in lines:
            if not line.strip() or line.startswith("#"):
                header = None
                continue
            parts = line.split(",")
            if header is None:
                header = parts
            else:
                rows.append(dict(zip(header, parts)))
        return rows
    except Exception as e:
        return []


def grafana_request(path: str, method="GET", body=None) -> dict | None:
    creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
    headers = {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}
    req = urllib.request.Request(
        f"{GRAFANA_URL}{path}",
        data=json.dumps(body).encode() if body else None,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return {"_error": e.code, "_body": e.read().decode()[:300]}
        except Exception:
            return {"_error": e.code}
    except Exception as e:
        return {"_error": str(e)}


# ── 1. Redpanda ───────────────────────────────────────────────
def test_redpanda():
    section("Redpanda — Mensajes por topic")
    rp = subprocess.run(
        ["docker", "ps", "-qf", "label=com.docker.compose.service=redpanda"],
        capture_output=True, text=True
    ).stdout.strip()

    if not rp:
        fail("Redpanda", "contenedor no encontrado")
        return

    for topic in REDPANDA_TOPICS:
        result = subprocess.run(
            ["docker", "exec", rp, "rpk", "topic", "describe", topic, "--print-partitions"],
            capture_output=True, text=True, timeout=10
        )
        count = 0
        for line in result.stdout.splitlines():
            parts = line.split()
            if parts and parts[0].isdigit():
                try:
                    count += int(parts[-1])   # HIGH-WATERMARK es la última columna
                except ValueError:
                    pass
        if count > 0:
            ok(f"Topic {topic}", f"{count:,} mensajes")
        else:
            warn(f"Topic {topic}", "0 mensajes — ¿sim y bridge corriendo?")


# ── 2. InfluxDB ───────────────────────────────────────────────
def test_influxdb():
    section("InfluxDB — machine_stats")

    # 2a. Datos en los últimos 30 minutos
    rows_30m = influx_query(
        f'from(bucket:"{INFLUX_BUCKET}")'
        f' |> range(start: -30m)'
        f' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "avg_temp_c")'
        f' |> last()'
        f' |> keep(columns: ["device_id", "_value", "_time"])'
    )
    data_rows = [r for r in rows_30m if r.get("device_id", "").startswith("machine")]

    if data_rows:
        ok("Datos últimos 30 min", f"{len(data_rows)} máquina(s) con datos recientes")
        for r in data_rows:
            alert_rows = influx_query(
                f'from(bucket:"{INFLUX_BUCKET}")'
                f' |> range(start: -30m)'
                f' |> filter(fn:(r) => r._measurement == "machine_stats"'
                f'   and r._field == "alert" and r.device_id == "{r["device_id"]}")'
                f' |> last()'
                f' |> keep(columns: ["_value"])'
            )
            alert_val = next((x.get("_value","?") for x in alert_rows if x.get("_value")), "?")
            estado = "⚠ ALERTA" if alert_val == "1" else "✓ OK"
            print(f"    • {r['device_id']:12s} avg={float(r['_value']):.1f}°C  {estado}")
    else:
        warn("Datos últimos 30 min", "sin datos recientes — ¿analytics job corriendo?")

        # Intentar con rango mayor
        rows_3h = influx_query(
            f'from(bucket:"{INFLUX_BUCKET}")'
            f' |> range(start: -3h)'
            f' |> filter(fn:(r) => r._measurement == "machine_stats")'
            f' |> last()'
            f' |> keep(columns: ["_time"])'
        )
        times = [r.get("_time","") for r in rows_3h if r.get("_time","").startswith("20")]
        if times:
            warn("Datos en las últimas 3 h", f"último dato: {times[0]} — expande el rango en Grafana")
        else:
            fail("InfluxDB machine_stats", "sin ningún dato en las últimas 3 h")

    # 2b. Contar alertas activas
    alert_rows = influx_query(
        f'from(bucket:"{INFLUX_BUCKET}")'
        f' |> range(start: -30m)'
        f' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "alert")'
        f' |> last()'
        f' |> filter(fn:(r) => r._value == 1)'
        f' |> keep(columns: ["device_id"])'
    )
    alertas = [r["device_id"] for r in alert_rows if r.get("device_id","").startswith("machine")]
    if alertas:
        warn("Máquinas en ALERTA (>80°C)", ", ".join(alertas))
    elif data_rows:
        ok("Sin alertas activas", "todas las máquinas por debajo de 80°C")


# ── 3. RustFS ──────────────────────────────────────────────────
def test_minio():
    section("RustFS — Cold Path (datalake)")
    try:
        from minio import Minio
        client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

        if not client.bucket_exists(MINIO_BUCKET):
            fail("Bucket datalake", "no existe — ¿init_pipeline.sh ejecutado?")
            return
        ok("Bucket datalake", "existe")

        objs = list(client.list_objects(MINIO_BUCKET, recursive=True))
        json_files = [o for o in objs if o.object_name.endswith(".json")]
        success_files = [o for o in objs if o.object_name.endswith("_SUCCESS")]

        if json_files:
            total_bytes = sum(o.size or 0 for o in json_files)
            ok("Archivos JSON", f"{len(json_files)} archivo(s) — {total_bytes/1024:.1f} KB total")
            # Mostrar particiones únicas
            partitions = set()
            for o in json_files:
                parts = [p for p in o.object_name.split("/") if "=" in p]
                if parts:
                    partitions.add("/".join(parts[:2]))  # year=X/month=Y
            for p in sorted(partitions)[:5]:
                print(f"    • {p}")
            if success_files:
                ok("Particiones comprometidas", f"{len(success_files)} _SUCCESS file(s)")
        else:
            warn("Archivos JSON", "0 archivos — flink_to_minio tarda ~10 min en emitir la primera partición")
            if objs:
                print(f"    (hay {len(objs)} objeto(s) en el bucket, ninguno .json aún)")

    except ImportError:
        fail("RustFS", "minio no instalado — pip install minio")
    except Exception as e:
        fail("RustFS", str(e))


# ── 4. Grafana ────────────────────────────────────────────────
def test_grafana():
    section("Grafana — Datasource y Dashboards")

    # 4a. Health
    health = grafana_request("/api/health")
    if isinstance(health, dict) and health.get("database") == "ok":
        ok("Grafana health", f"version={health.get('version','?')}")
    else:
        fail("Grafana health", str(health))
        return

    # 4b. Datasource InfluxDB
    ds_health = grafana_request("/api/datasources/uid/influxdb/health")
    if isinstance(ds_health, dict) and ds_health.get("status") == "OK":
        ok("Datasource InfluxDB", ds_health.get("message","OK"))
    else:
        fail("Datasource InfluxDB", str(ds_health))

    # 4c. Query directa via Grafana → InfluxDB (últimos 30 min)
    query_body = {
        "queries": [{
            "refId": "A",
            "datasource": {"uid": "influxdb"},
            "query": (
                'from(bucket:"sensores")'
                ' |> range(start: -30m)'
                ' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "avg_temp_c")'
                ' |> last()'
            ),
        }],
        "from": "now-30m",
        "to": "now",
    }
    result = grafana_request("/api/ds/query", method="POST", body=query_body)
    if isinstance(result, dict) and "_error" not in result:
        frames = result.get("results", {}).get("A", {}).get("frames", [])
        if frames:
            ok("Query Grafana→InfluxDB (30m)", f"{len(frames)} frame(s) — paneles deberían mostrar datos")
        else:
            warn("Query Grafana→InfluxDB (30m)", "0 frames — sin datos en los últimos 30 min")
            # Retry con 3h
            query_body["queries"][0]["query"] = query_body["queries"][0]["query"].replace("-30m", "-3h")
            query_body["from"] = "now-3h"
            result3h = grafana_request("/api/ds/query", method="POST", body=query_body)
            frames3h = result3h.get("results", {}).get("A", {}).get("frames", []) if isinstance(result3h, dict) else []
            if frames3h:
                warn("Query Grafana→InfluxDB (3h)", f"{len(frames3h)} frame(s) — cambia el rango del dashboard a 'Last 3 hours'")
            else:
                fail("Query Grafana→InfluxDB (3h)", "sin datos en las últimas 3 h")
    else:
        fail("Query Grafana→InfluxDB", str(result))

    # 4d. Dashboards provisionados
    dashboards = grafana_request("/api/search?type=dash-db")
    if isinstance(dashboards, list):
        ok("Dashboards provisionados", f"{len(dashboards)} dashboard(s)")
        for d in dashboards:
            print(f"    • {d.get('title','?')}  (uid={d.get('uid','?')})")
    else:
        warn("Dashboards", "no se pudo listar")

    # 4e. Verificar cada panel del dashboard de Telemetría
    section("Grafana — Paneles Telemetría IoT")
    PANEL_QUERIES = {
        "Máquinas activas": (
            'from(bucket:"sensores") |> range(start: -30m)'
            ' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "avg_temp_c")'
            ' |> group(columns: ["device_id"]) |> last() |> group() |> count()'
        ),
        "Máquinas en alerta": (
            'from(bucket:"sensores") |> range(start: -30m)'
            ' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "alert")'
            ' |> group(columns: ["device_id"]) |> last()'
            ' |> filter(fn:(r) => r._value == 1) |> group() |> count()'
        ),
        "Temperatura media global": (
            'from(bucket:"sensores") |> range(start: -30m)'
            ' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "avg_temp_c")'
            ' |> group(columns: ["device_id"]) |> last() |> group() |> mean(column: "_value")'
        ),
        "Temperatura máxima global": (
            'from(bucket:"sensores") |> range(start: -30m)'
            ' |> filter(fn:(r) => r._measurement == "machine_stats" and r._field == "max_temp_c")'
            ' |> group(columns: ["device_id"]) |> last() |> group() |> max(column: "_value")'
        ),
    }
    for panel_name, query in PANEL_QUERIES.items():
        body = {
            "queries": [{"refId": "A", "datasource": {"uid": "influxdb"}, "query": query}],
            "from": "now-30m", "to": "now",
        }
        res = grafana_request("/api/ds/query", method="POST", body=body)
        if isinstance(res, dict) and "_error" not in res:
            frames = res.get("results", {}).get("A", {}).get("frames", [])
            # Extraer valor si existe
            val = "sin valor"
            try:
                val = res["results"]["A"]["frames"][0]["data"]["values"][-1][0]
                val = f"valor={val:.2f}" if isinstance(val, float) else f"valor={val}"
            except Exception:
                pass
            if frames:
                ok(panel_name, val)
            else:
                warn(panel_name, "0 frames — sin datos en rango de 30 min")
        else:
            fail(panel_name, str(res.get("_body", res) if isinstance(res, dict) else res))


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("")
    print("╔══════════════════════════════════════════════════════╗")
    print("║   ILERNA PAC DES - Verificación de Bases de Datos   ║")
    print("╚══════════════════════════════════════════════════════╝")

    test_redpanda()
    test_influxdb()
    test_minio()
    test_grafana()

    total = PASS + FAIL + WARN
    print("\n──────────────────────────────────────────────────────")
    print(f"  {GREEN}✅ OK: {PASS}{NC}  {YELLOW}⚠️  WARN: {WARN}{NC}  {RED}❌ FAIL: {FAIL}{NC}  (total: {total} checks)")
    print("")
    sys.exit(FAIL)
