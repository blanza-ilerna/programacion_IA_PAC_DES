"""
check_grafana_endpoints.py — Comprueba los endpoints y campos que consumen los dashboards de Grafana.

Sirve para saber, sin abrir Grafana ni usar curl, qué paneles tendrán datos:
  · redpanda.json   → Admin API (:9644) y Pandaproxy (:8082)
  · telemetria.json → InfluxDB (consulta Flux de la variable $device y machine_stats)
  · (opcional)      → Flink REST y FastAPI

Uso (desde el workspace del devcontainer, con el pipeline levantado):
  python tests/check_grafana_endpoints.py

Hosts sobrescribibles con variables de entorno: REDPANDA_ADMIN, REDPANDA_PROXY,
FLINK_URL, FASTAPI_URL, INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET.
"""

import json
import os
import sys
import urllib.error
import urllib.request

REDPANDA_ADMIN = os.getenv("REDPANDA_ADMIN", "http://redpanda:9644")
REDPANDA_PROXY = os.getenv("REDPANDA_PROXY", "http://redpanda:8082")
FLINK_URL      = os.getenv("FLINK_URL",      "http://jobmanager:8081")
FASTAPI_URL    = os.getenv("FASTAPI_URL",    "http://localhost:8000")
INFLUX_URL     = os.getenv("INFLUX_URL",     "http://influxdb:8086")
INFLUX_TOKEN   = os.getenv("INFLUX_TOKEN",   "supersecrettoken")
INFLUX_ORG     = os.getenv("INFLUX_ORG",     "ilerna")
INFLUX_BUCKET  = os.getenv("INFLUX_BUCKET",  "sensores")

if hasattr(sys.stdout, "reconfigure"):  # consolas Windows (cp1252) no soportan "──"
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TOPICS = ["sensors_raw", "sensors_clean", "sensors_verified", "sensors_invalid"]

GREEN, RED, YELLOW, NC = "\033[0;32m", "\033[0;31m", "\033[1;33m", "\033[0m"
FAILS = 0


def report(status: str, name: str, detail: str = "") -> None:
    global FAILS
    color = {"OK": GREEN, "FAIL": RED, "INFO": YELLOW}[status]
    if status == "FAIL":
        FAILS += 1
    print(f"  {color}{status:<4}{NC} {name}" + (f" — {detail}" if detail else ""))


def get_json(url: str, timeout: int = 5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def first(obj):
    """Primer elemento si es lista; el propio objeto en otro caso."""
    return obj[0] if isinstance(obj, list) and obj else obj


def check_field(name: str, url: str, field: str, required: bool = True) -> None:
    try:
        data = first(get_json(url))
        if isinstance(data, dict) and field in data:
            report("OK", name, f"{field}={data[field]!r}")
        else:
            keys = sorted(data) if isinstance(data, dict) else type(data).__name__
            report("FAIL" if required else "INFO", name, f"campo '{field}' ausente; campos: {keys}")
    except urllib.error.HTTPError as e:
        report("FAIL" if required else "INFO", name, f"HTTP {e.code} en {url}")
    except Exception as e:
        report("FAIL" if required else "INFO", name, f"{type(e).__name__}: {e} ({url})")


def influx_flux(flux: str) -> list[str]:
    req = urllib.request.Request(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        data=flux.encode("utf-8"),
        headers={"Authorization": f"Token {INFLUX_TOKEN}", "Content-Type": "application/vnd.flux",
                 "Accept": "application/csv"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return [ln for ln in r.read().decode("utf-8").splitlines() if ln.strip() and not ln.startswith("#")]


def main() -> None:
    print("\n── Redpanda Admin API (redpanda.json, paneles 1, 3, 4)")
    check_field("brokers", f"{REDPANDA_ADMIN}/v1/brokers", "node_id")
    check_field("health_overview", f"{REDPANDA_ADMIN}/v1/cluster/health_overview", "is_healthy")

    print("\n── Mensajes por topic (redpanda.json, paneles 6-9)")
    for t in TOPICS:
        check_field(f"Pandaproxy {t}", f"{REDPANDA_PROXY}/topics/{t}/partitions/0/offsets", "end_offset")
    check_field("Admin API partitions (antiguo, ¿high_watermark?)",
                f"{REDPANDA_ADMIN}/v1/partitions/kafka/sensors_raw/0", "high_watermark", required=False)

    print("\n── InfluxDB (telemetria.json)")
    try:
        rows = influx_flux(
            'import "influxdata/influxdb/schema"\n'
            f'schema.tagValues(bucket: "{INFLUX_BUCKET}", tag: "device_id", start: -24h)')
        devices = [r.split(",")[-1] for r in rows[1:]]
        if devices:
            report("OK", "variable $device (schema.tagValues)", ", ".join(devices))
        else:
            report("FAIL", "variable $device (schema.tagValues)", "sin máquinas en 24 h — ¿pipeline corriendo?")
        rows = influx_flux(
            f'from(bucket: "{INFLUX_BUCKET}") |> range(start: -10m)'
            ' |> filter(fn: (r) => r._measurement == "machine_stats" and r._field == "avg_temp_c")'
            ' |> group() |> last()')
        report("OK" if len(rows) > 1 else "FAIL", "machine_stats últimos 10 min",
               f"{len(rows) - 1} fila(s)" if len(rows) > 1 else "sin datos — ¿job de analítica RUNNING?")
    except Exception as e:
        report("FAIL", "InfluxDB", f"{type(e).__name__}: {e}")

    print("\n── Flink / FastAPI (opcionales)")
    check_field("Flink jobs/overview", f"{FLINK_URL}/jobs/overview", "jobs", required=False)
    check_field("FastAPI machines/status", f"{FASTAPI_URL}/machines/status", "device_id", required=False)

    print(f"\n  {'Todo OK' if FAILS == 0 else f'{FAILS} comprobación(es) fallida(s)'}\n")
    sys.exit(FAILS)


if __name__ == "__main__":
    main()
