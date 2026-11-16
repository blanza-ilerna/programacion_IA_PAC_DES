"""
check_grafana_panels.py — Ejecuta la consulta de CADA panel de los dashboards provisionados
a través de Grafana (/api/ds/query) y muestra el error real o cuántas filas devuelve.

Sirve para saber por qué un panel sale con el triángulo rojo o "No data" sin tener que
pasar el ratón sobre cada panel.

Uso (desde la raíz del repo, con el pipeline y Grafana levantados):
  python tests/check_grafana_panels.py              # todos los dashboards
  python tests/check_grafana_panels.py --dry        # solo imprime las consultas, sin llamar a Grafana

Variables: GRAFANA_URL, GRAFANA_USER, GRAFANA_PASS, INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
GRAFANA_USER = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASS = os.getenv("GRAFANA_PASS", "Ilerna_Programaci0n")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "supersecrettoken")
INFLUX_ORG = os.getenv("INFLUX_ORG", "ilerna")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensores")
DASH_DIR = Path(os.getenv("DASH_DIR", "config/grafana/provisioning/dashboards"))
FALLBACK_DEVICES = [f"machine-00{i}" for i in range(1, 6)]

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

GREEN, RED, YELLOW, NC = "\033[0;32m", "\033[0;31m", "\033[1;33m", "\033[0m"
DRY = "--dry" in sys.argv
FAILS = 0


def say(kind: str, title: str, detail: str = "") -> None:
    global FAILS
    color = {"OK": GREEN, "FAIL": RED, "VACÍO": YELLOW}[kind]
    if kind == "FAIL":
        FAILS += 1
    print(f"  {color}{kind:<5}{NC} {title}" + (f" — {detail}" if detail else ""))


def devices_from_influx() -> list[str]:
    flux = ('import "influxdata/influxdb/schema"\n'
            f'schema.tagValues(bucket: "{INFLUX_BUCKET}", tag: "device_id", start: -24h)')
    req = urllib.request.Request(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}", data=flux.encode(),
        headers={"Authorization": f"Token {INFLUX_TOKEN}", "Content-Type": "application/vnd.flux",
                 "Accept": "application/csv"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            rows = [ln for ln in r.read().decode().splitlines() if ln.strip() and not ln.startswith("#")]
        found = [ln.split(",")[-1].strip() for ln in rows[1:]]
        return found or FALLBACK_DEVICES
    except Exception:
        return FALLBACK_DEVICES


def grafana_query(body: dict) -> tuple[int, dict]:
    creds = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
    req = urllib.request.Request(
        f"{GRAFANA_URL}/api/ds/query", data=json.dumps(body).encode(),
        headers={"Authorization": f"Basic {creds}", "Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"message": f"{type(e).__name__}: {e}"}


def rows_in(frames: list) -> int:
    total = 0
    for f in frames:
        vals = f.get("data", {}).get("values", [])
        total += len(vals[0]) if vals else 0
    return total


def main() -> None:
    files = sorted(DASH_DIR.glob("*.json"))
    if not files:
        print(f"No hay dashboards en {DASH_DIR.resolve()} (ejecuta desde la raíz del repo)")
        sys.exit(1)
    devices = devices_from_influx() if not DRY else FALLBACK_DEVICES
    print(f"\nMáquinas usadas para ${{device}}: {', '.join(devices)}")

    for path in files:
        dash = json.loads(path.read_text(encoding="utf-8"))
        print(f"\n── {dash.get('title', path.name)}  ({path.name})")
        for panel in dash.get("panels", []):
            for target in panel.get("targets", []):
                ds = target.get("datasource") or panel.get("datasource") or {}
                query = dict(target)
                query["datasource"] = {"uid": ds.get("uid"), "type": ds.get("type")}
                query.setdefault("refId", "A")
                query["intervalMs"], query["maxDataPoints"] = 15000, 800
                if isinstance(query.get("query"), str):
                    query["query"] = query["query"].replace("${device:json}", json.dumps(devices))
                title = f"[{panel.get('type')}] {panel.get('title')}"
                if DRY:
                    print(f"\n  {title}\n{query.get('query') or query.get('url')}")
                    continue
                status, resp = grafana_query({"queries": [query], "from": "now-30m", "to": "now"})
                result = resp.get("results", {}).get(query["refId"], {})
                err = result.get("error") or (resp.get("message") if status != 200 else None)
                if err:
                    say("FAIL", title, str(err).replace("\n", " ")[:300])
                else:
                    n = rows_in(result.get("frames", []))
                    say("OK" if n else "VACÍO", title,
                        f"{len(result.get('frames', []))} frame(s), {n} fila(s)")

    if not DRY:
        print(f"\n  {'Todos los paneles responden' if FAILS == 0 else f'{FAILS} panel(es) con error'}\n")
        sys.exit(FAILS)


if __name__ == "__main__":
    main()
