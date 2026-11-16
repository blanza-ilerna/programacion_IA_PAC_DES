"""
list_datalake.py — Lista el contenido del bucket S3 (RustFS) sin necesitar la consola web.

Útil en Codespaces, donde el login de la consola puede fallar tras el proxy de puertos.

Uso (desde el workspace, con el pipeline levantado):
  python tests/list_datalake.py                 # 20 objetos más recientes + resumen por hora
  python tests/list_datalake.py --limit 50      # más objetos
  python tests/list_datalake.py --prefix clean/year=2026/   # filtrar por prefijo
  python tests/list_datalake.py --preview       # además muestra las primeras líneas del objeto más reciente

Variables: MINIO_ENDPOINT (minio:9000), MINIO_ACCESS, MINIO_SECRET, MINIO_BUCKET (datalake)
"""

import argparse
import os
import re
import sys
from collections import Counter

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
ACCESS = os.getenv("MINIO_ACCESS", "admin")
SECRET = os.getenv("MINIO_SECRET", "Ilerna_Programaci0n")
BUCKET = os.getenv("MINIO_BUCKET", "datalake")

PARTITION = re.compile(r"(year=\d+/month=\d+/day=\d+/hour=\d+)")


def human(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"


def main() -> int:
    ap = argparse.ArgumentParser(description="Lista el contenido del bucket S3 (RustFS)")
    ap.add_argument("--prefix", default="", help="prefijo a listar (por defecto, todo el bucket)")
    ap.add_argument("--limit", type=int, default=20, help="nº de objetos más recientes a mostrar")
    ap.add_argument("--preview", action="store_true", help="muestra las primeras líneas del objeto más reciente")
    ap.add_argument("--lines", type=int, default=3, help="líneas a mostrar con --preview")
    args = ap.parse_args()

    try:
        from minio import Minio
    except ImportError:
        print("Falta el cliente S3: pip install minio")
        return 1

    client = Minio(ENDPOINT, access_key=ACCESS, secret_key=SECRET, secure=False)
    try:
        if not client.bucket_exists(BUCKET):
            existing = [b.name for b in client.list_buckets()]
            print(f"El bucket '{BUCKET}' no existe. Buckets disponibles: {existing or 'ninguno'}")
            print("¿Has ejecutado `source .devcontainer/init_pipeline.sh`?")
            return 1
        objs = list(client.list_objects(BUCKET, prefix=args.prefix, recursive=True))
    except Exception as e:
        print(f"No se pudo consultar {ENDPOINT}: {type(e).__name__}: {e}")
        return 1

    files = [o for o in objs if not o.is_dir]
    print(f"\nBucket '{BUCKET}' en {ENDPOINT}" + (f"  (prefijo '{args.prefix}')" if args.prefix else ""))
    if not files:
        print("  Sin objetos todavía. kafka_to_minio.py escribe cada 60 s: ¿están corriendo `bridge` y `sim`?")
        return 0

    files.sort(key=lambda o: o.last_modified, reverse=True)
    total = sum(o.size or 0 for o in files)
    print(f"  {len(files)} objeto(s), {human(total)} en total\n")

    print(f"  Últimos {min(args.limit, len(files))} objetos:")
    print(f"  {'Modificado (UTC)':<20} {'Tamaño':>9}  Objeto")
    for o in files[: args.limit]:
        print(f"  {o.last_modified.strftime('%Y-%m-%d %H:%M:%S'):<20} {human(o.size or 0):>9}  {o.object_name}")

    parts = Counter()
    for o in files:
        m = PARTITION.search(o.object_name)
        parts[m.group(1) if m else "(sin partición)"] += 1
    print("\n  Archivos por partición horaria:")
    for part, n in sorted(parts.items()):
        print(f"    {part}  →  {n}")

    if args.preview:
        newest = files[0]
        print(f"\n  Primeras {args.lines} líneas de {newest.object_name}:")
        resp = client.get_object(BUCKET, newest.object_name)
        try:
            data = resp.read(8192).decode("utf-8", errors="replace")
        finally:
            resp.close()
            resp.release_conn()
        for line in data.splitlines()[: args.lines]:
            print(f"    {line[:200]}")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
