#!/usr/bin/env bash
# s3web.sh — Cliente web S3 (s3manager) para explorar el bucket de RustFS desde el navegador.
#
# Por qué existe: la consola propia de RustFS (:19001) falla en Codespaces con
# SignatureDoesNotMatch (el navegador firma con la URL del proxy). s3manager firma las
# peticiones desde el servidor, dentro de la red Docker, así que no le afecta el proxy.
#
# NO está en docker-compose.yml a propósito: si su imagen dejara de descargarse, el
# Codespace no arrancaría (pasó con MinIO). Así, un fallo aquí solo da un aviso.
#
# Uso:  bash .devcontainer/s3web.sh [start|stop|status]     (por defecto: start)
# Web:  puerto 19002 (pestaña Ports de Codespaces) — solo lectura (ALLOW_DELETE=false)
#       Comparte la red del devcontainer: no publica puertos en el host.

IMAGE="cloudlena/s3manager:v0.13.0"
NAME="s3web"
PORT="19002"   # puerto en el devcontainer (lo reenvía Codespaces)

GREEN="\033[0;32m"; YELLOW="\033[1;33m"; NC="\033[0m"
ok()   { echo -e "    ${GREEN}✅ $*${NC}"; }
warn() { echo -e "    ${YELLOW}⚠️  $*${NC}"; }


start() {
  if ! command -v docker >/dev/null 2>&1; then warn "docker no disponible: s3web no arrancado"; return 0; fi

  if [[ -n "$(docker ps -qf "name=^${NAME}$" 2>/dev/null)" ]]; then
    ok "Cliente web S3 ya está corriendo → puerto ${PORT}"; return 0
  fi
  docker rm -f "${NAME}" >/dev/null 2>&1 || true      # restos de un arranque anterior

  # Se comparte la red del contenedor 'workspace' (el devcontainer): s3manager escucha en
  # localhost:${PORT} DENTRO del devcontainer, igual que Streamlit o FastAPI, y Codespaces
  # lo reenvía como cualquier proceso propio. No se publica ningún puerto en el host
  # (publicar -p 19002 chocaba: "bind: address already in use" en el host del Codespace).
  # Desde 'workspace' también resuelve el hostname 'minio' (misma red Docker del compose).
  local ws
  ws="$(docker ps -qf "label=com.docker.compose.service=workspace" 2>/dev/null | head -1)"
  if [[ -z "${ws}" ]]; then warn "Contenedor 'workspace' no encontrado: s3web no arrancado"; return 0; fi

  local err
  if err="$(docker run -d --name "${NAME}" --network "container:${ws}" \
       -e PORT="${PORT}" \
       -e ENDPOINT="minio:9000" \
       -e ACCESS_KEY_ID="${MINIO_ACCESS:-admin}" \
       -e SECRET_ACCESS_KEY="${MINIO_SECRET:-Ilerna_Programaci0n}" \
       -e REGION="us-east-1" \
       -e USE_SSL="false" \
       -e BUCKET_LOOKUP="Path" \
       -e ALLOW_DELETE="false" \
       "${IMAGE}" 2>&1 >/dev/null)"; then
    ok "Cliente web S3 (s3manager) arrancado → puerto ${PORT} (solo lectura)"
  else
    docker rm -f "${NAME}" >/dev/null 2>&1 || true   # no dejar un contenedor a medias
    warn "No se pudo arrancar s3web (${IMAGE}). No afecta al pipeline."
    # La primera línea es el error útil; la última suele ser "Run 'docker run --help'..."
    warn "Error de docker: $(echo "${err}" | grep -v -e '^$' -e "docker run --help" | tail -n 1)"
    warn "Alternativas: python tests/list_datalake.py  ·  s3 ls s3://datalake/"
  fi
  return 0
}

stop() {
  docker rm -f "${NAME}" >/dev/null 2>&1 && ok "s3web detenido" || warn "s3web no estaba corriendo"
  return 0
}

status() {
  if [[ -n "$(docker ps -qf "name=^${NAME}$" 2>/dev/null)" ]]; then
    ok "s3web corriendo → puerto ${PORT}"
    python3 -c "import urllib.request;print('    HTTP', urllib.request.urlopen('http://localhost:${PORT}', timeout=5).status)" 2>/dev/null \
      || warn "s3web corre pero no responde aún en http://localhost:${PORT} (espera unos segundos)"
  else
    warn "s3web no está corriendo. Lanza: bash .devcontainer/s3web.sh"
  fi
  return 0
}

case "${1:-start}" in
  start)  start ;;
  stop)   stop ;;
  status) status ;;
  *) echo "Uso: $0 [start|stop|status]" ;;
esac
exit 0
