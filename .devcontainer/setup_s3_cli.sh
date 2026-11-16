#!/usr/bin/env bash
# setup_s3_cli.sh — Instala y configura el AWS CLI v2 para usar RustFS desde la terminal.
#
# - Instala el binario oficial en ~/.local (sin sudo y SIN tocar los paquetes Python del
#   proyecto: no se usa `pip install awscli`, que fija versiones de otras dependencias).
# - Crea el perfil "rustfs" (endpoint http://minio:9000, rutas path-style) sin sobrescribir
#   perfiles que ya existan.
# - Añade la función de shell `s3` a ~/.bashrc:  s3 ls s3://datalake/ --recursive
#
# Idempotente y nunca falla (exit 0): si algo no se puede hacer, solo avisa.
# Variable de prueba: S3CLI_SKIP_INSTALL=1 omite la descarga del binario.

GREEN="\033[0;32m"; YELLOW="\033[1;33m"; NC="\033[0m"
ok()   { echo -e "    ${GREEN}✅ $*${NC}"; }
warn() { echo -e "    ${YELLOW}⚠️  $*${NC}"; }

AWS_LOCAL_BIN="${HOME}/.local/bin/aws"
ACCESS="${MINIO_ACCESS:-admin}"
SECRET="${MINIO_SECRET:-Ilerna_Programaci0n}"

# ── 1. Binario ───────────────────────────────────────────────
install_cli() {
  if command -v aws >/dev/null 2>&1 || [[ -x "${AWS_LOCAL_BIN}" ]]; then
    ok "AWS CLI ya instalado"; return 0
  fi
  if [[ "${S3CLI_SKIP_INSTALL:-0}" == "1" ]]; then warn "Instalación omitida (S3CLI_SKIP_INSTALL=1)"; return 0; fi

  local arch tmp
  arch="$(uname -m)"                      # x86_64 | aarch64 (mismos nombres que usa AWS)
  tmp="$(mktemp -d)"
  if ! curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${arch}.zip" -o "${tmp}/awscli.zip"; then
    warn "No se pudo descargar el AWS CLI (sin red o arquitectura ${arch} no soportada)"; rm -rf "${tmp}"; return 0
  fi
  if command -v unzip >/dev/null 2>&1; then
    unzip -q "${tmp}/awscli.zip" -d "${tmp}"
  else
    python3 -m zipfile -e "${tmp}/awscli.zip" "${tmp}" && chmod -R u+x "${tmp}/aws"
  fi
  mkdir -p "${HOME}/.local/bin"
  if "${tmp}/aws/install" -i "${HOME}/.local/aws-cli" -b "${HOME}/.local/bin" --update >/dev/null 2>&1; then
    ok "AWS CLI instalado en ~/.local/bin/aws"
  else
    warn "Falló el instalador del AWS CLI (el resto del entorno no se ve afectado)"
  fi
  rm -rf "${tmp}"
  return 0
}

# ── 2. Perfil "rustfs" ───────────────────────────────────────
configure_profile() {
  mkdir -p "${HOME}/.aws"
  touch "${HOME}/.aws/config" "${HOME}/.aws/credentials"
  chmod 600 "${HOME}/.aws/credentials"

  if grep -q '^\[profile rustfs\]' "${HOME}/.aws/config"; then
    ok "Perfil 'rustfs' ya existe en ~/.aws/config (no se modifica)"
  else
    cat >> "${HOME}/.aws/config" <<EOF

[profile rustfs]
region = us-east-1
endpoint_url = http://minio:9000
s3 =
    addressing_style = path
EOF
    ok "Perfil 'rustfs' añadido a ~/.aws/config"
  fi

  if grep -q '^\[rustfs\]' "${HOME}/.aws/credentials"; then
    ok "Credenciales 'rustfs' ya existen (no se modifican)"
  else
    cat >> "${HOME}/.aws/credentials" <<EOF

[rustfs]
aws_access_key_id = ${ACCESS}
aws_secret_access_key = ${SECRET}
EOF
    ok "Credenciales 'rustfs' añadidas a ~/.aws/credentials"
  fi
}

# ── 3. Función `s3` en ~/.bashrc ─────────────────────────────
add_shell_helper() {
  local rc="${HOME}/.bashrc" marker="# === ILERNA s3 (RustFS) ==="
  touch "${rc}"
  if grep -q "${marker}" "${rc}"; then
    sed -i "/^${marker}$/,/^# === fin ILERNA s3 ===$/d" "${rc}"
  fi
  cat >> "${rc}" <<'EOF'
# === ILERNA s3 (RustFS) ===
_aws_bin() { command -v aws 2>/dev/null || echo "${HOME}/.local/bin/aws"; }
s3()    { "$(_aws_bin)" --profile rustfs s3    "$@"; }   # s3 ls s3://datalake/ --recursive
s3api() { "$(_aws_bin)" --profile rustfs s3api "$@"; }   # s3api list-buckets
# === fin ILERNA s3 ===
EOF
  ok "Funciones 's3' y 's3api' añadidas a ~/.bashrc (abre una terminal nueva o: source ~/.bashrc)"
}

echo ">>> Configurando acceso S3 por terminal (AWS CLI → RustFS)..."
install_cli
configure_profile
add_shell_helper
exit 0
