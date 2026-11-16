"""
hash_chain.py — Verificación de integridad SHA256 en cadena.

Cada sensor firma sus mensajes formando una cadena hash: el hash de cada mensaje
incluye el hash del mensaje anterior, lo que hace imposible modificar un mensaje
sin romper la cadena.

Este módulo replica la lógica del job Flink (flink_hash_verifier_job.py) como
funciones Python puras para uso en notebooks, tests y auditorías offline.

Uso:
    from src.processing.hash_chain import compute_hash, verify_chain, verify_message

    # Verificar un único mensaje (necesitas el hash previo)
    ok, reason = verify_message(msg, expected_prev_hash="abc123...")

    # Verificar una secuencia completa ordenada por ts
    results = verify_chain(messages)
    invalid = [r for r in results if not r["ok"]]
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Hash inicial para el primer mensaje de cada device (sin predecesor)
GENESIS_HASH = "0" * 64

# Campos que el bridge añade DESPUÉS de que el simulador firmó.
# Se excluyen del hash para que la verificación coincida con la firma original.
BRIDGE_ADDED_FIELDS = {"_ingested_at", "_mqtt_topic", "_mqtt_qos"}
EXCLUDE_FROM_HASH   = {"hash", "prev_hash"} | BRIDGE_ADDED_FIELDS


def _content_for_hash(msg: dict[str, Any]) -> dict[str, Any]:
    """Extrae los campos del mensaje que participan en el hash.

    El simulador firma con todos los campos excepto 'hash' y 'prev_hash'.
    Los campos añadidos por el bridge también se excluyen.
    """
    return {k: v for k, v in msg.items() if k not in EXCLUDE_FROM_HASH}


def compute_hash(msg: dict[str, Any], prev_hash: str) -> str:
    """Calcula el hash SHA256 de un mensaje.

    El hash se calcula sobre: JSON(campos_contenido, sort_keys=True) + prev_hash

    Args:
        msg:       Mensaje de sensors_raw (se excluyen los campos de EXCLUDE_FROM_HASH).
        prev_hash: Hash SHA256 del mensaje anterior (GENESIS_HASH para el primero).

    Returns:
        Hash SHA256 en hexadecimal (64 caracteres).
    """
    content = _content_for_hash(msg)
    payload = json.dumps(content, sort_keys=True) + prev_hash
    return hashlib.sha256(payload.encode()).hexdigest()


def verify_message(msg: dict[str, Any], expected_prev_hash: str) -> tuple[bool, str]:
    """Verifica la integridad de un mensaje individual.

    Comprueba:
    1. Que prev_hash declarado en el mensaje coincide con expected_prev_hash.
    2. Que el hash declarado en el mensaje coincide con compute_hash(msg, prev_hash).

    Args:
        msg:                Mensaje de sensors_raw.
        expected_prev_hash: Hash del mensaje anterior conocido por el verificador.

    Returns:
        (True, "") si el mensaje es válido.
        (False, reason) si la cadena está rota.
    """
    declared_prev = msg.get("prev_hash", "")
    declared_hash = msg.get("hash", "")

    if declared_prev != expected_prev_hash:
        return False, (
            f"prev_hash_mismatch: declared={declared_prev[:16]}… "
            f"expected={expected_prev_hash[:16]}…"
        )

    expected_hash = compute_hash(msg, declared_prev)
    if declared_hash != expected_hash:
        return False, (
            f"hash_mismatch: declared={declared_hash[:16]}… "
            f"computed={expected_hash[:16]}…"
        )

    return True, ""


def verify_chain(
    messages: list[dict[str, Any]],
    initial_prev_hash: str = GENESIS_HASH,
) -> list[dict[str, Any]]:
    """Verifica una secuencia completa de mensajes de un mismo device.

    Los mensajes deben estar ordenados cronológicamente (por 'ts').
    El verificador mantiene el estado del último hash válido.

    Args:
        messages:          Lista de mensajes de sensors_raw (mismo device_id).
        initial_prev_hash: Hash previo inicial (por defecto GENESIS_HASH).

    Returns:
        Lista de dicts con:
          - idx:       índice en la lista de entrada
          - device_id: ID del dispositivo
          - ts:        timestamp del mensaje
          - ok:        True si la integridad es correcta
          - reason:    motivo del fallo (vacío si ok=True)
    """
    results = []
    current_prev = initial_prev_hash

    for idx, msg in enumerate(messages):
        ok, reason = verify_message(msg, current_prev)
        results.append({
            "idx":       idx,
            "device_id": msg.get("device_id", "?"),
            "ts":        msg.get("ts", "?"),
            "ok":        ok,
            "reason":    reason,
        })

        if ok:
            # Avanzar la cadena solo si el mensaje es válido
            current_prev = msg.get("hash", current_prev)
        # Si es inválido, el estado del verificador se mantiene en el último hash válido
        # (comportamiento conservador: un mensaje corrupto no avanza la cadena)

    return results


def chain_integrity_report(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Genera un resumen de integridad a partir de verify_chain().

    Returns:
        Dict con: total, valid, invalid, integrity_pct, broken_at (lista de idx)
    """
    total   = len(results)
    valid   = sum(1 for r in results if r["ok"])
    invalid = total - valid
    broken  = [r["idx"] for r in results if not r["ok"]]

    return {
        "total":         total,
        "valid":         valid,
        "invalid":       invalid,
        "integrity_pct": round(valid / total * 100, 2) if total > 0 else 0.0,
        "broken_at":     broken,
    }
