"""
test_processing.py — Tests unitarios para src/processing/

Cubre las tres funciones clave del pipeline replicadas como Python puro:
  · transformations.py  → to_celsius, normalize_record, normalize_batch
  · validators.py       → validate_raw_message, validate_clean_message, filter_valid
  · hash_chain.py       → compute_hash, verify_message, verify_chain, chain_integrity_report

Ejecución (sin Docker ni servicios externos):
    cd c:/Trabajo/Ilerna/programacion_IA_PAC_DES
    python -m pytest tests/test_processing.py -v
"""
import sys
import os
import hashlib
import json

import pytest

# Añadir raíz del proyecto al path para imports relativos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.processing.transformations import to_celsius, normalize_record, normalize_batch
from src.processing.validators import (
    validate_raw_message, validate_clean_message, filter_valid,
    VALID_UNITS, TEMP_LIMITS, REQUIRED_FIELDS_RAW,
)
from src.processing.hash_chain import (
    compute_hash, verify_message, verify_chain, chain_integrity_report,
    GENESIS_HASH, EXCLUDE_FROM_HASH,
)


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

def _raw_msg(device_id="machine-001", temperature=75.0, unit="C",
             ts="2026-03-11T09:00:00Z", prev_hash=None, hash_val=None):
    """Genera un mensaje sensors_raw con hash-chain calculado."""
    prev = prev_hash if prev_hash is not None else GENESIS_HASH
    msg = {
        "device_id":    device_id,
        "temperature":  temperature,
        "unit":         unit,
        "ts":           ts,
        "_ingested_at": 1741687200000,
        "prev_hash":    prev,
    }
    if hash_val is not None:
        msg["hash"] = hash_val
    else:
        msg["hash"] = compute_hash(msg, prev)
    return msg


# ══════════════════════════════════════════════════════════════════════════════
# 1 · transformations.py
# ══════════════════════════════════════════════════════════════════════════════

class TestToCelsius:
    def test_celsius_passthrough(self):
        assert to_celsius(80.0, "C") == 80.0

    def test_fahrenheit_boiling(self):
        assert abs(to_celsius(212.0, "F") - 100.0) < 1e-9

    def test_fahrenheit_freezing(self):
        assert abs(to_celsius(32.0, "F") - 0.0) < 1e-9

    def test_fahrenheit_body(self):
        assert abs(to_celsius(98.6, "F") - 37.0) < 0.01

    def test_kelvin_boiling(self):
        assert abs(to_celsius(373.15, "K") - 100.0) < 1e-9

    def test_kelvin_absolute_zero(self):
        assert abs(to_celsius(273.15, "K") - 0.0) < 1e-9

    def test_alert_threshold_f(self):
        """176°F debe ser exactamente 80°C (umbral de alerta)."""
        assert abs(to_celsius(176.0, "F") - 80.0) < 1e-9

    def test_alert_threshold_k(self):
        """353.15K debe ser exactamente 80°C."""
        assert abs(to_celsius(353.15, "K") - 80.0) < 1e-9

    def test_case_insensitive_lowercase(self):
        assert to_celsius(0.0, "c") == 0.0
        assert abs(to_celsius(212.0, "f") - 100.0) < 1e-9
        assert abs(to_celsius(273.15, "k") - 0.0) < 1e-9

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unidad desconocida"):
            to_celsius(100.0, "R")

    def test_unknown_unit_rankine(self):
        with pytest.raises(ValueError):
            to_celsius(671.67, "RANKINE")


class TestNormalizeRecord:
    def _base(self, **kwargs):
        base = {
            "device_id": "machine-001", "temperature": 75.0,
            "unit": "C", "ts": "2026-03-11T09:00:00Z",
        }
        base.update(kwargs)
        return base

    def test_valid_celsius(self):
        r = normalize_record(self._base())
        assert r is not None
        assert r["temperature_c"] == 75.0
        assert r["unit_original"] == "C"
        assert r["device_id"] == "machine-001"

    def test_valid_fahrenheit_converted(self):
        r = normalize_record(self._base(temperature=176.0, unit="F"))
        assert r is not None
        assert abs(r["temperature_c"] - 80.0) < 0.001

    def test_valid_kelvin_converted(self):
        r = normalize_record(self._base(temperature=353.15, unit="K"))
        assert r is not None
        assert abs(r["temperature_c"] - 80.0) < 0.001

    def test_missing_device_id_returns_none(self):
        msg = {"temperature": 75.0, "unit": "C", "ts": "2026-03-11T09:00:00Z"}
        assert normalize_record(msg) is None

    def test_missing_temperature_returns_none(self):
        assert normalize_record(self._base(temperature=None)) is None

    def test_unknown_unit_returns_none(self):
        assert normalize_record(self._base(unit="X")) is None

    def test_temperature_above_max_returns_none(self):
        """201°C supera el límite de normalize_record (-50…200)."""
        assert normalize_record(self._base(temperature=201.0)) is None

    def test_temperature_below_min_returns_none(self):
        assert normalize_record(self._base(temperature=-51.0)) is None

    def test_output_has_required_keys(self):
        r = normalize_record(self._base())
        for key in ("device_id", "temperature_c", "unit_original", "ts"):
            assert key in r


class TestNormalizeBatch:
    def test_mixed_batch(self):
        records = [
            {"device_id": "m-001", "temperature": 75.0,  "unit": "C",  "ts": "t1"},  # OK
            {"device_id": "m-002", "temperature": 176.0, "unit": "F",  "ts": "t2"},  # OK (→ 80°C)
            {"device_id": "m-003", "temperature": "hot", "unit": "C",  "ts": "t3"},  # inválido: no numérico
            {"device_id": "m-004", "temperature": 99999, "unit": "C",  "ts": "t4"},  # inválido: fuera de rango
        ]
        clean, invalid = normalize_batch(records)
        assert len(clean) == 2
        assert len(invalid) == 2

    def test_all_valid(self):
        records = [
            {"device_id": f"m-{i}", "temperature": 50.0 + i, "unit": "C", "ts": "t"}
            for i in range(5)
        ]
        clean, invalid = normalize_batch(records)
        assert len(clean) == 5
        assert len(invalid) == 0

    def test_empty_batch(self):
        clean, invalid = normalize_batch([])
        assert clean == []
        assert invalid == []


# ══════════════════════════════════════════════════════════════════════════════
# 2 · validators.py
# ══════════════════════════════════════════════════════════════════════════════

class TestValidateRawMessage:
    def _valid(self, **kwargs):
        base = {
            "device_id":   "machine-001",
            "temperature": 75.0,
            "unit":        "C",
            "ts":          "2026-03-11T09:00:00Z",
            "hash":        "a" * 64,
            "prev_hash":   "0" * 64,
        }
        base.update(kwargs)
        return base

    def test_valid_message(self):
        ok, reason = validate_raw_message(self._valid())
        assert ok is True
        assert reason == ""

    def test_valid_units(self):
        # Usar temperaturas dentro del rango de cada unidad
        cases = [("C", 75.0), ("F", 176.0), ("K", 353.15)]
        for unit, temp in cases:
            ok, reason = validate_raw_message(self._valid(unit=unit, temperature=temp))
            assert ok, f"Unidad {unit} con {temp} debería ser válida, reason={reason}"

    def test_invalid_unit(self):
        ok, reason = validate_raw_message(self._valid(unit="X"))
        assert ok is False
        assert "invalid_unit" in reason

    def test_missing_field_temperature(self):
        msg = self._valid()
        del msg["temperature"]
        ok, reason = validate_raw_message(msg)
        assert ok is False
        assert "missing_fields" in reason

    def test_missing_field_hash(self):
        msg = self._valid()
        del msg["hash"]
        ok, reason = validate_raw_message(msg)
        assert ok is False

    def test_empty_device_id(self):
        ok, reason = validate_raw_message(self._valid(device_id=""))
        assert ok is False
        assert "empty_device_id" in reason

    def test_temperature_not_numeric(self):
        ok, reason = validate_raw_message(self._valid(temperature="hot"))
        assert ok is False
        assert "not_numeric" in reason

    def test_temperature_out_of_range_celsius(self):
        ok, reason = validate_raw_message(self._valid(temperature=250.0, unit="C"))
        assert ok is False
        assert "out_of_range" in reason

    def test_temperature_at_boundary_celsius(self):
        ok, _ = validate_raw_message(self._valid(temperature=-50.0, unit="C"))
        assert ok is True
        ok, _ = validate_raw_message(self._valid(temperature=200.0, unit="C"))
        assert ok is True

    def test_temperature_out_of_range_kelvin(self):
        ok, reason = validate_raw_message(self._valid(temperature=100.0, unit="K"))
        assert ok is False


class TestValidateCleanMessage:
    def test_valid_clean(self):
        ok, _ = validate_clean_message({
            "device_id": "m-001", "temperature_c": 72.5, "ts": "2026-03-11T09:00:00Z"
        })
        assert ok is True

    def test_missing_temperature_c(self):
        ok, reason = validate_clean_message({"device_id": "m-001", "ts": "t"})
        assert ok is False
        assert "missing_fields" in reason

    def test_temperature_c_out_of_range(self):
        ok, reason = validate_clean_message({
            "device_id": "m-001", "temperature_c": 250.0, "ts": "t"
        })
        assert ok is False


class TestFilterValid:
    def test_filter_raw_schema(self):
        records = [
            {"device_id": "m-001", "temperature": 50.0, "unit": "C",
             "ts": "t", "hash": "a"*64, "prev_hash": "0"*64},
            {"device_id": "",      "temperature": 50.0, "unit": "C",
             "ts": "t", "hash": "a"*64, "prev_hash": "0"*64},
        ]
        valid, invalid = filter_valid(records, schema="raw")
        assert len(valid) == 1
        assert len(invalid) == 1
        assert "_validation_error" in invalid[0]

    def test_filter_clean_schema(self):
        records = [
            {"device_id": "m-001", "temperature_c": 72.0, "ts": "t"},
            {"device_id": "m-002", "temperature_c": None, "ts": "t"},
        ]
        valid, invalid = filter_valid(records, schema="clean")
        assert len(valid) == 1
        assert len(invalid) == 1


# ══════════════════════════════════════════════════════════════════════════════
# 3 · hash_chain.py
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeHash:
    def test_returns_64_hex_chars(self):
        msg = _raw_msg()
        h = compute_hash(msg, GENESIS_HASH)
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        msg = _raw_msg()
        h1 = compute_hash(msg, GENESIS_HASH)
        h2 = compute_hash(msg, GENESIS_HASH)
        assert h1 == h2

    def test_different_prev_hash_different_result(self):
        msg = _raw_msg()
        h1 = compute_hash(msg, GENESIS_HASH)
        h2 = compute_hash(msg, "a" * 64)
        assert h1 != h2

    def test_content_change_changes_hash(self):
        msg1 = _raw_msg(temperature=75.0)
        msg2 = _raw_msg(temperature=76.0)
        h1 = compute_hash(msg1, GENESIS_HASH)
        h2 = compute_hash(msg2, GENESIS_HASH)
        assert h1 != h2

    def test_excluded_fields_do_not_affect_hash(self):
        """Los campos de EXCLUDE_FROM_HASH (hash, prev_hash y los añadidos por
        el bridge) no participan en el hash; cualquier otro campo sí."""
        msg = _raw_msg()
        h1 = compute_hash(msg, GENESIS_HASH)
        for field in EXCLUDE_FROM_HASH:
            assert compute_hash(dict(msg, **{field: "otro"}), GENESIS_HASH) == h1, field

    def test_unlisted_extra_field_changes_hash(self):
        """El simulador firma todos los campos salvo los excluidos, por lo que
        un campo extra distinto sí altera el hash (detecta manipulación)."""
        msg = _raw_msg()
        h1 = compute_hash(msg, GENESIS_HASH)
        assert compute_hash(dict(msg, extra_field="x"), GENESIS_HASH) != h1


class TestVerifyMessage:
    def test_valid_genesis_message(self):
        msg = _raw_msg()  # hash calculado correctamente con GENESIS_HASH
        ok, reason = verify_message(msg, GENESIS_HASH)
        assert ok is True
        assert reason == ""

    def test_tampered_hash(self):
        msg = _raw_msg()
        msg["hash"] = "f" * 64  # hash incorrecto
        ok, reason = verify_message(msg, GENESIS_HASH)
        assert ok is False
        assert "hash_mismatch" in reason

    def test_wrong_prev_hash(self):
        msg = _raw_msg()
        ok, reason = verify_message(msg, "a" * 64)  # estado esperado distinto
        assert ok is False
        assert "prev_hash_mismatch" in reason

    def test_tampered_temperature(self):
        msg = _raw_msg(temperature=75.0)
        msg["temperature"] = 99.0  # modificar contenido sin actualizar hash
        ok, reason = verify_message(msg, GENESIS_HASH)
        assert ok is False


class TestVerifyChain:
    def _build_chain(self, n, device_id="machine-001", base_temp=70.0):
        """Genera una cadena de n mensajes correctamente encadenados."""
        messages = []
        prev = GENESIS_HASH
        for i in range(n):
            msg = _raw_msg(
                device_id=device_id,
                temperature=base_temp + i,
                ts=f"2026-03-11T09:{i:02d}:00Z",
                prev_hash=prev,
            )
            prev = msg["hash"]
            messages.append(msg)
        return messages

    def test_valid_chain(self):
        messages = self._build_chain(10)
        results = verify_chain(messages)
        assert all(r["ok"] for r in results)

    def test_single_message_valid(self):
        msg = _raw_msg()
        results = verify_chain([msg])
        assert results[0]["ok"] is True

    def test_empty_chain(self):
        assert verify_chain([]) == []

    def test_tampered_message_detected(self):
        messages = self._build_chain(5)
        messages[2]["temperature"] = 999.0  # manipular mensaje índice 2
        results = verify_chain(messages)
        assert results[0]["ok"] is True
        assert results[1]["ok"] is True
        assert results[2]["ok"] is False  # detectado

    def test_tampered_propagates_to_next(self):
        """Tras un mensaje inválido, el siguiente también falla porque
        el estado del verificador no avanza con hashes incorrectos."""
        messages = self._build_chain(4)
        messages[1]["temperature"] = 999.0  # romper índice 1
        results = verify_chain(messages)
        assert results[1]["ok"] is False
        # el mensaje 2 fue generado a partir del hash correcto de msg[1],
        # pero el verificador se quedó en el hash de msg[0] → falla
        assert results[2]["ok"] is False

    def test_chain_result_keys(self):
        results = verify_chain(self._build_chain(2))
        for r in results:
            for key in ("idx", "device_id", "ts", "ok", "reason"):
                assert key in r

    def test_custom_initial_hash(self):
        """Permite verificar cadenas que no empiezan desde GENESIS_HASH."""
        messages = self._build_chain(3)
        # Simular que el verificador ya tiene el hash del mensaje 0
        partial = messages[1:]
        results = verify_chain(partial, initial_prev_hash=messages[0]["hash"])
        assert all(r["ok"] for r in results)


class TestChainIntegrityReport:
    def _results(self, pattern):
        """pattern: lista de True/False para simular resultados de verify_chain."""
        return [{"idx": i, "device_id": "m", "ts": "t", "ok": v, "reason": "" if v else "err"}
                for i, v in enumerate(pattern)]

    def test_all_valid(self):
        report = chain_integrity_report(self._results([True, True, True, True]))
        assert report["total"] == 4
        assert report["valid"] == 4
        assert report["invalid"] == 0
        assert report["integrity_pct"] == 100.0
        assert report["broken_at"] == []

    def test_partial_invalid(self):
        report = chain_integrity_report(self._results([True, False, True, False]))
        assert report["valid"] == 2
        assert report["invalid"] == 2
        assert report["integrity_pct"] == 50.0
        assert report["broken_at"] == [1, 3]

    def test_all_invalid(self):
        report = chain_integrity_report(self._results([False, False]))
        assert report["integrity_pct"] == 0.0

    def test_empty_results(self):
        report = chain_integrity_report([])
        assert report["total"] == 0
        assert report["integrity_pct"] == 0.0

    def test_single_valid(self):
        report = chain_integrity_report(self._results([True]))
        assert report["integrity_pct"] == 100.0
        assert report["broken_at"] == []
