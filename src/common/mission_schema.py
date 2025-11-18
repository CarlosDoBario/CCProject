#!/usr/bin/env python3
"""
mission_schema.py

Validação e (de/para) serialização de mission_spec para uso em MissionLink (ML).

Este módulo continua a validar a estrutura das mission_spec (mesmo sem JSON em
transporte, as missões são objetos Python internos). Adiciona helpers para:
 - normalizar a mission_spec (area normalizada, campos por defeito)
 - converter mission_spec -> lista de TLVs (usada para construir MISSION_ASSIGN)
 - converter TLV map (como devolvido por binary_proto.parse_ml_datagram) -> mission_spec

Desta forma mantemos a validação semântica, e fornecemos integração directa com o
formato binário TLV definido em common/binary_proto.py.
"""

from typing import Dict, Any, Tuple, List, Optional
import json
import struct

from common import binary_proto, utils

# Tarefas suportadas e validação específica por tarefa
TASK_TYPES = {"capture_images", "collect_samples", "env_analysis"}


def _validate_capture_images(params: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if "interval_s" not in params:
        errs.append("capture_images: missing 'interval_s'")
    else:
        try:
            if float(params["interval_s"]) <= 0:
                errs.append("capture_images: 'interval_s' must be > 0")
        except Exception:
            errs.append("capture_images: 'interval_s' must be numeric")
    if "frames" not in params:
        errs.append("capture_images: missing 'frames'")
    else:
        try:
            if int(params["frames"]) <= 0:
                errs.append("capture_images: 'frames' must be > 0")
        except Exception:
            errs.append("capture_images: 'frames' must be integer")
    # optional resolution
    if "resolution" in params:
        if not isinstance(params["resolution"], str):
            errs.append("capture_images: 'resolution' must be string like '1024x768'")
    return errs


def _validate_collect_samples(params: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    # accept either sample_count or depth_mm+sample_count
    if "sample_count" not in params:
        errs.append("collect_samples: missing 'sample_count'")
    else:
        try:
            if int(params["sample_count"]) <= 0:
                errs.append("collect_samples: 'sample_count' must be > 0")
        except Exception:
            errs.append("collect_samples: 'sample_count' must be integer")
    # optional depth_mm
    if "depth_mm" in params:
        try:
            if float(params["depth_mm"]) < 0:
                errs.append("collect_samples: 'depth_mm' must be >= 0")
        except Exception:
            errs.append("collect_samples: 'depth_mm' must be numeric")
    return errs


def _validate_env_analysis(params: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    if "sensors" not in params:
        errs.append("env_analysis: missing 'sensors' (list)")
    else:
        if not isinstance(params["sensors"], list) or len(params["sensors"]) == 0:
            errs.append("env_analysis: 'sensors' must be a non-empty list")
    if "sampling_rate_s" not in params:
        errs.append("env_analysis: missing 'sampling_rate_s'")
    else:
        try:
            if float(params["sampling_rate_s"]) <= 0:
                errs.append("env_analysis: 'sampling_rate_s' must be > 0")
        except Exception:
            errs.append("env_analysis: 'sampling_rate_s' must be numeric")
    return errs


def _normalize_area(area: Optional[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    """
    Normalize the area shape: ensure x1,y1,z1,x2,y2,z2 floats and order coordinates.
    """
    if area is None:
        return None
    try:
        x1 = float(area.get("x1"))
        y1 = float(area.get("y1"))
        x2 = float(area.get("x2"))
        y2 = float(area.get("y2"))
    except Exception:
        return None
    z1 = float(area.get("z1", 0.0))
    z2 = float(area.get("z2", 0.0))
    return {
        "x1": min(x1, x2),
        "y1": min(y1, y2),
        "z1": min(z1, z2),
        "x2": max(x1, x2),
        "y2": max(y1, y2),
        "z2": max(z1, z2),
    }


def validate_mission_spec(mission_spec: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate top-level mission_spec dict.
    Returns (ok, errors_list).
    Checks:
      - 'task' present and in TASK_TYPES
      - 'params' validated according to task
      - 'area' basic numeric shape (x1,y1,x2,y2) optional but if present normalized
    """
    errors: List[str] = []

    if not isinstance(mission_spec, dict):
        return False, ["mission_spec must be an object"]

    task = mission_spec.get("task")
    if not task:
        errors.append("missing 'task'")
        return False, errors

    if task not in TASK_TYPES:
        errors.append(f"unknown task '{task}' (allowed: {sorted(TASK_TYPES)})")
        return False, errors

    params = mission_spec.get("params", {})
    if not isinstance(params, dict):
        errors.append("'params' must be an object")
        return False, errors

    # task-specific validation
    if task == "capture_images":
        errors.extend(_validate_capture_images(params))
    elif task == "collect_samples":
        errors.extend(_validate_collect_samples(params))
    elif task == "env_analysis":
        errors.extend(_validate_env_analysis(params))

    # area validation (optional)
    area = mission_spec.get("area")
    if area is not None:
        norm = _normalize_area(area)
        if norm is None:
            errors.append("area must include numeric x1,y1,x2,y2 (z optional)")

    ok = len(errors) == 0
    return ok, errors


# -------------------------------------------------------------------
# Helpers to convert mission_spec <-> TLVs (binary_proto)
# -------------------------------------------------------------------
def mission_spec_to_tlvs(mission_spec: Dict[str, Any]) -> List[tuple]:
    """
    Convert a mission_spec (dict) into a list of TLV tuples suitable for
    binary_proto.pack_ml_datagram.

    TLVs emitted (if present):
      - TLV_MISSION_ID (string)  -- if mission_id present
      - TLV_TASK (string)
      - TLV_AREA (6*float32) -- if area present
      - TLV_PARAMS_JSON (string) -- JSON-encoded params (keeps params flexible)
    """
    tlvs: List[tuple] = []

    # mission_id (optional)
    mid = mission_spec.get("mission_id")
    if mid:
        tlvs.append((binary_proto.TLV_MISSION_ID, str(mid).encode("utf-8")))

    # task (required)
    task = mission_spec.get("task")
    if task:
        tlvs.append(binary_proto.tlv_string(binary_proto.TLV_TASK, str(task)))

    # area -- normalize and encode as 6*float32 (x1,y1,z1,x2,y2,z2)
    area = _normalize_area(mission_spec.get("area")) if mission_spec.get("area") is not None else None
    if area:
        vals = (float(area["x1"]), float(area["y1"]), float(area["z1"]), float(area["x2"]), float(area["y2"]), float(area["z2"]))
        area_bytes = struct.pack(">ffffff", *vals)
        tlvs.append((binary_proto.TLV_AREA, area_bytes))

    # params -> JSON string TLV
    params = mission_spec.get("params")
    if params is not None:
        try:
            params_b = json.dumps(params, ensure_ascii=False).encode("utf-8")
            tlvs.append((binary_proto.TLV_PARAMS_JSON, params_b))
        except Exception:
            # fallback: string representation
            tlvs.append((binary_proto.TLV_PARAMS_JSON, str(params).encode("utf-8")))

    return tlvs


def mission_spec_from_tlvmap(tlv_map: Dict[int, List[bytes]]) -> Dict[str, Any]:
    """
    Convert TLV map (as returned by binary_proto.parse_ml_datagram()['tlvs'])
    back to a mission_spec dict.

    Recognises TLV_MISSION_ID, TLV_TASK, TLV_AREA, TLV_PARAMS_JSON, TLV_MISSION_SPEC.
    """
    ms: Dict[str, Any] = {}

    if binary_proto.TLV_MISSION_ID in tlv_map:
        try:
            ms["mission_id"] = tlv_map[binary_proto.TLV_MISSION_ID][0].decode("utf-8")
        except Exception:
            ms["mission_id"] = tlv_map[binary_proto.TLV_MISSION_ID][0].decode("latin-1", errors="ignore")

    if binary_proto.TLV_TASK in tlv_map:
        ms["task"] = tlv_map[binary_proto.TLV_TASK][0].decode("utf-8")

    if binary_proto.TLV_AREA in tlv_map:
        v = tlv_map[binary_proto.TLV_AREA][0]
        if len(v) >= 24:
            try:
                x1, y1, z1, x2, y2, z2 = struct.unpack(">ffffff", v[:24])
                ms["area"] = {"x1": float(x1), "y1": float(y1), "z1": float(z1), "x2": float(x2), "y2": float(y2), "z2": float(z2)}
            except Exception:
                # ignore parse errors; leave out area
                pass

    if binary_proto.TLV_PARAMS_JSON in tlv_map:
        try:
            params_json = tlv_map[binary_proto.TLV_PARAMS_JSON][0].decode("utf-8")
            ms["params"] = json.loads(params_json)
        except Exception:
            # fallback to raw string
            try:
                ms["params"] = json.loads(tlv_map[binary_proto.TLV_PARAMS_JSON][0].decode("latin-1"))
            except Exception:
                ms["params"] = {"_raw": tlv_map[binary_proto.TLV_PARAMS_JSON][0].decode("utf-8", errors="ignore")}

    # If TLV_MISSION_SPEC present (string blob), attempt to parse as JSON first
    if binary_proto.TLV_MISSION_SPEC in tlv_map and "params" not in ms:
        try:
            spec_s = tlv_map[binary_proto.TLV_MISSION_SPEC][0].decode("utf-8")
            parsed = json.loads(spec_s)
            if isinstance(parsed, dict):
                # merge fields
                ms.update(parsed)
        except Exception:
            # ignore

            pass

    # Normalize area shapes if present
    if "area" in ms:
        norm = _normalize_area(ms.get("area"))
        if norm:
            ms["area"] = norm

    return ms


def normalize_mission_spec(mission_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a normalized copy of mission_spec:
      - ensure 'params' exists (dict)
      - normalize area shape
    """
    out: Dict[str, Any] = {}
    out.update(mission_spec)
    params = out.get("params", {})
    if not isinstance(params, dict):
        params = {}
    out["params"] = params
    if "area" in out and out["area"] is not None:
        out["area"] = _normalize_area(out["area"])
    return out


__all__ = [
    "TASK_TYPES",
    "validate_mission_spec",
    "mission_spec_to_tlvs",
    "mission_spec_from_tlvmap",
    "normalize_mission_spec",
]