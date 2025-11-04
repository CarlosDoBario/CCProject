#!/usr/bin/env python3
"""
mission_schema.py
Validação simples de mission_spec para MissionLink.
Define TASK_TYPES suportadas e validação de 'params' por task.
Fornece validate_mission_spec(mission_spec) -> (ok: bool, errors: list[str])
"""

from typing import Dict, Any, Tuple, List, Optional

TASK_TYPES = {"capture_images", "collect_samples", "env_analysis"}

def _validate_capture_images(params: Dict[str, Any]) -> List[str]:
    errs = []
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
    errs = []
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
    errs = []
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
    Reuse normalized area shape: ensure x1,y1,z1,x2,y2,z2 floats (compatibility).
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