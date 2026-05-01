from typing import Any, Dict, Optional
import math


def _as_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float):
        return None if math.isnan(value) else value
    value_str = str(value).strip()
    if value_str == "" or value_str.lower() in {"none", "na", "nan", "null"}:
        return None
    try:
        return float(value_str)
    except ValueError:
        return None


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    value_str = str(value).strip().lower()
    if value_str in {"1", "true", "yes", "y"}:
        return True
    if value_str in {"0", "false", "no", "n"}:
        return False
    return default


def with_warc_metric_defaults(summary: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(summary)
    normalized["validated_hits_warc"] = _as_optional_float(
        normalized.get("validated_hits_warc")
    )
    normalized["validated_hits_warc_per_10k"] = _as_optional_float(
        normalized.get("validated_hits_warc_per_10k")
    )
    normalized["warc_validation_attempted"] = _as_bool(
        normalized.get("warc_validation_attempted"), default=False
    )
    normalized["warc_validation_notes"] = str(
        normalized.get("warc_validation_notes", "")
    )
    return normalized
