from __future__ import annotations

import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def serialize_json(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def validate_json_payload(payload: dict[str, Any]) -> bool:
    try:
        json.dumps(payload)
        return True
    except (TypeError, ValueError):
        return False


def generate_id(prefix: str, index: int, width: int = 6) -> str:
    return f"{prefix}{index:0{width}d}"


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def weighted_choice(options: list[tuple[Any, float]]) -> Any:
    total = sum(weight for _, weight in options)
    rand = random.uniform(0, total)
    cumulative = 0.0
    for option, weight in options:
        cumulative += weight
        if rand <= cumulative:
            return option
    return options[-1][0]
