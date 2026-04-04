from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


def _timestamp_now() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def build_run_label(source_id: str | None = None, source_set: str | None = None) -> str:
    if source_set:
        return source_set
    return source_id or "all"


def write_report_json(
    reports_root: Path,
    operation: str,
    stem: str,
    payload: dict[str, Any] | list[Any],
) -> Path:
    day_dir = reports_root / datetime.now().strftime("%Y-%m-%d") / operation
    day_dir.mkdir(parents=True, exist_ok=True)
    report_path = day_dir / f"{stem}-{_timestamp_now()}.json"
    report_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report_path


def copy_to_latest(reports_root: Path, operation: str, path: Path) -> Path:
    latest_dir = reports_root / "latest" / operation
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_path = latest_dir / path.name
    shutil.copy2(path, latest_path)
    return latest_path


def copy_report_artifact(
    reports_root: Path,
    operation: str,
    artifact_path: Path,
) -> Path:
    day_dir = reports_root / datetime.now().strftime("%Y-%m-%d") / operation
    day_dir.mkdir(parents=True, exist_ok=True)
    copied_path = day_dir / artifact_path.name
    shutil.copy2(artifact_path, copied_path)
    return copied_path
