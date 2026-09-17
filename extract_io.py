"""Self-contained helpers used by run-local workflow result extractors."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ImportError:  # pragma: no cover - the report environment provides pandas
    pd = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def base_payload(
    workflow: str, version: str, result_dir: Path, **extra: Any
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "workflow": workflow,
        "version": version,
        "result_dir": str(result_dir.resolve()),
        "extracted_at": utc_now_iso(),
        "tables": {},
    }
    payload.update(extra)
    return payload


def write_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def write_xlsx(
    payload: dict[str, Any], path: Path, sheet_keys: list[tuple[str, str]]
) -> None:
    if pd is None:
        raise RuntimeError("pandas and openpyxl are required for Excel export")
    path.parent.mkdir(parents=True, exist_ok=True)
    meta = {key: value for key, value in payload.items() if key != "tables"}
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame([meta]).to_excel(writer, sheet_name="meta", index=False)
        for sheet, key in sheet_keys:
            pd.DataFrame(payload.get("tables", {}).get(key, []) or []).to_excel(
                writer, sheet_name=sheet, index=False
            )


def df_records(frame: Any) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    return frame.where(frame.notna(), None).to_dict(orient="records")


def read_csv_safe(path: Path, **kwargs: Any) -> Any:
    if pd is None or not path.is_file():
        return None
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:  # noqa: BLE001 - extractors intentionally tolerate malformed optional tables
        return None


def read_tsv_safe(path: Path, **kwargs: Any) -> Any:
    return read_csv_safe(path, sep="\t", **kwargs)


def read_excel_safe(path: Path, **kwargs: Any) -> Any:
    if pd is None or not path.is_file():
        return None
    try:
        return pd.read_excel(path, **kwargs)
    except Exception:  # noqa: BLE001 - extractors intentionally tolerate malformed optional tables
        return None
