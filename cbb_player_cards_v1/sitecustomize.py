from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

_CONCRETE_PATH = type(Path())
_ORIG_EXISTS = _CONCRETE_PATH.exists
_ORIG_IS_FILE = _CONCRETE_PATH.is_file
_ORIG_READ_TEXT = _CONCRETE_PATH.read_text
_ORIG_OPEN = _CONCRETE_PATH.open


def _transfer_projection_target(path: Path) -> tuple[str, Path] | None:
    parts = path.parts
    try:
        idx = parts.index("transfer_projection")
    except ValueError:
        return None
    if idx < 1 or idx + 1 >= len(parts):
        return None
    season_file = parts[idx + 1]
    if not season_file.endswith(".json"):
        return None
    season = season_file[:-5]
    if not season:
        return None
    return season, path.parent


def _load_split_transfer_payload(path: Path) -> str | None:
    target = _transfer_projection_target(path)
    if target is None:
        return None
    season, parent = target
    part1 = parent / f"{season}_part1.json"
    part2 = parent / f"{season}_part2.json"
    if not _ORIG_EXISTS(part1) and not _ORIG_EXISTS(part2):
        return None

    rows: list[Any] = []
    chunk_files: list[str] = []
    row_count = 0
    for part in (part1, part2):
        if not _ORIG_EXISTS(part):
            continue
        try:
            payload = json.loads(_ORIG_READ_TEXT(part, encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        part_rows = payload.get("rows")
        if isinstance(part_rows, list):
            rows.extend(part_rows)
            row_count += len(part_rows)
        cf = payload.get("chunk_files")
        if isinstance(cf, list):
            chunk_files.extend(str(v) for v in cf)

    merged = {
        "season": season,
        "generated_at": None,
        "section": "transfer_projection",
        "chunk_files": chunk_files,
        "row_count": row_count,
        "rows": rows,
        "source_parts": [p.name for p in (part1, part2) if _ORIG_EXISTS(p)],
    }
    return json.dumps(merged, ensure_ascii=True, indent=2)


def exists(self: Path) -> bool:
    if _ORIG_EXISTS(self):
        return True
    return _load_split_transfer_payload(self) is not None


def is_file(self: Path) -> bool:
    if _ORIG_IS_FILE(self):
        return True
    return _load_split_transfer_payload(self) is not None


def read_text(self: Path, *args: Any, **kwargs: Any) -> str:
    payload = _load_split_transfer_payload(self)
    if payload is not None:
        return payload
    return _ORIG_READ_TEXT(self, *args, **kwargs)


def open(self: Path, *args: Any, **kwargs: Any):
    payload = _load_split_transfer_payload(self)
    if payload is not None:
        mode = args[0] if args else kwargs.get("mode", "r")
        if "b" in mode:
            return io.BytesIO(payload.encode("utf-8"))
        return io.StringIO(payload)
    return _ORIG_OPEN(self, *args, **kwargs)


_CONCRETE_PATH.exists = exists  # type: ignore[assignment]
_CONCRETE_PATH.is_file = is_file  # type: ignore[assignment]
_CONCRETE_PATH.read_text = read_text  # type: ignore[assignment]
_CONCRETE_PATH.open = open  # type: ignore[assignment]
