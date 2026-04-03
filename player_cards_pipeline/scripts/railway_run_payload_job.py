#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw.strip())


def parse_years(spec: str) -> list[str]:
    out: list[int] = []
    for part in (spec or "").split(","):
        p = part.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            start, end = int(a), int(b)
            step = 1 if end >= start else -1
            out.extend(range(start, end + step, step))
        else:
            out.append(int(p))
    return [str(y) for y in sorted(set(out))]


def norm_season(value: str) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def shard_tag(chunk_index: int, chunk_count: int) -> str:
    width = max(2, len(str(max(0, chunk_count - 1))))
    return f"chunk_{chunk_index:0{width}d}_of_{chunk_count:0{width}d}"


def shard_outputs_ready(root: Path, out_dir: str, years: list[str], chunk_count: int) -> bool:
    if chunk_count < 2:
        return False
    out_root = root / out_dir
    for year in years:
        year_dir = out_root / norm_season(year)
        for chunk_index in range(chunk_count):
            tag = shard_tag(chunk_index, chunk_count)
            expected = (
                year_dir / f"manifest.{tag}.json",
                year_dir / f"index.{tag}.json",
                year_dir / f"errors.{tag}.json",
            )
            if not all(path.exists() for path in expected):
                return False
    return True


def maybe_run_auto_merge(root: Path, out_dir: str, years: list[str], chunk_count: int) -> None:
    if not env_flag("AUTO_MERGE", True):
        return
    if not env_flag("WRITE_SHARD_FILES", False):
        return
    if env_flag("MERGE_SHARDS", False):
        return
    if not shard_outputs_ready(root, out_dir, years, chunk_count):
        print("[railway] auto-merge skipped: waiting for more shard outputs")
        return

    merge_cmd = [
        sys.executable,
        "player_cards_pipeline/scripts/build_static_card_payloads.py",
        "--project-root",
        ".",
        "--years",
        ",".join(years),
        "--out-dir",
        out_dir,
        "--chunk-count",
        str(chunk_count),
        "--merge-shards",
    ]
    print(f"[railway] auto-merge command: {' '.join(merge_cmd)}")
    subprocess.run(merge_cmd, cwd=root, check=True)


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    years_spec = os.getenv("YEARS", "2026")
    out_dir = os.getenv("OUT_DIR", "player_cards_pipeline/public/cards")
    cmd = [
        sys.executable,
        "player_cards_pipeline/scripts/build_static_card_payloads.py",
        "--project-root",
        ".",
        "--out-dir",
        out_dir,
        "--years",
        years_spec,
        "--min-games",
        str(env_int("MIN_GAMES", 5)),
    ]

    limit = env_int("LIMIT", 0)
    if limit > 0:
        cmd.extend(["--limit", str(limit)])

    if env_flag("INCREMENTAL", True):
        cmd.append("--incremental")

    chunk_count = env_int("CHUNK_COUNT", 0)
    if chunk_count > 0:
        cmd.extend(["--chunk-count", str(chunk_count)])

    chunk_index = os.getenv("CHUNK_INDEX", "").strip()
    if chunk_index:
        cmd.extend(["--chunk-index", chunk_index])

    checkpoint_every = env_int("CHECKPOINT_EVERY", 25)
    if checkpoint_every > 0:
        cmd.extend(["--checkpoint-every", str(checkpoint_every)])

    if env_flag("WRITE_SHARD_FILES", False):
        cmd.append("--write-shard-files")

    targets_file = os.getenv("TARGETS_FILE", "").strip()
    if targets_file:
        cmd.extend(["--targets-file", targets_file])

    if env_flag("MERGE_SHARDS", False):
        cmd.append("--merge-shards")

    print(f"[railway] running payload job from {root}")
    print(f"[railway] command: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=root, check=True)
    maybe_run_auto_merge(root, out_dir, parse_years(years_spec), chunk_count)


if __name__ == "__main__":
    main()
