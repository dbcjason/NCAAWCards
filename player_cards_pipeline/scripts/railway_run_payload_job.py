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


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    cmd = [
        sys.executable,
        "player_cards_pipeline/scripts/build_static_card_payloads.py",
        "--project-root",
        ".",
        "--years",
        os.getenv("YEARS", "2026"),
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


if __name__ == "__main__":
    main()
