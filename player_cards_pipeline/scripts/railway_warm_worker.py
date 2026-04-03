#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc


def load_settings(project_root: Path) -> dict[str, Any]:
    p = project_root / "player_cards_pipeline" / "config" / "settings.json"
    return json.loads(p.read_text(encoding="utf-8"))


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw.strip())


def build_shard_command(chunk_index: int, chunk_count: int) -> list[str]:
    years = os.getenv("YEARS", os.getenv("WARM_SEASON", "2026")).strip() or "2026"
    out_dir = os.getenv("OUT_DIR", "player_cards_pipeline/public/cards").strip() or "player_cards_pipeline/public/cards"
    cmd = [
        sys.executable,
        "player_cards_pipeline/scripts/build_static_card_payloads.py",
        "--project-root",
        ".",
        "--out-dir",
        out_dir,
        "--years",
        years,
        "--min-games",
        str(env_int("MIN_GAMES", 5)),
        "--chunk-count",
        str(chunk_count),
        "--chunk-index",
        str(chunk_index),
        "--checkpoint-every",
        str(env_int("CHECKPOINT_EVERY", 25)),
    ]
    if env_flag("INCREMENTAL", True):
        cmd.append("--incremental")
    if env_flag("WRITE_SHARD_FILES", True):
        cmd.append("--write-shard-files")
    limit = env_int("LIMIT", 0)
    if limit > 0:
        cmd.extend(["--limit", str(limit)])
    targets_file = os.getenv("TARGETS_FILE", "").strip()
    if targets_file:
        cmd.extend(["--targets-file", targets_file])
    return cmd


def run_bootstrap_worker() -> None:
    chunk_count = max(1, env_int("CHUNK_COUNT", 10))
    max_parallel = max(1, env_int("MAX_PARALLEL_SHARDS", chunk_count))
    pending = list(range(chunk_count))
    running: list[tuple[int, subprocess.Popen[str]]] = []
    failures: list[int] = []

    print(
        f"[railway-worker] bootstrap mode chunk_count={chunk_count} "
        f"max_parallel={max_parallel} years={os.getenv('YEARS', os.getenv('WARM_SEASON', '2026'))}"
    )

    while pending or running:
        while pending and len(running) < max_parallel:
            chunk_index = pending.pop(0)
            cmd = build_shard_command(chunk_index, chunk_count)
            print(f"[railway-worker] start shard {chunk_index + 1}/{chunk_count}: {' '.join(cmd)}")
            proc = subprocess.Popen(cmd, cwd=PROJECT_ROOT, text=True)
            running.append((chunk_index, proc))

        time.sleep(5)
        next_running: list[tuple[int, subprocess.Popen[str]]] = []
        for chunk_index, proc in running:
            code = proc.poll()
            if code is None:
                next_running.append((chunk_index, proc))
                continue
            if code == 0:
                print(f"[railway-worker] shard {chunk_index + 1}/{chunk_count} finished successfully")
            else:
                print(f"[railway-worker] shard {chunk_index + 1}/{chunk_count} failed with exit code {code}")
                failures.append(chunk_index)
        running = next_running

    if failures:
        raise SystemExit(f"bootstrap shards failed: {failures}")

    print("[railway-worker] bootstrap mode finished successfully")


def main() -> None:
    worker_mode = os.getenv("WORKER_MODE", "warm").strip().lower()
    if worker_mode == "bootstrap":
        run_bootstrap_worker()
        return

    heartbeat_seconds = int(os.getenv("HEARTBEAT_SECONDS", "300"))
    settings = load_settings(PROJECT_ROOT)
    bt_csv = rel_to_pipeline(PROJECT_ROOT, settings["bt_advstats_csv"])
    _header, bt_rows = bpc.read_csv_rows(bt_csv)
    print(f"[railway-worker] loaded {len(bt_rows)} BT rows")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    players_all = bpc.build_player_pool_from_bt(bt_rows)
    print(f"[railway-worker] built player pool with {len(players_all)} players")

    season = os.getenv("WARM_SEASON", "2026").strip()
    warm_players = [p for p in players_all if bpc.norm_season(p.season) == bpc.norm_season(season)]
    print(f"[railway-worker] warm season {season}: {len(warm_players)} players available")

    while True:
        print(f"[railway-worker] heartbeat season={season} players={len(warm_players)}")
        time.sleep(heartbeat_seconds)


if __name__ == "__main__":
    main()
