#!/usr/bin/env python3
from __future__ import annotations

import json
import os
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


def main() -> None:
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
