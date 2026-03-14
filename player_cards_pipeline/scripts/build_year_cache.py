#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc


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


def init_db(path: Path, season: str, min_games: int) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("DROP TABLE IF EXISTS metadata")
    conn.execute("DROP TABLE IF EXISTS card_cache")
    conn.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("CREATE TABLE card_cache (cache_key TEXT PRIMARY KEY, payload_json TEXT NOT NULL)")
    meta = {
        "schema_version": str(bpc.CACHE_SCHEMA_VERSION),
        "season": str(season),
        "min_games": str(min_games),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    conn.executemany("INSERT INTO metadata(key, value) VALUES(?, ?)", list(meta.items()))
    conn.commit()
    return conn


def settings_paths(project_root: Path) -> tuple[Path, dict[str, Any]]:
    settings_path = project_root / "player_cards_pipeline" / "config" / "settings.json"
    settings = json.loads(settings_path.read_text(encoding="utf-8"))
    return settings_path, settings


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def build_year_cache(
    project_root: Path,
    year: str,
    min_games: int,
    bt_rows: list[dict[str, str]],
    players_all: list[bpc.PlayerGameStats],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    rsci_map: dict[str, int],
    adv_rows_by_year: dict[str, list[dict[str, str]]],
    bt_playerstat_template: str,
    out_db: Path,
    limit: int = 0,
) -> None:
    ys = bpc.norm_season(year)
    players = [p for p in players_all if bpc.norm_season(p.season) == ys]
    if limit > 0:
        players = players[:limit]
    conn = init_db(out_db, ys, min_games)

    bt_playerstat_rows: list[dict[str, Any]] = []
    local_ps = (
        project_root
        / "player_cards_pipeline"
        / "data"
        / "bt"
        / "raw_playerstat_json"
        / f"{ys}_pbp_playerstat_array.json"
    )
    if local_ps.exists():
        try:
            bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(str(local_ps))
        except Exception:
            bt_playerstat_rows = []
    if not bt_playerstat_rows:
        try:
            bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(bt_playerstat_template.format(year=ys))
        except Exception:
            bt_playerstat_rows = []

    adv_rows = adv_rows_by_year.get(ys, [])

    t0 = time.perf_counter()
    print(f"[cache] {ys}: players={len(players)}")
    with conn:
        for idx, target in enumerate(players, start=1):
            bt_percentiles_html = bpc.build_bt_percentile_html(target, bt_rows, adv_rows, [])
            grade_boxes_html = bpc.build_grade_boxes_html(target, bt_rows)
            self_creation_html = bpc.build_self_creation_html(target, bt_rows, bt_playerstat_rows, [], pbp_games_map={})
            playstyles_html = bpc.build_playstyles_html(target, bt_rows)
            team_impact_html = bpc.build_team_impact_html(target, bt_rows)
            shot_diet_html = bpc.build_shot_diet_html(target, bt_rows)
            player_comparisons_html = bpc.build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5)
            draft_projection_html = bpc.build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map)
            bt_fgm, bt_fga = bpc.bt_fg_totals_for_target(target, bt_rows)
            per_game_pcts = bpc.build_per_game_percentiles(players_all, target, min_games, bt_rows=bt_rows)

            _act_pps, _exp_pps, pps_oe, pps_oe_pct = bpc.pps_over_expected_from_enriched(target)
            if pps_oe is not None:
                if pps_oe_pct is not None:
                    p_rank = max(1, min(99, int(round(pps_oe_pct))))
                    pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% ({bpc.ordinal(p_rank)} Percentile)"
                else:
                    pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% (Percentile N/A)"
            else:
                pps_line = "Points per Shot Over Expectation: N/A"

            payload = {
                "bt_percentiles_html": bt_percentiles_html,
                "grade_boxes_html": grade_boxes_html,
                "self_creation_html": self_creation_html,
                "playstyles_html": playstyles_html,
                "team_impact_html": team_impact_html,
                "shot_diet_html": shot_diet_html,
                "player_comparisons_html": player_comparisons_html,
                "draft_projection_html": draft_projection_html,
                "pps_line": pps_line,
                "bt_fgm": bt_fgm,
                "bt_fga": bt_fga,
                "per_game_pcts": per_game_pcts,
            }
            ck = bpc.card_cache_key(target.player, target.team, target.season)
            conn.execute(
                "INSERT OR REPLACE INTO card_cache(cache_key, payload_json) VALUES(?, ?)",
                (ck, json.dumps(payload, ensure_ascii=True)),
            )

            if idx % 100 == 0 or idx == len(players):
                elapsed = time.perf_counter() - t0
                print(f"[cache] {ys}: {idx}/{len(players)} ({elapsed:.1f}s)")

    conn.close()
    print(f"[cache] {ys}: wrote {out_db}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build card-section cache sqlite DB(s) for player-card rendering.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--years", required=True, help="Years spec, e.g. 2010-2025 or 2026 or 2019,2020,2021")
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--gender", default="Women", help="Enriched gender token (Women or Men).")
    ap.add_argument("--out-dir", default="player_cards_pipeline/data/cache/card_sections")
    ap.add_argument("--limit", type=int, default=0, help="Optional max players per season (testing).")
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    bpc.ENRICHED_GENDER = bpc.enriched_gender_token(args.gender)
    years = parse_years(args.years)
    if not years:
        raise SystemExit("No years parsed from --years")

    _settings_path, settings = settings_paths(project_root)
    bt_csv = rel_to_pipeline(project_root, settings["bt_advstats_csv"])
    _h, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)

    players_all = bpc.build_player_pool_from_bt(bt_rows)

    bio_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    bio_rel = settings.get("bio_csv", "")
    if bio_rel:
        bio_path = rel_to_pipeline(project_root, bio_rel)
        if bio_path.exists():
            bio_lookup = bpc.load_bio_lookup(bio_path)

    rsci_path = (
        project_root
        / "player_cards_pipeline"
        / "data"
        / "manual"
        / "rsci"
        / "rsci_rankings.csv"
    )
    rsci_map = bpc.load_rsci_rankings(rsci_path) if rsci_path.exists() else {}

    adv_rows_by_year: dict[str, list[dict[str, str]]] = {}
    adv_map = settings.get("advgames_csv_by_year", {}) or {}
    for y in years:
        rel = adv_map.get(y)
        if not rel:
            adv_rows_by_year[y] = []
            continue
        p = rel_to_pipeline(project_root, rel)
        if p.exists():
            _ah, rows = bpc.read_csv_rows(p)
            adv_rows_by_year[y] = rows
        else:
            adv_rows_by_year[y] = []

    bt_playerstat_template = settings.get(
        "bt_playerstat_url_template",
        "https://barttorvik.com/ncaaw/{year}_pbp_playerstat_array.json",
    )

    out_dir = project_root / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    for y in years:
        out_db = out_dir / f"{bpc.norm_season(y)}.sqlite"
        build_year_cache(
            project_root=project_root,
            year=y,
            min_games=args.min_games,
            bt_rows=bt_rows,
            players_all=players_all,
            bio_lookup=bio_lookup,
            rsci_map=rsci_map,
            adv_rows_by_year=adv_rows_by_year,
            bt_playerstat_template=bt_playerstat_template,
            out_db=out_db,
            limit=args.limit,
        )


if __name__ == "__main__":
    main()
