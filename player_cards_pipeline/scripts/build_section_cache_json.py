#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any


SECTION_NAMES = {
    "grade_boxes_html",
    "bt_percentiles_html",
    "self_creation_html",
    "playstyles_html",
    "team_impact_html",
    "shot_diet_html",
    "player_comparisons_html",
    "draft_projection_html",
}


def load_card_module(project_root: Path):
    mod_path = project_root / "cbb_player_cards_v1" / "build_player_card.py"
    spec = importlib.util.spec_from_file_location("build_player_card", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


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


def select_shard_rows(rows: list[tuple[Any, dict[str, str]]], chunk_count: int, chunk_index: int):
    if chunk_count <= 1:
        return rows
    return [row for idx, row in enumerate(rows) if idx % chunk_count == chunk_index]


def load_target_keys(bpc: Any, project_root: Path, targets_file: str) -> set[str] | None:
    if not targets_file.strip():
        return None
    path = Path(targets_file)
    if not path.is_absolute():
        path = project_root / path
    if not path.exists():
        print(f"[section-cache] targets file not found; continuing without target filter: {path}", flush=True)
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        keys = {
            str(item.get("cache_key") or "").strip()
            for item in payload
            if isinstance(item, dict) and str(item.get("cache_key") or "").strip()
        }
        if keys:
            return keys
        teams = {
            f"::team::{bpc.norm_team(item.get('team', ''))}"
            for item in payload
            if isinstance(item, dict) and str(item.get("team") or "").strip()
        }
        return teams or None
    return None


def compute_section_html(
    bpc: Any,
    section: str,
    target: Any,
    bt_rows: list[dict[str, str]],
    adv_rows: list[dict[str, str]],
    bt_playerstat_rows: list[dict[str, Any]],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    rsci_map: dict[str, int],
) -> str:
    if section == "grade_boxes_html":
        return bpc.build_grade_boxes_html(target, bt_rows)
    if section == "bt_percentiles_html":
        return bpc.build_bt_percentile_html(target, bt_rows, adv_rows, [])
    if section == "self_creation_html":
        return bpc.build_self_creation_html(target, bt_rows, bt_playerstat_rows, [], pbp_games_map={})
    if section == "playstyles_html":
        return bpc.build_playstyles_html(target, bt_rows)
    if section == "team_impact_html":
        return bpc.build_team_impact_html(target, bt_rows)
    if section == "shot_diet_html":
        return bpc.build_shot_diet_html(target, bt_rows)
    if section == "player_comparisons_html":
        return bpc.build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5)
    if section == "draft_projection_html":
        sig = inspect.signature(bpc.build_draft_projection_html)
        if len(sig.parameters) >= 5:
            return bpc.build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map, {})
        return bpc.build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map)
    raise ValueError(f"Unsupported section: {section}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build one section JSON cache for player-card rendering.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--years", required=True)
    ap.add_argument("--section", required=True, choices=sorted(SECTION_NAMES))
    ap.add_argument("--season", default="", help="Optional single-season override.")
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--chunk-count", type=int, default=1)
    ap.add_argument("--chunk-index", type=int, default=0)
    ap.add_argument("--targets-file", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument(
        "--out-json",
        default="",
        help="Optional explicit output path. Defaults to player_cards_pipeline/data/cache/section_payloads/<section>/<season>.json",
    )
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    bpc = load_card_module(project_root)
    years = parse_years(args.season or args.years)
    if not years:
        raise SystemExit("No seasons provided.")

    settings = json.loads((project_root / "player_cards_pipeline" / "config" / "settings.json").read_text(encoding="utf-8"))
    bt_csv = project_root / "player_cards_pipeline" / settings["bt_advstats_csv"]
    _h, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)

    players_all = bpc.build_player_pool_from_bt(bt_rows)
    bio_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    bio_rel = settings.get("bio_csv", "")
    if bio_rel:
        bio_path = project_root / "player_cards_pipeline" / bio_rel
        if bio_path.exists():
            bio_lookup = bpc.load_bio_lookup(bio_path)

    rsci_path = project_root / "player_cards_pipeline" / "data" / "manual" / "rsci" / "rsci_rankings.csv"
    rsci_map = bpc.load_rsci_rankings(rsci_path) if rsci_path.exists() else {}

    adv_rows_by_year: dict[str, list[dict[str, str]]] = {}
    adv_map = settings.get("advgames_csv_by_year", {}) or {}
    for y in years:
        rel = adv_map.get(y)
        if not rel:
            adv_rows_by_year[y] = []
            continue
        p = project_root / "player_cards_pipeline" / rel
        if p.exists():
            _ah, rows = bpc.read_csv_rows(p)
            adv_rows_by_year[y] = rows
        else:
            adv_rows_by_year[y] = []

    out_rows: dict[str, str] = {}
    target_keys = load_target_keys(bpc, project_root, args.targets_file)
    season = str(args.season or years[0])

    for y in years:
        ys = bpc.norm_season(y)
        season_players = [p for p in players_all if bpc.norm_season(p.season) == ys]
        if args.limit > 0:
            season_players = season_players[: args.limit]
        shard_players = select_shard_rows(
            [(p, {"season": p.season, "team": p.team, "player": p.player}) for p in season_players],
            max(1, int(args.chunk_count)),
            int(args.chunk_index),
        )
        shard_players = [(p, {}) for p, _ in shard_players]
        print(
            f"[section-cache] {args.section} season={ys} players={len(season_players)} shard={int(args.chunk_index)+1}/{max(1, int(args.chunk_count))} rows={len(shard_players)}",
            flush=True,
        )

        bt_playerstat_rows: list[dict[str, Any]] = []
        local_ps = project_root / "player_cards_pipeline" / "data" / "bt" / "raw_playerstat_json" / f"{ys}_pbp_playerstat_array.json"
        if local_ps.exists():
            try:
                bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(str(local_ps))
            except Exception:
                bt_playerstat_rows = []
        if not bt_playerstat_rows:
            template = settings.get("bt_playerstat_url_template", "https://barttorvik.com/ncaaw/{year}_pbp_playerstat_array.json")
            try:
                bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(template.format(year=ys))
            except Exception:
                bt_playerstat_rows = []

        for idx, target in enumerate([p for p, _ in shard_players], start=1):
            cache_key = bpc.card_cache_key(target.player, target.team, target.season)
            team_key = f"::team::{bpc.norm_team(target.team)}"
            if target_keys is not None and cache_key not in target_keys and team_key not in target_keys:
                continue
            html = compute_section_html(
                bpc=bpc,
                section=args.section,
                target=target,
                bt_rows=bt_rows,
                adv_rows=adv_rows_by_year.get(ys, []),
                bt_playerstat_rows=bt_playerstat_rows,
                bio_lookup=bio_lookup,
                rsci_map=rsci_map,
            )
            out_rows[cache_key] = html
            if idx == 1 or idx % 100 == 0 or idx == len(shard_players):
                print(f"[section-cache] {args.section} {ys} {idx}/{len(shard_players)}", flush=True)

    out_json = Path(args.out_json) if args.out_json else project_root / "player_cards_pipeline" / "data" / "cache" / "section_payloads" / args.section / f"{season}.json"
    if not out_json.is_absolute():
        out_json = project_root / out_json
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(
        json.dumps(
            {
                "season": season,
                "section": args.section,
                "chunk_count": max(1, int(args.chunk_count)),
                "chunk_index": int(args.chunk_index),
                "row_count": len(out_rows),
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "rows": out_rows,
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[section-cache] wrote {out_json} rows={len(out_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
