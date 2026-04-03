#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
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


def slugify(v: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "_" for ch in (v or "").strip())
    s = "_".join([p for p in s.split("_") if p])
    return s or "player"


def load_settings(project_root: Path) -> dict[str, Any]:
    p = project_root / "player_cards_pipeline" / "config" / "settings.json"
    return json.loads(p.read_text(encoding="utf-8"))


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def find_bt_row_for_target(target: bpc.PlayerGameStats, bt_rows: list[dict[str, str]]) -> dict[str, str]:
    pk = bpc.norm_player_name(target.player)
    tk = bpc.norm_team(target.team)
    yk = bpc.norm_season(target.season)
    for r in bt_rows:
        if (
            bpc.norm_player_name(bpc.bt_get(r, ["player_name"])) == pk
            and bpc.norm_team(bpc.bt_get(r, ["team"])) == tk
            and bpc.norm_season(bpc.bt_get(r, ["year"])) == yk
        ):
            return r
    return {}


def find_enriched_row_for_target(target: bpc.PlayerGameStats) -> dict[str, Any]:
    players = bpc.load_enriched_players_for_script_season(target.season)
    if not players:
        return {}
    pk = bpc.norm_player_name(target.player)
    tk = bpc.norm_team(target.team)
    yk = bpc.norm_season(target.season)
    for p in players:
        if (
            bpc.norm_player_name(p.get("key", "")) == pk
            and bpc.norm_team(p.get("team", "")) == tk
            and bpc.norm_season(p.get("year", "")) == yk
        ):
            return p
    for p in players:
        if (
            bpc.norm_player_name(p.get("key", "")) == pk
            and bpc.norm_season(p.get("year", "")) == yk
        ):
            return p
    return {}


def stable_hash_payload(parts: dict[str, Any]) -> str:
    raw = json.dumps(parts, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_payload_for_target(
    target: bpc.PlayerGameStats,
    bt_rows: list[dict[str, str]],
    adv_rows: list[dict[str, str]],
    bt_playerstat_rows: list[dict[str, Any]],
    players_all: list[bpc.PlayerGameStats],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    rsci_map: dict[str, int],
    min_games: int,
) -> dict[str, Any]:
    bt_row = find_bt_row_for_target(target, bt_rows)
    enriched_row = find_enriched_row_for_target(target)

    shots, shot_makes, shot_attempts = ([], 0, 0)
    if enriched_row:
        shots, shot_makes, shot_attempts = bpc.build_shots_from_enriched_player_row(enriched_row)
    shot_fg_pct = (100.0 * shot_makes / shot_attempts) if shot_attempts else 0.0

    _act_pps, _exp_pps, pps_oe, pps_oe_pct = bpc.pps_over_expected_from_enriched(target)
    if pps_oe is not None:
        if pps_oe_pct is not None:
            p_rank = max(1, min(99, int(round(pps_oe_pct))))
            pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% ({bpc.ordinal(p_rank)} Percentile)"
        else:
            pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% (Percentile N/A)"
    else:
        pps_line = "Points per Shot Over Expectation: N/A"

    per_game_pcts = bpc.build_per_game_percentiles(players_all, target, min_games, bt_rows=bt_rows)

    bio = dict(bpc.lookup_bio_fallback(bio_lookup, target.player, target.team, target.season))
    rsci_rank = rsci_map.get(bpc.norm_player_name(target.player))
    rsci_display = f"{bpc.ordinal(rsci_rank)}" if rsci_rank else "Unranked"

    src_hash = stable_hash_payload(
        {
            "bt": bt_row,
            "adv_count": len(adv_rows),
            "enriched": enriched_row,
            "season": target.season,
            "player": target.player,
            "team": target.team,
        }
    )

    payload = {
        "schema_version": "card_payload_v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_hash": src_hash,
        "player": target.player,
        "team": target.team,
        "season": target.season,
        "bio": {
            "position": bio.get("position", ""),
            "height": bio.get("height", ""),
            "dob": bio.get("dob", ""),
            "age_june25": bpc.age_on_june25_for_season(bio.get("dob", ""), target.season),
            "rsci": rsci_display,
        },
        "per_game": {
            "ppg": target.ppg,
            "rpg": target.rpg,
            "apg": target.apg,
            "spg": target.spg,
            "bpg": target.bpg,
            "fg_pct": target.fg_pct,
            "tp_pct": target.tp_pct,
            "ft_pct": target.ft_pct,
            "percentiles": per_game_pcts,
        },
        "shot_chart": {
            "attempts": shot_attempts,
            "makes": shot_makes,
            "fg_pct": shot_fg_pct,
            "pps_over_expectation_line": pps_line,
            "shots": shots,
        },
        "sections_html": {
            "grade_boxes_html": bpc.build_grade_boxes_html(target, bt_rows),
            "bt_percentiles_html": bpc.build_bt_percentile_html(target, bt_rows, adv_rows, []),
            "self_creation_html": bpc.build_self_creation_html(target, bt_rows, bt_playerstat_rows, [], pbp_games_map={}),
            "playstyles_html": bpc.build_playstyles_html(target, bt_rows),
            "team_impact_html": bpc.build_team_impact_html(target, bt_rows),
            "shot_diet_html": bpc.build_shot_diet_html(target, bt_rows),
            "player_comparisons_html": bpc.build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5),
            "draft_projection_html": bpc.build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map),
        },
    }
    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description="Build static player-card payload JSONs (additive pipeline).")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--years", required=True, help="Years spec, e.g. 2019-2025 or 2026")
    ap.add_argument("--out-dir", default="player_cards_pipeline/public/cards")
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--incremental", action="store_true", help="Skip unchanged players using per-year manifest hashes.")
    ap.add_argument("--limit", type=int, default=0, help="Optional max players per year (testing).")
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    years = parse_years(args.years)
    if not years:
        raise SystemExit("No years parsed from --years")

    settings = load_settings(project_root)
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

    rsci_map: dict[str, int] = {}
    rsci_path = project_root / "player_cards_pipeline" / "data" / "manual" / "rsci" / "rsci_rankings.csv"
    if rsci_path.exists():
        rsci_map = bpc.load_rsci_rankings(rsci_path)

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

    out_root = project_root / args.out_dir
    out_root.mkdir(parents=True, exist_ok=True)

    for y in years:
        ys = bpc.norm_season(y)
        year_dir = out_root / ys
        year_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = year_dir / "manifest.json"
        index_path = year_dir / "index.json"
        prior_manifest: dict[str, str] = {}
        if args.incremental and manifest_path.exists():
            try:
                prior_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                prior_manifest = {}

        bt_playerstat_rows: list[dict[str, Any]] = []
        local_ps = project_root / "player_cards_pipeline" / "data" / "bt" / "raw_playerstat_json" / f"{ys}_pbp_playerstat_array.json"
        if local_ps.exists():
            try:
                bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(str(local_ps))
            except Exception:
                bt_playerstat_rows = []
        if not bt_playerstat_rows:
            print(f"[payload] {ys}: local playerstat JSON missing, continuing without playerstat rows")

        adv_rows = adv_rows_by_year.get(ys, [])
        year_players = [p for p in players_all if bpc.norm_season(p.season) == ys]
        year_players = sorted(year_players, key=lambda p: (bpc.norm_team(p.team), bpc.norm_player_name(p.player)))
        if args.limit > 0:
            year_players = year_players[: args.limit]

        print(f"[payload] {ys}: players={len(year_players)}")
        new_manifest: dict[str, str] = {}
        index_rows: list[dict[str, str]] = []
        built = 0
        skipped = 0
        for i, target in enumerate(year_players, start=1):
            payload = build_payload_for_target(
                target=target,
                bt_rows=bt_rows,
                adv_rows=adv_rows,
                bt_playerstat_rows=bt_playerstat_rows,
                players_all=players_all,
                bio_lookup=bio_lookup,
                rsci_map=rsci_map,
                min_games=args.min_games,
            )
            ck = bpc.card_cache_key(target.player, target.team, target.season)
            src_hash = str(payload.get("source_hash", ""))
            new_manifest[ck] = src_hash
            player_slug = slugify(target.player)
            team_slug = slugify(target.team)
            rel_path = f"{team_slug}__{player_slug}.json"
            out_path = year_dir / rel_path
            index_rows.append(
                {
                    "player": target.player,
                    "team": target.team,
                    "season": ys,
                    "cache_key": ck,
                    "source_hash": src_hash,
                    "path": rel_path,
                }
            )

            if args.incremental and prior_manifest.get(ck) == src_hash and out_path.exists():
                skipped += 1
            else:
                out_path.write_text(json.dumps(payload, ensure_ascii=True, separators=(",", ":")), encoding="utf-8")
                built += 1

            if i % 100 == 0 or i == len(year_players):
                print(f"[payload] {ys}: {i}/{len(year_players)} built={built} skipped={skipped}")

        manifest_path.write_text(json.dumps(new_manifest, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
        index_path.write_text(json.dumps(index_rows, ensure_ascii=True, indent=2), encoding="utf-8")
        print(f"[payload] {ys}: wrote index={index_path} manifest={manifest_path} built={built} skipped={skipped}")


if __name__ == "__main__":
    main()
