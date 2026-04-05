#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg
from psycopg.rows import dict_row

import cbb_player_cards_v1.build_player_card as bpc
import player_cards_pipeline.scripts.build_static_card_payloads as bsp

BASE_METADATA_PHASE = "base_metadata"
PER_GAME_PHASE = "per_game_percentiles"
SECTION_PHASES = list(bsp.SECTION_ORDER)
ALL_PHASES = [BASE_METADATA_PHASE, PER_GAME_PHASE, *[phase for phase in SECTION_PHASES if phase != PER_GAME_PHASE]]
BATCH_SIZE = 100


def log(message: str) -> None:
    print(message, flush=True)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Apply targeted payload phases directly into Supabase")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--season", default="2026")
    ap.add_argument("--gender", required=True, choices=["men", "women"])
    ap.add_argument("--targets-file", required=True)
    ap.add_argument("--phases", required=True, help="Comma-separated phases to refresh")
    ap.add_argument("--chunk-count", type=int, default=1)
    ap.add_argument("--chunk-index", type=int, default=0)
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--db-url", default="")
    return ap.parse_args()


def resolve_phase_list(raw: str) -> list[str]:
    phases = [part.strip() for part in raw.split(",") if part.strip()]
    unknown = [phase for phase in phases if phase not in ALL_PHASES]
    if unknown:
        raise RuntimeError(f"Unknown phases: {', '.join(unknown)}")
    return phases


def build_base_patch(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    top_level = {
        "schema_version": payload.get("schema_version"),
        "generated_at_utc": payload.get("generated_at_utc"),
        "source_hash": payload.get("source_hash"),
        "player": payload.get("player"),
        "team": payload.get("team"),
        "season": payload.get("season"),
        "bio": payload.get("bio") or {},
        "shot_chart": payload.get("shot_chart") or {},
    }
    per_game = payload.get("per_game") or {}
    per_game_base = {
        "ppg": per_game.get("ppg"),
        "rpg": per_game.get("rpg"),
        "apg": per_game.get("apg"),
        "spg": per_game.get("spg"),
        "bpg": per_game.get("bpg"),
        "fg_pct": per_game.get("fg_pct"),
        "tp_pct": per_game.get("tp_pct"),
        "ft_pct": per_game.get("ft_pct"),
    }
    return top_level, per_game_base


def merge_payload(
    existing_payload: dict[str, Any] | None,
    *,
    cache_key: str,
    base_top: dict[str, Any] | None,
    per_game_base: dict[str, Any] | None,
    per_game_percentiles: dict[str, Any] | None,
    sections_html: dict[str, str] | None,
) -> dict[str, Any]:
    payload = dict(existing_payload or {})
    if base_top:
        payload.update(base_top)

    per_game = dict(payload.get("per_game") or {})
    if per_game_base:
        per_game.update({key: value for key, value in per_game_base.items() if value is not None})
    if per_game_percentiles is not None:
        per_game["percentiles"] = per_game_percentiles
    else:
        per_game.setdefault("percentiles", dict(per_game.get("percentiles") or {}))
    payload["per_game"] = per_game

    merged_sections = dict(payload.get("sections_html") or {})
    if sections_html:
        merged_sections.update(sections_html)
    payload["sections_html"] = merged_sections
    payload["section_bundles"] = bsp.split_section_bundles(merged_sections)
    payload.setdefault("schema_version", "card_sections_v2")
    payload["cache_key"] = cache_key
    return payload


def chunked(seq: list[Any], size: int) -> list[list[Any]]:
    return [seq[index : index + size] for index in range(0, len(seq), size)]


def fetch_existing_payloads(conn: psycopg.Connection[Any], gender: str, season: int, targets: list[dict[str, Any]]) -> dict[tuple[str, str, int], dict[str, Any]]:
    rows_json = json.dumps(
        [{"player": row["player"], "team": row["team"], "season": row["season"]} for row in targets],
        ensure_ascii=True,
    )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with data as (
              select *
              from json_to_recordset(%s::json) as x(player text, team text, season int)
            )
            select p.player, p.team, p.season, p.cache_key, p.payload_json, p.source_hash
            from public.player_payload_index p
            join data
              on p.player = data.player
             and p.team = data.team
             and p.season = data.season
            where p.gender = %s
              and p.season = %s
            """,
            (rows_json, gender, season),
        )
        result = cur.fetchall()
    return {
        (str(row["player"]), str(row["team"]), int(row["season"])): {
            "cache_key": row.get("cache_key") or "",
            "payload_json": row.get("payload_json") or {},
            "source_hash": row.get("source_hash") or "",
        }
        for row in result
    }


def main() -> None:
    args = parse_args()
    if args.chunk_count < 1:
        raise RuntimeError("--chunk-count must be >= 1")
    if args.chunk_index < 0 or args.chunk_index >= args.chunk_count:
        raise RuntimeError("--chunk-index must be between 0 and chunk-count-1")

    phases = resolve_phase_list(args.phases)
    project_root = Path(args.project_root).resolve()
    season = int(str(args.season).strip())
    season_key = bpc.norm_season(season)
    db_url = args.db_url.strip() or os.getenv("SUPABASE_DB_URL", "").strip()
    if not db_url:
        raise RuntimeError("Missing SUPABASE_DB_URL")

    targets_payload = load_json(Path(args.targets_file).resolve())
    if not isinstance(targets_payload, list):
        raise RuntimeError("Targets file must be a JSON list")

    settings = bsp.load_settings(project_root)
    bt_csv = bsp.rel_to_pipeline(project_root, settings["bt_advstats_csv"])
    _header, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    players_all = bpc.build_player_pool_from_bt(bt_rows)

    bio_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    bio_rel = settings.get("bio_csv", "")
    if bio_rel:
        bio_path = bsp.rel_to_pipeline(project_root, bio_rel)
        if bio_path.exists():
            bio_lookup = bpc.load_bio_lookup(bio_path)

    rsci_map: dict[str, int] = {}
    rsci_path = project_root / "player_cards_pipeline" / "data" / "manual" / "rsci" / "rsci_rankings.csv"
    if rsci_path.exists():
        rsci_map = bpc.load_rsci_rankings(rsci_path)

    adv_rows: list[dict[str, str]] = []
    adv_map = settings.get("advgames_csv_by_year", {}) or {}
    adv_rel = adv_map.get(season_key)
    if adv_rel:
        adv_path = bsp.rel_to_pipeline(project_root, adv_rel)
        if adv_path.exists():
            _adv_header, adv_rows = bpc.read_csv_rows(adv_path)

    bt_playerstat_rows: list[dict[str, Any]] = []
    local_ps = project_root / "player_cards_pipeline" / "data" / "bt" / "raw_playerstat_json" / f"{season_key}_pbp_playerstat_array.json"
    if local_ps.exists():
        try:
            bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(str(local_ps))
        except Exception:
            bt_playerstat_rows = []
    if not bt_playerstat_rows:
        bt_ps_url_template = str(settings.get("bt_playerstat_url_template", "")).strip()
        if bt_ps_url_template:
            try:
                bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(bt_ps_url_template.format(year=season_key))
            except Exception:
                bt_playerstat_rows = []

    target_filters = targets_payload
    season_players = [player for player in players_all if bpc.norm_season(player.season) == season_key]
    season_players = [player for player in season_players if bsp.target_matches_filter(player, target_filters)]
    season_players = [player for player in season_players if bsp.shard_for_cache_key(bsp.cache_key_for_target(player), args.chunk_count) == args.chunk_index]
    season_players = sorted(season_players, key=lambda player: (bpc.norm_team(player.team), bpc.norm_player_name(player.player)))
    season_players = bsp.dedupe_targets_by_cache_key(season_players)
    if args.limit > 0:
        season_players = season_players[: args.limit]

    if not season_players:
        log(f"[supabase-refresh] no targets for gender={args.gender} season={season} shard={args.chunk_index + 1}/{args.chunk_count}")
        return

    per_game_percentiles_map: dict[str, dict[str, float | None]] = {}
    if PER_GAME_PHASE in phases:
        per_game_percentiles_map = bsp.build_per_game_percentiles_map(season_players, players_all, args.min_games, bt_rows=bt_rows)

    updates: list[dict[str, Any]] = []
    for index, target in enumerate(season_players, start=1):
        cache_key = bsp.cache_key_for_target(target)
        base_top = None
        per_game_base = None
        per_game_percentiles = None
        sections_html: dict[str, str] = {}

        if BASE_METADATA_PHASE in phases:
            base_payload = bsp.build_base_payload_for_target(
                target=target,
                bt_rows=bt_rows,
                adv_rows=adv_rows,
                bt_playerstat_rows=bt_playerstat_rows,
                players_all=players_all,
                bio_lookup=bio_lookup,
                rsci_map=rsci_map,
                min_games=args.min_games,
            )
            base_top, per_game_base = build_base_patch(base_payload)

        if PER_GAME_PHASE in phases:
            per_game_percentiles = per_game_percentiles_map.get(cache_key, {})

        for phase_name in SECTION_PHASES:
            if phase_name == PER_GAME_PHASE or phase_name not in phases:
                continue
            sections_html[phase_name] = str(
                bsp.build_phase_value(
                    phase_name,
                    target=target,
                    bt_rows=bt_rows,
                    adv_rows=adv_rows,
                    bt_playerstat_rows=bt_playerstat_rows,
                    players_all=players_all,
                    bio_lookup=bio_lookup,
                    rsci_map=rsci_map,
                    min_games=args.min_games,
                )
                or ""
            )

        updates.append(
            {
                "player": target.player,
                "team": target.team,
                "season": season,
                "cache_key": cache_key,
                "base_top": base_top,
                "per_game_base": per_game_base,
                "per_game_percentiles": per_game_percentiles,
                "sections_html": sections_html,
            }
        )
        if index == 1 or index % 50 == 0 or index == len(season_players):
            log(f"[supabase-refresh] built {index}/{len(season_players)} updates shard={args.chunk_index + 1}/{args.chunk_count}")

    with psycopg.connect(db_url, sslmode="require") as conn:
        total_written = 0
        for batch in chunked(updates, BATCH_SIZE):
            existing = fetch_existing_payloads(conn, args.gender, season, batch)
            upsert_rows: list[tuple[Any, ...]] = []
            for update in batch:
                key = (update["player"], update["team"], int(update["season"]))
                current = existing.get(key, {})
                existing_payload = current.get("payload_json") if isinstance(current, dict) else {}
                merged = merge_payload(
                    existing_payload if isinstance(existing_payload, dict) else {},
                    cache_key=update["cache_key"],
                    base_top=update.get("base_top"),
                    per_game_base=update.get("per_game_base"),
                    per_game_percentiles=update.get("per_game_percentiles"),
                    sections_html=update.get("sections_html") or {},
                )
                source_hash = str((update.get("base_top") or {}).get("source_hash") or current.get("source_hash") or merged.get("source_hash") or "")
                upsert_rows.append(
                    (
                        args.gender,
                        season,
                        update["team"],
                        update["player"],
                        update["cache_key"],
                        source_hash,
                        json.dumps(merged, ensure_ascii=True),
                    )
                )

            with conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into public.player_payload_index (
                      gender,
                      season,
                      team,
                      player,
                      cache_key,
                      source_hash,
                      storage_provider,
                      payload_json,
                      updated_at
                    ) values (
                      %s, %s, %s, %s, %s, %s, 'supabase', %s::jsonb, now()
                    )
                    on conflict (gender, season, team, player)
                    do update set
                      cache_key = excluded.cache_key,
                      source_hash = excluded.source_hash,
                      storage_provider = 'supabase',
                      payload_json = excluded.payload_json,
                      updated_at = now()
                    """,
                    upsert_rows,
                )
            conn.commit()
            total_written += len(upsert_rows)
            log(f"[supabase-refresh] wrote {total_written}/{len(updates)} rows shard={args.chunk_index + 1}/{args.chunk_count}")


if __name__ == "__main__":
    main()
