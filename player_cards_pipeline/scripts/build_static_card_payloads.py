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


def load_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def stable_hash_payload(parts: dict[str, Any]) -> str:
    raw = json.dumps(parts, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def split_section_bundles(sections_html: dict[str, str]) -> dict[str, dict[str, str]]:
    core_keys = (
        "grade_boxes_html",
        "bt_percentiles_html",
        "self_creation_html",
        "playstyles_html",
        "team_impact_html",
        "shot_diet_html",
    )
    heavy_keys = (
        "player_comparisons_html",
        "draft_projection_html",
    )
    return {
        "core": {key: sections_html[key] for key in core_keys if key in sections_html},
        "heavy": {key: sections_html[key] for key in heavy_keys if key in sections_html},
    }


def shard_tag(chunk_index: int, chunk_count: int) -> str:
    width = max(2, len(str(max(0, chunk_count - 1))))
    return f"chunk_{chunk_index:0{width}d}_of_{chunk_count:0{width}d}"


def shard_paths(year_dir: Path, chunk_index: int, chunk_count: int) -> tuple[Path, Path, Path]:
    tag = shard_tag(chunk_index, chunk_count)
    return (
        year_dir / f"manifest.{tag}.json",
        year_dir / f"index.{tag}.json",
        year_dir / f"errors.{tag}.json",
    )


def cache_key_for_target(target: bpc.PlayerGameStats) -> str:
    return bpc.card_cache_key(target.player, target.team, target.season)


def shard_for_cache_key(cache_key: str, chunk_count: int) -> int:
    digest = hashlib.sha256(cache_key.encode("utf-8")).hexdigest()
    return int(digest[:16], 16) % chunk_count


def parse_targets_file(path_str: str) -> list[dict[str, str]]:
    if not path_str:
        return []
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Targets file not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Targets file must be a JSON list")
    targets: list[dict[str, str]] = []
    for item in payload:
        if isinstance(item, str):
            targets.append({"cache_key": item.strip()})
            continue
        if not isinstance(item, dict):
            continue
        targets.append(
            {
                "cache_key": str(item.get("cache_key", "")).strip(),
                "player": str(item.get("player", "")).strip(),
                "team": str(item.get("team", "")).strip(),
                "season": str(item.get("season", "")).strip(),
            }
        )
    return [target for target in targets if any(target.values())]


def target_matches_filter(target: bpc.PlayerGameStats, target_filters: list[dict[str, str]]) -> bool:
    if not target_filters:
        return True
    candidate_cache_key = cache_key_for_target(target)
    player_key = bpc.norm_player_name(target.player)
    team_key = bpc.norm_team(target.team)
    season_key = bpc.norm_season(target.season)
    for entry in target_filters:
        if entry.get("cache_key") and entry["cache_key"] == candidate_cache_key:
            return True
        season_match = not entry.get("season") or bpc.norm_season(entry["season"]) == season_key
        team_match = not entry.get("team") or bpc.norm_team(entry["team"]) == team_key
        player_match = not entry.get("player") or bpc.norm_player_name(entry["player"]) == player_key
        if season_match and team_match and player_match:
            return True
    return False


def load_prior_manifest(manifest_path: Path, shard_manifest_path: Path, incremental: bool) -> dict[str, str]:
    if not incremental:
        return {}
    prior_manifest = load_json(manifest_path, {})
    shard_manifest = load_json(shard_manifest_path, {})
    merged: dict[str, str] = {}
    if isinstance(prior_manifest, dict):
        merged.update({str(k): str(v) for k, v in prior_manifest.items()})
    if isinstance(shard_manifest, dict):
        merged.update({str(k): str(v) for k, v in shard_manifest.items()})
    return merged


def write_checkpoint(
    *,
    manifest_path: Path,
    index_path: Path,
    error_path: Path,
    manifest: dict[str, str],
    index_rows: list[dict[str, str]],
    errors: list[dict[str, str]],
) -> None:
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
    index_path.write_text(json.dumps(index_rows, ensure_ascii=True, indent=2), encoding="utf-8")
    error_path.write_text(json.dumps(errors, ensure_ascii=True, indent=2), encoding="utf-8")


def merge_year_outputs(year_dir: Path, chunk_count: int) -> tuple[int, int]:
    merged_manifest: dict[str, str] = {}
    merged_index_rows: list[dict[str, str]] = []
    merged_errors: list[dict[str, str]] = []
    present_shards = 0
    for chunk_index in range(chunk_count):
        shard_manifest_path, shard_index_path, shard_error_path = shard_paths(year_dir, chunk_index, chunk_count)
        shard_manifest = load_json(shard_manifest_path, {})
        shard_index_rows = load_json(shard_index_path, [])
        shard_errors = load_json(shard_error_path, [])
        if shard_manifest_path.exists() or shard_index_path.exists():
            present_shards += 1
        if isinstance(shard_manifest, dict):
            merged_manifest.update({str(k): str(v) for k, v in shard_manifest.items()})
        if isinstance(shard_index_rows, list):
            merged_index_rows.extend([row for row in shard_index_rows if isinstance(row, dict)])
        if isinstance(shard_errors, list):
            merged_errors.extend([row for row in shard_errors if isinstance(row, dict)])

    merged_index_rows.sort(
        key=lambda row: (
            bpc.norm_team(str(row.get("team", ""))),
            bpc.norm_player_name(str(row.get("player", ""))),
        )
    )

    manifest_path = year_dir / "manifest.json"
    index_path = year_dir / "index.json"
    errors_path = year_dir / "errors.json"
    manifest_path.write_text(json.dumps(merged_manifest, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
    index_path.write_text(json.dumps(merged_index_rows, ensure_ascii=True, indent=2), encoding="utf-8")
    errors_path.write_text(json.dumps(merged_errors, ensure_ascii=True, indent=2), encoding="utf-8")
    return present_shards, len(merged_index_rows)


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
        if bpc.norm_player_name(p.get("key", "")) == pk and bpc.norm_season(p.get("year", "")) == yk:
            return p
    return {}


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

    sections_html = {
        "grade_boxes_html": bpc.build_grade_boxes_html(target, bt_rows),
        "bt_percentiles_html": bpc.build_bt_percentile_html(target, bt_rows, adv_rows, []),
        "self_creation_html": bpc.build_self_creation_html(target, bt_rows, bt_playerstat_rows, [], pbp_games_map={}),
        "playstyles_html": bpc.build_playstyles_html(target, bt_rows),
        "team_impact_html": bpc.build_team_impact_html(target, bt_rows),
        "shot_diet_html": bpc.build_shot_diet_html(target, bt_rows),
        "player_comparisons_html": bpc.build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5),
        "draft_projection_html": bpc.build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map),
    }

    return {
        "schema_version": "card_sections_v2",
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
        "section_bundles": split_section_bundles(sections_html),
        "sections_html": sections_html,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Build static player-card payload JSONs (additive pipeline).")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--years", required=True, help="Years spec, e.g. 2019-2025 or 2026")
    ap.add_argument("--out-dir", default="player_cards_pipeline/public/cards")
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--incremental", action="store_true", help="Skip unchanged players using manifest hashes.")
    ap.add_argument("--limit", type=int, default=0, help="Optional max players per year (testing).")
    ap.add_argument("--chunk-count", type=int, default=1, help="Total shard count for parallel workers.")
    ap.add_argument("--chunk-index", type=int, default=0, help="Zero-based shard index for this worker.")
    ap.add_argument("--checkpoint-every", type=int, default=25, help="Write shard progress every N players.")
    ap.add_argument("--write-shard-files", action="store_true", help="Write shard-specific manifest/index/error files.")
    ap.add_argument(
        "--targets-file",
        default="",
        help="Optional JSON list of cache_keys or {player,team,season} objects for changed-player runs.",
    )
    ap.add_argument("--merge-shards", action="store_true", help="Merge shard manifests/indexes into canonical files.")
    args = ap.parse_args()

    if args.chunk_count < 1:
        raise SystemExit("--chunk-count must be >= 1")
    if args.chunk_index < 0 or args.chunk_index >= args.chunk_count:
        raise SystemExit("--chunk-index must be between 0 and chunk-count-1")

    project_root = Path(args.project_root).resolve()
    years = parse_years(args.years)
    if not years:
        raise SystemExit("No years parsed from --years")

    out_root = project_root / args.out_dir
    out_root.mkdir(parents=True, exist_ok=True)

    if args.merge_shards:
        for y in years:
            ys = bpc.norm_season(y)
            year_dir = out_root / ys
            year_dir.mkdir(parents=True, exist_ok=True)
            present_shards, merged_rows = merge_year_outputs(year_dir, args.chunk_count)
            print(f"[payload] {ys}: merged {present_shards} shard files into {merged_rows} index rows")
        return

    target_filters = parse_targets_file(args.targets_file)

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

    had_errors = False
    for y in years:
        ys = bpc.norm_season(y)
        year_dir = out_root / ys
        year_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = year_dir / "manifest.json"
        index_path = year_dir / "index.json"
        shard_manifest_path, shard_index_path, shard_error_path = shard_paths(year_dir, args.chunk_index, args.chunk_count)
        prior_manifest = load_prior_manifest(manifest_path, shard_manifest_path, args.incremental)

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
        year_players = [p for p in year_players if target_matches_filter(p, target_filters)]
        year_players = [p for p in year_players if shard_for_cache_key(cache_key_for_target(p), args.chunk_count) == args.chunk_index]
        year_players = sorted(year_players, key=lambda p: (bpc.norm_team(p.team), bpc.norm_player_name(p.player)))
        if args.limit > 0:
            year_players = year_players[: args.limit]

        print(
            f"[payload] {ys}: shard={args.chunk_index + 1}/{args.chunk_count} "
            f"players={len(year_players)} targets_file={'yes' if args.targets_file else 'no'}"
        )

        new_manifest: dict[str, str] = {}
        index_rows: list[dict[str, str]] = []
        errors: list[dict[str, str]] = []
        built = 0
        skipped = 0

        for i, target in enumerate(year_players, start=1):
            ck = cache_key_for_target(target)
            player_slug = slugify(target.player)
            team_slug = slugify(target.team)
            rel_path = f"{team_slug}__{player_slug}.json"
            out_path = year_dir / rel_path

            try:
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
                src_hash = str(payload.get("source_hash", ""))
                new_manifest[ck] = src_hash
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
            except Exception as exc:
                had_errors = True
                errors.append(
                    {
                        "player": target.player,
                        "team": target.team,
                        "season": ys,
                        "cache_key": ck,
                        "error": str(exc),
                    }
                )

            if i % args.checkpoint_every == 0 or i == len(year_players):
                target_manifest_path = shard_manifest_path if args.write_shard_files else manifest_path
                target_index_path = shard_index_path if args.write_shard_files else index_path
                target_error_path = shard_error_path if args.write_shard_files else year_dir / "errors.json"
                write_checkpoint(
                    manifest_path=target_manifest_path,
                    index_path=target_index_path,
                    error_path=target_error_path,
                    manifest=new_manifest,
                    index_rows=index_rows,
                    errors=errors,
                )
                print(f"[payload] {ys}: {i}/{len(year_players)} built={built} skipped={skipped} errors={len(errors)}")

    if had_errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
