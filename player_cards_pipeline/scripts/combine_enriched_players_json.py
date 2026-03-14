#!/usr/bin/env python3
"""Combine enrichedPlayers JSON tiers (Low/Medium/High) into one file per year.

Notes:
- Source naming uses "json year" (e.g., 2022).
- Card pipeline uses "script season" where script season = json year + 1
  (e.g., json year 2022 corresponds to script season 2023).
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input-dir",
        default="/Users/henryhalverson/Downloads/enrichedPlayers",
        help="Directory containing players_all_<Gender>_<year>_<tier>.json files.",
    )
    ap.add_argument(
        "--output-dir",
        default="player_cards_pipeline/data/manual/enriched_players",
        help="Repo-relative output root.",
    )
    ap.add_argument(
        "--gender",
        default="Women",
        help="Gender token in filenames: Women or Men.",
    )
    return ap.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"Expected object JSON in {path}")
    return obj


def file_meta(path: Path) -> tuple[str, str] | None:
    # players_all_Women_2024_High.json
    name = path.name
    parts = name.replace(".json", "").split("_")
    if len(parts) != 5:
        return None
    _, _, _, year, tier = parts
    if not year.isdigit():
        return None
    return year, tier


def main() -> None:
    args = parse_args()
    gender = args.gender.strip().capitalize()
    in_dir = Path(args.input_dir)
    out_root = Path(args.output_dir)
    by_json_year_dir = out_root / "by_json_year"
    by_script_season_dir = out_root / "by_script_season"
    by_json_year_dir.mkdir(parents=True, exist_ok=True)
    by_script_season_dir.mkdir(parents=True, exist_ok=True)

    files_by_year: dict[str, list[tuple[str, Path]]] = defaultdict(list)
    for p in sorted(in_dir.glob(f"players_all_{gender}_*_*.json")):
        meta = file_meta(p)
        if meta is None:
            continue
        year, tier = meta
        if tier not in {"Low", "Medium", "High"}:
            continue
        files_by_year[year].append((tier, p))

    priority = {"Low": 0, "Medium": 1, "High": 2}
    manifest: list[dict[str, Any]] = []

    for year in sorted(files_by_year.keys()):
        tier_files = sorted(files_by_year[year], key=lambda x: priority[x[0]])
        combined_players: list[dict[str, Any]] = []
        source_files: list[str] = []

        confs_union: set[str] = set()
        conf_map_merged: dict[str, Any] = {}
        last_updated = None

        for tier, path in tier_files:
            obj = load_json(path)
            players = obj.get("players", [])
            if isinstance(players, list):
                for r in players:
                    if isinstance(r, dict):
                        rr = dict(r)
                        rr["source_tier"] = tier
                        combined_players.append(rr)
            confs = obj.get("confs", [])
            if isinstance(confs, list):
                confs_union.update(str(c) for c in confs)
            conf_map = obj.get("confMap", {})
            if isinstance(conf_map, dict):
                conf_map_merged.update(conf_map)
            last_updated = obj.get("lastUpdated", last_updated)
            source_files.append(path.name)

        out_obj = {
            "lastUpdated": last_updated,
            "json_year": year,
            "script_season": str(int(year) + 1),
            "source_files": source_files,
            "confs": sorted(confs_union),
            "confMap": conf_map_merged,
            "players": combined_players,
        }

        out_json_year = by_json_year_dir / f"players_all_{gender}_{year}_combined.json"
        with out_json_year.open("w", encoding="utf-8") as f:
            json.dump(out_obj, f, ensure_ascii=False)

        out_script_season = (
            by_script_season_dir
            / f"players_all_{gender}_scriptSeason_{int(year)+1}_fromJsonYear_{year}.json"
        )
        with out_script_season.open("w", encoding="utf-8") as f:
            json.dump(out_obj, f, ensure_ascii=False)

        manifest.append(
            {
                "json_year": year,
                "script_season": int(year) + 1,
                "tiers_included": [t for t, _ in tier_files],
                "source_files": source_files,
                "players_count": len(combined_players),
                "output_json_year": str(out_json_year),
                "output_script_season": str(out_script_season),
            }
        )

    manifest_path = out_root / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Combined years: {len(manifest)}")
    print(f"Manifest: {manifest_path}")
    for row in manifest:
        print(
            f"json_year={row['json_year']} script_season={row['script_season']} "
            f"players={row['players_count']} tiers={','.join(row['tiers_included'])}"
        )


if __name__ == "__main__":
    main()
