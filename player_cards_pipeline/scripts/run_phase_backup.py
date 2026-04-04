#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from player_cards_pipeline.scripts.railway_warm_worker import (
    DEFAULT_GENDER,
    backup_phase_outputs_to_endpoint,
    env_int,
    output_dir_path,
    parse_years,
    run_phase_wave,
)

VALID_PHASES = {
    "base_metadata",
    "per_game_percentiles",
    "grade_boxes_html",
    "bt_percentiles_html",
    "self_creation_html",
    "playstyles_html",
    "team_impact_html",
    "shot_diet_html",
    "player_comparisons_html",
    "draft_projection_html",
    "finalize",
}


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run a single payload phase and overwrite only that phase's Supabase backup chunks."
    )
    ap.add_argument("--phase", required=True, help="Phase to compute and back up")
    ap.add_argument("--chunk-count", type=int, default=0, help="Override CHUNK_COUNT if needed")
    ap.add_argument("--max-parallel", type=int, default=0, help="Override MAX_PARALLEL_SHARDS if needed")
    ap.add_argument("--years", default="", help="Override YEARS/WARM_SEASON if needed")
    args = ap.parse_args()

    phase = args.phase.strip()
    if phase not in VALID_PHASES:
        raise SystemExit(f"Unknown phase: {phase}")

    if args.years.strip():
        os.environ["YEARS"] = args.years.strip()
    years = os.getenv("YEARS", os.getenv("WARM_SEASON", "2026")).strip() or "2026"
    parsed_years = parse_years(years)
    chunk_count = args.chunk_count if args.chunk_count > 0 else max(1, env_int("CHUNK_COUNT", 20))
    max_parallel = args.max_parallel if args.max_parallel > 0 else max(1, env_int("MAX_PARALLEL_SHARDS", 1))
    out_dir = output_dir_path()

    print(
        f"[phase-backup] start phase={phase} years={','.join(parsed_years)} chunk_count={chunk_count} "
        f"max_parallel={max_parallel} out_dir={out_dir}"
    )
    run_phase_wave(chunk_count, max_parallel, [phase])
    backup_phase_outputs_to_endpoint(DEFAULT_GENDER, phase, chunk_count)
    print(f"[phase-backup] finished phase={phase} years={','.join(parsed_years)}")


if __name__ == "__main__":
    main()
