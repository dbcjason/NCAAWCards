#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import subprocess
from pathlib import Path


def run(cmd: list[str], cwd: Path) -> None:
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(cwd), check=True)


def count_matched_teams(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0
    with csv_path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return sum(1 for _ in r)


def main() -> None:
    ap = argparse.ArgumentParser(description="Chunked 2026 plays pull (regular + postseason) for all teams.")
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--teams-csv", required=True)
    ap.add_argument("--team-col", default="team")
    ap.add_argument("--chunk-size", type=int, default=30)
    ap.add_argument("--max-csv-mb", type=float, default=95.0)
    ap.add_argument("--max-requests", type=int, default=3000)
    ap.add_argument("--sleep-sec", type=float, default=0.15)
    args = ap.parse_args()

    root = Path(args.project_root)
    pull_script = root / "pull_cbbd_lineups_plays_only_chunked_tmp.py"

    # warm-up run for first chunk, also writes target_teams_matched.csv
    run(
        [
            "python3", str(pull_script),
            "--year", "2026",
            "--teams-csv", args.teams_csv,
            "--team-col", args.team_col,
            "--season-type", "both",
            "--datasets", "plays",
            "--team-start", "1",
            "--team-end", str(args.chunk_size),
            "--chunk-tag", f"chunk001_{args.chunk_size:03d}",
            "--max-csv-mb", str(args.max_csv_mb),
            "--max-requests", str(args.max_requests),
            "--sleep-sec", str(args.sleep_sec),
        ],
        cwd=root,
    )

    matched_csv = root / "cbbd_seasons" / "2025-2026" / "tables" / "target_teams_matched.csv"
    total = count_matched_teams(matched_csv)
    if total <= args.chunk_size:
        # Merge once and exit.
        run(
            [
                "python3", str(pull_script),
                "--year", "2026",
                "--teams-csv", args.teams_csv,
                "--team-col", args.team_col,
                "--season-type", "both",
                "--datasets", "plays",
                "--merge-only",
                "--merge-chunks",
                "--max-csv-mb", str(args.max_csv_mb),
            ],
            cwd=root,
        )
        return

    n_chunks = math.ceil(total / args.chunk_size)
    for idx in range(2, n_chunks + 1):
        s = (idx - 1) * args.chunk_size + 1
        e = min(idx * args.chunk_size, total)
        tag = f"chunk{s:03d}_{e:03d}"
        run(
            [
                "python3", str(pull_script),
                "--year", "2026",
                "--teams-csv", args.teams_csv,
                "--team-col", args.team_col,
                "--season-type", "both",
                "--datasets", "plays",
                "--team-start", str(s),
                "--team-end", str(e),
                "--chunk-tag", tag,
                "--max-csv-mb", str(args.max_csv_mb),
                "--max-requests", str(args.max_requests),
                "--sleep-sec", str(args.sleep_sec),
            ],
            cwd=root,
        )

    # merge all chunk files into canonical tables (with safe split if oversized)
    run(
        [
            "python3", str(pull_script),
            "--year", "2026",
            "--teams-csv", args.teams_csv,
            "--team-col", args.team_col,
            "--season-type", "both",
            "--datasets", "plays",
            "--merge-only",
            "--merge-chunks",
            "--max-csv-mb", str(args.max_csv_mb),
        ],
        cwd=root,
    )


if __name__ == "__main__":
    main()
