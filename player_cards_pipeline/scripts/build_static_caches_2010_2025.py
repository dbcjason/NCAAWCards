#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description="Build static card caches for seasons 2010-2025.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--min-games", type=int, default=5)
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    cmd = [
        "python3",
        "player_cards_pipeline/scripts/build_year_cache.py",
        "--project-root",
        str(root),
        "--years",
        "2010-2025",
        "--min-games",
        str(args.min_games),
    ]
    subprocess.run(cmd, cwd=str(root), check=True)


if __name__ == "__main__":
    main()
