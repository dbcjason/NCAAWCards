#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description='Copy local Bart playerstat JSON files (2010..2025) into project data dir.')
    ap.add_argument('--downloads-dir', required=True)
    ap.add_argument('--project-root', required=True)
    ap.add_argument('--year-start', type=int, default=2010)
    ap.add_argument('--year-end', type=int, default=2025)
    args = ap.parse_args()

    src = Path(args.downloads_dir)
    dst = Path(args.project_root) / 'player_cards_pipeline' / 'data' / 'bt' / 'raw_playerstat_json'
    dst.mkdir(parents=True, exist_ok=True)

    copied = 0
    missing = []
    for y in range(args.year_start, args.year_end + 1):
        name = f'{y}_pbp_playerstat_array.json'
        sp = src / name
        dp = dst / name
        if sp.exists():
            shutil.copy2(sp, dp)
            copied += 1
        else:
            missing.append(name)

    print(f'copied={copied}')
    if missing:
        print('missing=' + ','.join(missing))


if __name__ == '__main__':
    main()
