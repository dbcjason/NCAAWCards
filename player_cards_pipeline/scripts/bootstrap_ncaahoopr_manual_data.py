#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import json
from pathlib import Path
from subprocess import run


def season_end_year(label: str) -> str:
    # 2010-11 -> 2011
    left, right = label.split('-', 1)
    return f"20{right}" if len(right) == 2 else right


def concat_year_csvs(pbp_dir: Path, out_csv_gz: Path) -> tuple[int, int]:
    files = sorted(pbp_dir.glob('*/*.csv'))
    out_csv_gz.parent.mkdir(parents=True, exist_ok=True)
    if not files:
        return 0, 0

    n_rows = 0
    header: list[str] = []
    with gzip.open(out_csv_gz, 'wt', newline='', encoding='utf-8') as out_f:
        writer = None
        for fp in files:
            with fp.open('r', newline='', encoding='utf-8') as f:
                r = csv.DictReader(f)
                if not r.fieldnames:
                    continue
                if writer is None:
                    header = list(r.fieldnames)
                    writer = csv.DictWriter(out_f, fieldnames=header)
                    writer.writeheader()
                if list(r.fieldnames) != header:
                    # Normalize differing schema to first header seen.
                    for row in r:
                        writer.writerow({k: row.get(k, '') for k in header})
                        n_rows += 1
                else:
                    for row in r:
                        writer.writerow(row)
                        n_rows += 1
    return len(files), n_rows


def main() -> None:
    ap = argparse.ArgumentParser(description='Build yearly manual PBP assets from ncaahoopR data.')
    ap.add_argument('--ncaahoopr-root', required=True)
    ap.add_argument('--project-root', required=True)
    ap.add_argument('--year-start', type=int, default=2009, help='Season start year, e.g. 2009 for 2009-10.')
    ap.add_argument('--year-end', type=int, default=2024, help='Season start year, e.g. 2024 for 2024-25.')
    ap.add_argument('--combine-raw', action='store_true', help='Also create one compressed plays CSV per season-year.')
    args = ap.parse_args()

    src_root = Path(args.ncaahoopr_root)
    root = Path(args.project_root)
    out_base = root / 'player_cards_pipeline' / 'data' / 'manual'
    metrics_base = out_base / 'pbp_metrics'
    plays_base = out_base / 'plays_by_year'
    summary = []

    build_metrics_script = root / 'build_pbp_player_metrics_2025.py'

    for season_start in range(args.year_start, args.year_end + 1):
        season_label = f"{season_start}-{str(season_start + 1)[-2:]}"
        year = str(season_start + 1)
        pbp_dir = src_root / season_label / 'pbp_logs'
        if not pbp_dir.exists():
            summary.append({'year': year, 'season_label': season_label, 'status': 'missing_pbp_dir'})
            continue

        metrics_out = metrics_base / year / f'pbp_player_metrics_{year}.csv'
        metrics_out.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            'python3', str(build_metrics_script),
            '--pbp-root', str(pbp_dir),
            '--out-csv', str(metrics_out),
            '--season-year', year,
            '--bt-csv', str(root / 'player_cards_pipeline' / 'data' / 'bt' / 'bt_advstats_2010_2026.csv'),
        ]
        run(cmd, check=True)

        files_ct = sum(1 for _ in pbp_dir.glob('*/*.csv'))
        item = {
            'year': year,
            'season_label': season_label,
            'status': 'ok',
            'pbp_files': files_ct,
            'metrics_csv': str(metrics_out),
        }

        if args.combine_raw:
            plays_out = plays_base / year / f'plays_{year}.csv.gz'
            fc, rc = concat_year_csvs(pbp_dir, plays_out)
            item['combined_raw_csv_gz'] = str(plays_out)
            item['combined_files'] = fc
            item['combined_rows'] = rc

        summary.append(item)
        print(f"[ok] {season_label} -> {year} files={files_ct}", flush=True)

    summary_path = out_base / 'pbp_metrics_build_summary_2010_2025.json'
    summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(f'wrote {summary_path}')


if __name__ == '__main__':
    main()
