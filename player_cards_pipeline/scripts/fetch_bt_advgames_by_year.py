#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import json
from pathlib import Path
from urllib.request import Request, urlopen

ADVGAMES_HEADERS = [
    'numdate', 'datetext', 'opstyle', 'quality', 'win1', 'opponent', 'muid', 'win2',
    'Min_per', 'ORtg', 'Usage', 'eFG', 'TS_per', 'ORB_per', 'DRB_per', 'AST_per', 'TO_per',
    'dunksmade', 'dunksatt', 'rimmade', 'rimatt', 'midmade', 'midatt', 'twoPM', 'twoPA',
    'TPM', 'TPA', 'FTM', 'FTA', 'bpm_rd', 'Obpm', 'Dbpm', 'bpm_net', 'pts', 'ORB', 'DRB',
    'AST', 'TOV', 'STL', 'BLK', 'stl_per', 'blk_per', 'PF', 'possessions', 'bpm', 'sbpm',
    'loc', 'tt', 'pp', 'inches', 'cls', 'pid', 'year'
]


def fetch_bytes(url: str) -> bytes:
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': '*/*'})
    with urlopen(req, timeout=180) as resp:
        return resp.read()


def bart_url(prefix: str, path: str) -> str:
    p = prefix.strip().strip('/')
    if p:
        return f'https://barttorvik.com/{p}/{path.lstrip("/")}'
    return f'https://barttorvik.com/{path.lstrip("/")}'


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=ADVGAMES_HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in ADVGAMES_HEADERS})


def main() -> None:
    ap = argparse.ArgumentParser(description='Fetch Bart all_advgames and convert to labeled CSV by year.')
    ap.add_argument('--year-start', type=int, default=2010)
    ap.add_argument('--year-end', type=int, default=2026)
    ap.add_argument('--out-dir', required=True, help='project_root/player_cards_pipeline/data/bt')
    ap.add_argument('--bart-prefix', default='ncaaw', help='Bart path prefix, e.g. ncaaw or empty for men.')
    args = ap.parse_args()

    bt_dir = Path(args.out_dir)
    raw_dir = bt_dir / 'raw_advgames_json'
    out_dir = bt_dir / 'advgames_labeled'
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(args.year_start, args.year_end + 1):
        urls = [
            bart_url(args.bart_prefix, f'{year}_all_advgames.json.gz'),
            bart_url(args.bart_prefix, f'{year}_all_advgames.json'),
        ]
        payload = None
        used = None
        for u in urls:
            try:
                payload = fetch_bytes(u)
                used = u
                if payload:
                    break
            except Exception:
                continue

        if not payload:
            print(f'[warn] no advgames payload for {year}')
            continue

        if used and used.endswith('.gz'):
            text = gzip.decompress(payload).decode('utf-8', errors='replace')
        else:
            text = payload.decode('utf-8', errors='replace')

        (raw_dir / f'{year}_all_advgames.json').write_text(text, encoding='utf-8')

        try:
            arr = json.loads(text)
        except Exception as e:
            print(f'[warn] bad json year={year}: {e}')
            continue

        rows: list[dict[str, str]] = []
        for item in arr:
            if not isinstance(item, list):
                continue
            row = {}
            for i, h in enumerate(ADVGAMES_HEADERS):
                row[h] = str(item[i]) if i < len(item) and item[i] is not None else ''
            rows.append(row)

        out_csv = out_dir / f'{year}_all_advgames_labeled.csv'
        write_csv(out_csv, rows)
        print(f'[ok] year={year} rows={len(rows)} -> {out_csv}')


if __name__ == '__main__':
    main()
