#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from urllib.request import Request, urlopen

BT_ADV_HEADERS = [
    "player_name", "team", "conf", "GP", "Min_per", "ORtg", "usg", "eFG", "TS_per", "ORB_per", "DRB_per",
    "AST_per", "TO_per", "FTM", "FTA", "FT_per", "twoPM", "twoPA", "twoP_per", "TPM", "TPA", "TP_per",
    "blk_per", "stl_per", "ftr", "yr", "ht", "num", "porpag", "adjoe", "pfr", "year", "pid", "type",
    "Rec Rank", "ast/tov", "rimmade", "rimmade+rimmiss", "midmade", "midmade+midmiss",
    "rimmade/(rimmade+rimmiss)", "midmade/(midmade+midmiss)", "dunksmade", "dunksmiss+dunksmade",
    "dunksmade/(dunksmade+dunksmiss)", "pick", "drtg", "adrtg", "dporpag", "stops", "bpm", "obpm", "dbpm",
    "gbpm", "mp", "ogbpm", "dgbpm", "oreb", "dreb", "treb", "ast", "stl", "blk", "pts", "role", "3p/100?", "dob",
]


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,application/json,*/*"})
    with urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8", errors="replace")


def bart_url(prefix: str, path: str) -> str:
    p = prefix.strip().strip("/")
    if p:
        return f"https://barttorvik.com/{p}/{path.lstrip('/')}"
    return f"https://barttorvik.com/{path.lstrip('/')}"


def parse_advstats_text(text: str) -> tuple[list[str], list[list[str]]]:
    rows = [r for r in csv.reader(text.splitlines()) if r]
    if not rows:
        return [], []
    # getadvstats.php returns data rows without a header row.
    return list(BT_ADV_HEADERS), rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Bart Torvik history (advstats CSV + playerstat JSON) for 2010..2025.")
    ap.add_argument("--year-start", type=int, default=2010)
    ap.add_argument("--year-end", type=int, default=2025)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--bart-prefix", default="ncaaw", help="Bart path prefix, e.g. ncaaw or empty for men.")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    bt_dir = out_dir / "data" / "bt"
    raw_dir = bt_dir / "raw_playerstat_json"
    trank_dir = bt_dir / "trank_by_year"
    bt_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    trank_dir.mkdir(parents=True, exist_ok=True)

    adv_out = bt_dir / f"bt_advstats_{args.year_start}_{args.year_end}.csv"
    playerstat_out = bt_dir / f"bt_playerstat_{args.year_start}_{args.year_end}.csv"
    trank_out = bt_dir / f"trank_{args.year_start}_{args.year_end}.csv"

    adv_header: list[str] = []
    adv_rows: list[list[str]] = []
    trank_header: list[str] = []
    trank_rows: list[dict[str, str]] = []

    playerstat_header = [
        "year", "pid", "player", "team",
        "rim_made", "rim_miss", "rim_assisted",
        "mid_made", "mid_miss", "mid_assisted",
        "three_made", "three_miss", "three_assisted",
        "dunks_made", "dunks_miss", "dunks_assisted",
    ]
    playerstat_rows: list[list[str]] = []

    for year in range(args.year_start, args.year_end + 1):
        # 1) Advstats CSV
        adv_urls = [
            bart_url(args.bart_prefix, f"getadvstats.php?year={year}&csv=1"),
            bart_url(args.bart_prefix, f"getadvstats.php?year={year}"),
            bart_url("", f"getadvstats.php?year={year}&csv=1"),
        ]
        try:
            h: list[str] = []
            rows: list[list[str]] = []
            used_url = ""
            for csv_url in adv_urls:
                text = fetch_text(csv_url)
                h_try, rows_try = parse_advstats_text(text)
                if h_try and rows_try:
                    h, rows = h_try, rows_try
                    used_url = csv_url
                    break
            if h and not adv_header:
                adv_header = h + ["bt_fetch_year"]
            if h and not trank_header:
                trank_header = list(h)
                if "trank_year" not in trank_header:
                    trank_header.append("trank_year")
            if h:
                year_rows: list[dict[str, str]] = []
                for r in rows:
                    rr = r + [""] * max(0, len(h) - len(r))
                    adv_rows.append(rr[:len(h)] + [str(year)])
                    d = {h[i]: rr[i] for i in range(len(h))}
                    d["trank_year"] = str(year)
                    year_rows.append(d)
                    trank_rows.append(d)
                with (trank_dir / f"trank_{year}.csv").open("w", newline="", encoding="utf-8") as yf:
                    yw = csv.DictWriter(yf, fieldnames=trank_header)
                    yw.writeheader()
                    yw.writerows([{k: row.get(k, "") for k in trank_header} for row in year_rows])
            if rows:
                print(f"[ok] advstats {year} rows={len(rows)} url={used_url}")
            else:
                print(f"[warn] advstats {year} empty across attempted URLs")
        except Exception as e:
            print(f"[warn] advstats {year} failed: {e}")

        # 2) Playerstat JSON array
        json_url = bart_url(args.bart_prefix, f"{year}_pbp_playerstat_array.json")
        try:
            raw = fetch_text(json_url)
            (raw_dir / f"{year}_pbp_playerstat_array.json").write_text(raw, encoding="utf-8")
            arr = json.loads(raw)
            kept = 0
            for item in arr:
                if not isinstance(item, list) or len(item) < 15:
                    continue
                row = [
                    str(year), str(item[0]), str(item[1]), str(item[2]),
                    str(item[3]), str(item[4]), str(item[5]),
                    str(item[6]), str(item[7]), str(item[8]),
                    str(item[9]), str(item[10]), str(item[11]),
                    str(item[12]), str(item[13]), str(item[14]),
                ]
                playerstat_rows.append(row)
                kept += 1
            print(f"[ok] playerstat {year} rows={kept}")
        except Exception as e:
            print(f"[warn] playerstat {year} failed: {e}")

    if adv_header:
        with adv_out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(adv_header)
            w.writerows(adv_rows)
        print(f"wrote {adv_out} rows={len(adv_rows)}")

    with playerstat_out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(playerstat_header)
        w.writerows(playerstat_rows)
    print(f"wrote {playerstat_out} rows={len(playerstat_rows)}")

    if trank_header:
        with trank_out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=trank_header)
            w.writeheader()
            w.writerows([{k: row.get(k, "") for k in trank_header} for row in trank_rows])
        print(f"wrote {trank_out} rows={len(trank_rows)}")


if __name__ == "__main__":
    main()
