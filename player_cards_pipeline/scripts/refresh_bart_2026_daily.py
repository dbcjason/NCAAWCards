#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import io
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

BT_ADV_HEADERS = [
    "player_name", "team", "conf", "GP", "Min_per", "ORtg", "usg", "eFG", "TS_per", "ORB_per", "DRB_per",
    "AST_per", "TO_per", "FTM", "FTA", "FT_per", "twoPM", "twoPA", "twoP_per", "TPM", "TPA", "TP_per",
    "blk_per", "stl_per", "ftr", "yr", "ht", "num", "porpag", "adjoe", "pfr", "year", "pid", "type",
    "Rec Rank", "ast/tov", "rimmade", "rimmade+rimmiss", "midmade", "midmade+midmiss",
    "rimmade/(rimmade+rimmiss)", "midmade/(midmade+midmiss)", "dunksmade", "dunksmiss+dunksmade",
    "dunksmade/(dunksmade+dunksmiss)", "pick", "drtg", "adrtg", "dporpag", "stops", "bpm", "obpm", "dbpm",
    "gbpm", "mp", "ogbpm", "dgbpm", "oreb", "dreb", "treb", "ast", "stl", "blk", "pts", "role", "3p/100?", "dob",
]


def fetch_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
    with urlopen(req, timeout=120) as resp:
        return resp.read()


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = list(r)
        return list(r.fieldnames or []), rows


def write_csv(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def bart_url(bart_prefix: str, path: str) -> str:
    prefix = bart_prefix.strip().strip("/")
    if prefix:
        return f"https://barttorvik.com/{prefix}/{path.lstrip('/')}"
    return f"https://barttorvik.com/{path.lstrip('/')}"


def parse_advstats_rows(text: str) -> list[list[str]]:
    parsed = [r for r in csv.reader(text.splitlines()) if r]
    if parsed and not (len(parsed) == 1 and parsed[0] and parsed[0][0].lstrip().startswith("[[")):
        return parsed

    try:
        raw = json.loads(text)
    except Exception:
        return []

    rows: list[list[str]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, list):
                rows.append([str(v) if v is not None else "" for v in item])
    return rows


def refresh_advstats_2026(bt_dir: Path, bart_prefix: str) -> None:
    urls = [
        bart_url(bart_prefix, "getadvstats.php?year=2026&csv=1"),
        bart_url(bart_prefix, "getadvstats.php?year=2026"),
    ]
    parsed: list[list[str]] = []
    for url in urls:
        try:
            txt = fetch_text(url)
            parsed = parse_advstats_rows(txt)
            if parsed:
                break
        except Exception:
            continue
    if not parsed:
        print("[warn] Empty advstats response for 2026; skipping advstats refresh")
        return

    # getadvstats.php returns data rows only (no header row).
    h26 = list(BT_ADV_HEADERS)
    rows26 = parsed
    h26_out = h26 + (["bt_fetch_year"] if "bt_fetch_year" not in h26 else [])

    out_2026 = bt_dir / "bt_advstats_2026.csv"
    out_2010_2026 = bt_dir / "bt_advstats_2010_2026.csv"
    base_2010_2025 = bt_dir / "bt_advstats_2010_2025.csv"
    trank_dir = bt_dir / "trank_by_year"
    ensure_dir(trank_dir)
    trank_2026 = trank_dir / "trank_2026.csv"
    trank_2010_2026 = bt_dir / "trank_2010_2026.csv"
    trank_2010_2025 = bt_dir / "trank_2010_2025.csv"

    rows26_dict: list[dict[str, str]] = []
    rows26_trank: list[dict[str, str]] = []
    for r in rows26:
        rr = r + [""] * max(0, len(h26) - len(r))
        d = {h26[i]: rr[i] for i in range(len(h26))}
        d["bt_fetch_year"] = "2026"
        rows26_dict.append(d)
        td = dict(d)
        td["trank_year"] = "2026"
        rows26_trank.append(td)

    write_csv(out_2026, h26_out, rows26_dict)
    trank_header = list(h26)
    if "trank_year" not in trank_header:
        trank_header.append("trank_year")
    write_csv(trank_2026, trank_header, rows26_trank)

    base_h, base_rows = read_csv(base_2010_2025)
    if not base_h:
        # Fall back to just 2026 if no base history available.
        write_csv(out_2010_2026, h26_out, rows26_dict)
        return

    header = list(base_h)
    for k in h26_out:
        if k not in header:
            header.append(k)

    merged = []
    for r in base_rows:
        if r.get("bt_fetch_year", "") == "2026":
            continue
        merged.append(r)
    merged.extend(rows26_dict)

    write_csv(out_2010_2026, header, merged)

    # Maintain a trank combined file with explicit source year.
    th, tr = read_csv(trank_2010_2025)
    if not th:
        # Backfill from bt_advstats_2010_2025.csv if needed.
        bh, br = read_csv(base_2010_2025)
        if bh:
            th = list(bh)
            if "trank_year" not in th:
                th.append("trank_year")
            tr = []
            for row in br:
                d = dict(row)
                d["trank_year"] = row.get("year", "") or row.get("bt_fetch_year", "")
                tr.append(d)

    if not th:
        write_csv(trank_2010_2026, trank_header, rows26_trank)
        return

    merged_trank_header = list(th)
    for c in trank_header:
        if c not in merged_trank_header:
            merged_trank_header.append(c)

    tr_no_2026 = [r for r in tr if (r.get("trank_year", "") or r.get("year", "")) != "2026"]
    tr_no_2026.extend(rows26_trank)
    write_csv(trank_2010_2026, merged_trank_header, tr_no_2026)


def refresh_playerstat_2026(bt_dir: Path, bart_prefix: str) -> None:
    raw_dir = bt_dir / "raw_playerstat_json"
    ensure_dir(raw_dir)

    url = bart_url(bart_prefix, "2026_pbp_playerstat_array.json")
    raw = fetch_text(url)
    (raw_dir / "2026_pbp_playerstat_array.json").write_text(raw, encoding="utf-8")

    arr = json.loads(raw)
    csv_out = bt_dir / "bt_playerstat_2026.csv"
    header = [
        "year", "pid", "player", "team",
        "rim_made", "rim_miss", "rim_assisted",
        "mid_made", "mid_miss", "mid_assisted",
        "three_made", "three_miss", "three_assisted",
        "dunks_made", "dunks_miss", "dunks_assisted",
    ]
    rows: list[dict[str, str]] = []
    for it in arr:
        if not isinstance(it, list) or len(it) < 15:
            continue
        rows.append({
            "year": "2026",
            "pid": str(it[0]),
            "player": str(it[1]),
            "team": str(it[2]),
            "rim_made": str(it[3]),
            "rim_miss": str(it[4]),
            "rim_assisted": str(it[5]),
            "mid_made": str(it[6]),
            "mid_miss": str(it[7]),
            "mid_assisted": str(it[8]),
            "three_made": str(it[9]),
            "three_miss": str(it[10]),
            "three_assisted": str(it[11]),
            "dunks_made": str(it[12]),
            "dunks_miss": str(it[13]),
            "dunks_assisted": str(it[14]),
        })
    write_csv(csv_out, header, rows)


def refresh_advgames_2026(bt_dir: Path, bart_prefix: str) -> None:
    raw_dir = bt_dir / "raw_advgames_json"
    out_dir = bt_dir / "advgames_labeled"
    ensure_dir(raw_dir)
    ensure_dir(out_dir)

    urls = [
        bart_url(bart_prefix, "2026_all_advgames.json.gz"),
        bart_url(bart_prefix, "2026_all_advgames.json"),
    ]

    payload: bytes | None = None
    used_url = ""
    for u in urls:
        try:
            payload = fetch_bytes(u)
            used_url = u
            if payload:
                break
        except Exception:
            continue
    if not payload:
        raise RuntimeError("Could not fetch 2026 all_advgames JSON")

    if used_url.endswith('.gz'):
        try:
            txt = gzip.decompress(payload).decode("utf-8", errors="replace")
        except Exception:
            txt = payload.decode("utf-8", errors="replace")
    else:
        txt = payload.decode("utf-8", errors="replace")

    (raw_dir / "2026_all_advgames.json").write_text(txt, encoding="utf-8")

    arr = json.loads(txt)
    rows: list[dict[str, str]] = []
    for it in arr:
        if not isinstance(it, list):
            continue
        row = {}
        for i, h in enumerate(ADVGAMES_HEADERS):
            row[h] = str(it[i]) if i < len(it) and it[i] is not None else ""
        rows.append(row)

    write_csv(out_dir / "2026_all_advgames_labeled.csv", ADVGAMES_HEADERS, rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Refresh 2026 Bart datasets used by player cards.")
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--bart-prefix", default="ncaaw", help="Bart path prefix, e.g. ncaaw or empty for men.")
    args = ap.parse_args()

    root = Path(args.project_root)
    bt_dir = root / "player_cards_pipeline" / "data" / "bt"
    ensure_dir(bt_dir)

    refresh_advstats_2026(bt_dir, args.bart_prefix)
    refresh_playerstat_2026(bt_dir, args.bart_prefix)
    refresh_advgames_2026(bt_dir, args.bart_prefix)

    print("Bart 2026 refresh complete")


if __name__ == "__main__":
    main()
