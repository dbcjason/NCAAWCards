#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


def norm_text(v: Any) -> str:
    if v is None:
        return ""
    return " ".join(str(v).strip().lower().split())


def norm_player_name(v: Any) -> str:
    s = str(v or "").strip()
    if "," in s:
        last, first = s.split(",", 1)
        s = f"{first.strip()} {last.strip()}".strip()
    return norm_text(s)


def norm_team(v: Any) -> str:
    s = norm_text(v)
    return "".join(ch for ch in s if ch.isalnum())


def to_float(v: Any) -> float | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def bt_num(row: dict[str, str], aliases: list[str]) -> float | None:
    alias_norm = {norm_text(a) for a in aliases}
    for k, v in row.items():
        if norm_text(k) in alias_norm:
            n = to_float(v)
            if n is not None and math.isfinite(n):
                return n
    return None


def bt_possessions_estimate(r: dict[str, str]) -> float | None:
    poss = bt_num(r, ["off_team_poss.value"])
    if poss is not None and poss > 0:
        return float(poss)
    poss = bt_num(r, ["possessions", " possessions"])
    if poss is not None and poss > 0:
        return float(poss)
    tpa = bt_num(r, ["TPA", " TPA", "tpa", " tpa"])
    tpa100 = bt_num(r, ["3p/100?", " 3p/100?"])
    if tpa is not None and tpa100 is not None and tpa100 > 0:
        return (float(tpa) * 100.0) / float(tpa100)
    return None


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_playerstat_rows(path: Path) -> list[dict[str, Any]]:
    arr = json.loads(path.read_text(encoding="utf-8"))
    out: list[dict[str, Any]] = []
    for r in arr:
        if not isinstance(r, list) or len(r) < 15:
            continue
        out.append(
            {
                "player": str(r[1]),
                "team": str(r[2]),
                "rim_made": float(r[3]),
                "rim_assisted": float(r[5]),
                "mid_made": float(r[6]),
                "mid_assisted": float(r[8]),
                "three_made": float(r[9]),
                "three_assisted": float(r[11]),
                "dunks_made": float(r[12]),
                "dunks_assisted": float(r[14]),
            }
        )
    return out


def find_playerstat_row(rows: list[dict[str, Any]], player: str, team: str) -> dict[str, Any] | None:
    np = norm_player_name(player)
    nt = norm_team(team)
    exact = [r for r in rows if norm_player_name(r.get("player", "")) == np and norm_team(r.get("team", "")) == nt]
    if exact:
        return exact[0]
    by_name = [r for r in rows if norm_player_name(r.get("player", "")) == np]
    if len(by_name) == 1:
        return by_name[0]
    return None


def metrics_from_playerstat(ps: dict[str, Any], possessions: float) -> dict[str, float]:
    un_rim = max(0.0, float(ps.get("rim_made", 0.0)) - float(ps.get("rim_assisted", 0.0)))
    un_mid = max(0.0, float(ps.get("mid_made", 0.0)) - float(ps.get("mid_assisted", 0.0)))
    un_3 = max(0.0, float(ps.get("three_made", 0.0)) - float(ps.get("three_assisted", 0.0)))
    un_dunks = max(0.0, float(ps.get("dunks_made", 0.0)) - float(ps.get("dunks_assisted", 0.0)))
    mul = 100.0 / float(possessions)
    return {
        "unassisted_dunks_100": un_dunks * mul,
        "unassisted_rim_makes_100": un_rim * mul,
        "unassisted_mid_makes_100": un_mid * mul,
        "unassisted_3pm_100": un_3 * mul,
        "unassisted_points_100": ((2.0 * un_rim) + (2.0 * un_mid) + (3.0 * un_3)) * mul,
    }


def parse_years(spec: str) -> list[int]:
    spec = spec.strip()
    if "-" in spec:
        a, b = spec.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(x.strip()) for x in spec.split(",") if x.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Build cached self-creation per-100 metrics by year.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--years", default="2010-2025")
    ap.add_argument("--bt-csv", default="player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv")
    ap.add_argument("--out-dir", default="player_cards_pipeline/data/bt/self_creation_by_year")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    bt_rows = load_csv((root / args.bt_csv).resolve())
    out_dir = (root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    years = parse_years(args.years)

    by_year: dict[int, list[dict[str, str]]] = {}
    for y in years:
        by_year[y] = [r for r in bt_rows if norm_text(r.get("year", "")) == str(y)]

    for y in years:
        ps_path = root / "player_cards_pipeline" / "data" / "bt" / "raw_playerstat_json" / f"{y}_pbp_playerstat_array.json"
        if not ps_path.exists():
            print(f"[skip] year={y} missing playerstat json: {ps_path}")
            continue
        ps_rows = load_playerstat_rows(ps_path)
        out_rows: list[dict[str, Any]] = []
        for r in by_year.get(y, []):
            player = str(r.get("player_name", "")).strip()
            team = str(r.get("team", "")).strip()
            if not player or not team:
                continue
            poss = bt_possessions_estimate(r)
            if poss is None or poss <= 0:
                continue
            ps = find_playerstat_row(ps_rows, player, team)
            if not ps:
                continue
            m = metrics_from_playerstat(ps, poss)
            out_rows.append(
                {
                    "player_name": player,
                    "team": team,
                    "year": str(y),
                    **{k: f"{v:.6f}" for k, v in m.items()},
                }
            )
        out_path = out_dir / f"self_creation_cache_{y}.csv"
        headers = [
            "player_name",
            "team",
            "year",
            "unassisted_dunks_100",
            "unassisted_rim_makes_100",
            "unassisted_mid_makes_100",
            "unassisted_3pm_100",
            "unassisted_points_100",
        ]
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            w.writerows(out_rows)
        print(f"[ok] year={y} rows={len(out_rows)} -> {out_path}")


if __name__ == "__main__":
    main()

