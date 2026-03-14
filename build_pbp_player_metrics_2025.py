#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import difflib
import glob
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def norm_name(v: str) -> str:
    return re.sub(r"\s+", " ", (v or "").strip())


def parse_bool(v: str) -> bool:
    return str(v).strip().upper() == "TRUE"


def parse_float(v: str) -> float | None:
    try:
        if v in ("", "NA", None):
            return None
        return float(v)
    except Exception:
        return None


def parse_int(v: str) -> int | None:
    try:
        if v in ("", "NA", None):
            return None
        return int(float(v))
    except Exception:
        return None


def is_na(v: str) -> bool:
    return (v or "").strip() in {"", "NA"}


def near_hoop_distance_ft(x: float, y: float) -> float:
    # ESPN/ncaahoopR half-court style coords appear in feet with x in [-45.75, 45.75], y in [-25, 25].
    # Hoops at (+/-41.75, 0).
    d1 = math.hypot(x - 41.75, y)
    d2 = math.hypot(x + 41.75, y)
    return min(d1, d2)


def classify_shot_zone(row: dict[str, str], desc: str) -> str | None:
    shot_outcome = (row.get("shot_outcome") or "").strip().lower()
    if shot_outcome not in {"made", "missed"}:
        return None

    three = parse_bool(row.get("three_pt") or "") or ("three point" in desc.lower())
    sx = parse_float(row.get("shot_x") or "")
    sy = parse_float(row.get("shot_y") or "")

    if three:
        return "three"

    if sx is not None and sy is not None:
        if near_hoop_distance_ft(sx, sy) <= 4.5:
            return "rim"
        return "mid"

    d = desc.lower()
    if any(k in d for k in ["dunk", "layup", "tip in", "tip shot", "alley oop"]):
        return "rim"
    return "mid"


def parse_sub_out(desc: str) -> tuple[str, str] | None:
    m = re.match(r"^(.*?) subbing out for (.*?)$", desc.strip())
    if not m:
        return None
    return norm_name(m.group(1)), norm_name(m.group(2))


def parse_sub_in(desc: str) -> tuple[str, str] | None:
    m = re.match(r"^(.*?) subbing in for (.*?)$", desc.strip())
    if not m:
        return None
    return norm_name(m.group(1)), norm_name(m.group(2))


def parse_foul_on(desc: str) -> str | None:
    m = re.search(r"Foul on (.*?)\.", desc)
    return norm_name(m.group(1)) if m else None


def parse_turnover_player(desc: str) -> str | None:
    m = re.match(r"^(.*?) Turnover\.", desc)
    return norm_name(m.group(1)) if m else None


def parse_rebound_player(desc: str) -> str | None:
    m = re.match(r"^(.*?) (Offensive|Defensive) Rebound\.", desc)
    return norm_name(m.group(1)) if m else None


def parse_off_foul_drawn_player(desc: str) -> str | None:
    # Best-effort patterns; often unavailable in this feed.
    patterns = [
        r"Offensive foul on .*?\. Drawn by (.*?)\.",
        r"Charge drawn by (.*?)\.",
        r"Player control foul by .*?\. Drawn by (.*?)\.",
    ]
    for p in patterns:
        m = re.search(p, desc, flags=re.IGNORECASE)
        if m:
            return norm_name(m.group(1))
    return None


def parse_assist_player(desc: str) -> str | None:
    m = re.search(r"Assisted by (.*?)\\.", desc or "")
    return norm_name(m.group(1)) if m else None


def parse_shot_desc(desc: str) -> tuple[str, bool, str, bool] | None:
    d = (desc or "").strip()
    m = re.match(r"^(.*?) (made|missed) (.*)$", d)
    if not m:
        return None
    player = norm_name(m.group(1))
    made = m.group(2) == "made"
    tail = m.group(3).lower()
    is_ft = "free throw" in tail
    if is_ft:
        return player, made, "", True
    if "three point" in tail or "3-point" in tail:
        return player, made, "three", False
    if any(k in tail for k in ["dunk", "layup", "tip in", "tip shot", "alley oop"]):
        return player, made, "rim", False
    return player, made, "mid", False


def ensure_on_court(
    on_court: Dict[str, set[str]],
    team: str,
    player: str,
) -> None:
    if not player or not team:
        return
    s = on_court[team]
    if player in s:
        return
    if len(s) < 5:
        s.add(player)


def iter_files(root: Path) -> Iterable[Path]:
    for p in sorted(glob.glob(str(root / "*" / "*.csv"))):
        yield Path(p)


def load_player_team_hints(bt_csv: Path, season_year: int) -> Dict[str, str]:
    out: Dict[str, set[str]] = defaultdict(set)
    if not bt_csv.exists():
        return {}
    with bt_csv.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            y = str(row.get("year") or "").strip()
            if y != str(season_year):
                continue
            p = norm_name(row.get("player_name") or "")
            t = norm_name(row.get("team") or "")
            if p and t:
                out[p].add(t)
    return {k: next(iter(v)) for k, v in out.items() if len(v) == 1}


def load_bt_games_map(bt_csv: Path, season_year: int) -> Dict[tuple[str, str, str], float]:
    out: Dict[tuple[str, str, str], float] = {}
    if not bt_csv.exists():
        return out
    season = str(season_year)
    with bt_csv.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            y = str(row.get("year") or "").strip()
            if y != season:
                continue
            p = norm_name(row.get("player_name") or "")
            t = norm_name(row.get("team") or "")
            gp = parse_float(row.get("GP") or "")
            if not p or not t or gp is None or gp <= 0:
                continue
            out[(season, t, p)] = float(gp)
    return out


def load_bt_games_by_player(bt_csv: Path, season_year: int) -> Dict[tuple[str, str], list[tuple[str, float]]]:
    out: Dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    if not bt_csv.exists():
        return {}
    season = str(season_year)
    with bt_csv.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            y = str(row.get("year") or "").strip()
            if y != season:
                continue
            p = norm_name(row.get("player_name") or "")
            t = norm_name(row.get("team") or "")
            gp = parse_float(row.get("GP") or "")
            if not p or not t or gp is None or gp <= 0:
                continue
            out[(season, p)].append((t, float(gp)))
    return dict(out)


def resolve_bart_games(
    season: str,
    team: str,
    player: str,
    bt_games_exact: Dict[tuple[str, str, str], float],
    bt_games_by_player: Dict[tuple[str, str], list[tuple[str, float]]],
) -> float | None:
    exact = bt_games_exact.get((season, team, player))
    if exact is not None:
        return exact
    opts = bt_games_by_player.get((season, player), [])
    if not opts:
        return None
    if len(opts) == 1:
        return opts[0][1]
    nt = norm_name(team).lower()
    scored = sorted(
        ((difflib.SequenceMatcher(None, nt, norm_name(t).lower()).ratio(), gp) for t, gp in opts),
        key=lambda x: x[0],
        reverse=True,
    )
    if scored and scored[0][0] >= 0.55:
        return scored[0][1]
    return None


def adjust_possessions_to_bart_games(
    pbp_possessions: float,
    pbp_games: float | None,
    bart_games: float | None,
) -> float:
    if pbp_possessions <= 0:
        return 0.0
    if pbp_games is None or bart_games is None or pbp_games <= 0 or bart_games <= 0:
        return float(pbp_possessions)
    return (float(pbp_possessions) / float(pbp_games)) * float(bart_games)


def main() -> None:
    ap = argparse.ArgumentParser(description="Build player possessions + self-creation metrics from ncaahoopR pbp logs.")
    ap.add_argument("--pbp-root", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--season-year", required=True, type=int, help="Season label year (e.g., 2025 for 2024-25).")
    ap.add_argument("--bt-csv", default="", help="Optional BT CSV for player->team hints in legacy logs.")
    args = ap.parse_args()

    pbp_root = Path(args.pbp_root)
    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # (season, team, player) -> counters
    C = defaultdict(lambda: defaultdict(float))

    files = list(iter_files(pbp_root))
    total = len(files)
    team_hints = load_player_team_hints(Path(args.bt_csv), args.season_year) if args.bt_csv else {}
    bt_games_map = load_bt_games_map(Path(args.bt_csv), args.season_year) if args.bt_csv else {}
    bt_games_by_player = load_bt_games_by_player(Path(args.bt_csv), args.season_year) if args.bt_csv else {}

    for i, fp in enumerate(files, start=1):
        with fp.open(newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            rows = list(r)

        if not rows:
            continue

        season = str(args.season_year)

        home = norm_name(rows[0].get("home") or "")
        away = norm_name(rows[0].get("away") or "")
        if not home or not away:
            continue

        on_court: Dict[str, set[str]] = {home: set(), away: set()}
        game_poss_players: set[tuple[str, str]] = set()
        prev_poss_team = ""

        for row in rows:
            desc = row.get("description") or ""
            action_side = (row.get("action_team") or "").strip().lower()
            action_team = home if action_side == "home" else away if action_side == "away" else ""
            shot_team_raw = (row.get("shot_team") or "").strip()
            if not action_team:
                st = shot_team_raw.lower()
                if st == "home":
                    action_team = home
                elif st == "away":
                    action_team = away
                elif st not in {"", "na", "none", "nan"}:
                    action_team = norm_name(shot_team_raw)
            poss_before_raw = norm_name(row.get("possession_before") or "")
            if not action_team and poss_before_raw:
                action_team = poss_before_raw

            # Substitutions
            s_out = parse_sub_out(desc)
            if s_out is not None:
                p, t = s_out
                if t in on_court:
                    if p not in on_court[t] and len(on_court[t]) < 5:
                        on_court[t].add(p)
                    on_court[t].discard(p)

            s_in = parse_sub_in(desc)
            if s_in is not None:
                p, t = s_in
                if t in on_court and p:
                    if len(on_court[t]) < 5 or p in on_court[t]:
                        on_court[t].add(p)

            # Heuristic lineup enrichment from active participants.
            shooter = norm_name(row.get("shooter") or "")
            assist = norm_name(row.get("assist") or "") if not is_na(row.get("assist") or "") else ""
            shot_desc = parse_shot_desc(desc)
            if not shooter and shot_desc:
                shooter = shot_desc[0]
            if not assist:
                assist = parse_assist_player(desc) or ""
            if not action_team and shooter:
                action_team = team_hints.get(shooter, "")
            foul_on = parse_foul_on(desc)
            tov_p = parse_turnover_player(desc)
            reb_p = parse_rebound_player(desc)
            for p in [shooter, assist, foul_on, tov_p, reb_p]:
                if p and action_team in on_court:
                    ensure_on_court(on_court, action_team, p)

            # Possession credit by possession_before transitions.
            poss_before = norm_name(row.get("possession_before") or "")
            if poss_before in {home, away} and poss_before != prev_poss_team:
                off = poss_before
                deff = away if off == home else home
                for p in on_court.get(off, set()):
                    C[(season, off, p)]["off_possessions"] += 1
                    game_poss_players.add((off, p))
                for p in on_court.get(deff, set()):
                    C[(season, deff, p)]["def_possessions"] += 1
                    game_poss_players.add((deff, p))
                prev_poss_team = poss_before

            # Player event counters (offensive/team-of-action context)
            if action_team not in {home, away}:
                continue

            key_team = action_team
            shot_outcome = (row.get("shot_outcome") or "").strip().lower()
            zone = classify_shot_zone(row, desc)
            is_made = shot_outcome == "made"
            is_ft_from_desc = False
            if not shot_outcome and shot_desc:
                is_made = shot_desc[1]
                if shot_desc[3]:
                    is_ft_from_desc = True
                if not zone and shot_desc[2]:
                    zone = shot_desc[2]
            is_three = parse_bool(row.get("three_pt") or "") or zone == "three"

            if shooter:
                k = (season, key_team, shooter)
                if zone == "rim":
                    C[k]["rim_att"] += 1
                if parse_bool(row.get("free_throw") or "") or is_ft_from_desc:
                    C[k]["fta"] += 1
                if is_made and zone == "rim" and not assist:
                    C[k]["unassisted_rim_makes"] += 1
                if is_made and zone == "mid" and not assist:
                    C[k]["unassisted_mid_makes"] += 1
                if is_made and is_three and not assist:
                    C[k]["unassisted_3pm"] += 1
                if is_made and "dunk" in desc.lower():
                    C[k]["dunks_made"] += 1

            if assist and is_made and zone == "rim":
                k = (season, key_team, assist)
                C[k]["rim_assists"] += 1

            drawn = parse_off_foul_drawn_player(desc)
            if drawn:
                # Drawn-by player is on defense when offensive foul happens.
                draw_team = away if key_team == home else home
                k = (season, draw_team, drawn)
                C[k]["off_fouls_drawn"] += 1

        # Count one game for players who were on-court for at least one tracked possession.
        for team_name, player_name in game_poss_players:
            C[(season, team_name, player_name)]["pbp_games"] += 1

        if i == 1 or i % 500 == 0 or i == total:
            print(f"[{i}/{total}] files processed", flush=True)

    fields = [
        "season",
        "team",
        "player",
        "off_possessions",
        "def_possessions",
        "pbp_games",
        "bart_games",
        "unassisted_rim_makes",
        "unassisted_mid_makes",
        "unassisted_3pm",
        "rim_assists",
        "off_fouls_drawn",
        "fta",
        "rim_att",
        "dunks_made",
        "unassisted_rim_makes_100",
        "unassisted_mid_makes_100",
        "unassisted_3pm_100",
        "rim_assists_100",
        "off_fouls_drawn_100",
        "fta_100",
        "rim_att_100",
        "dunks_100",
    ]

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for (season, team, player), m in sorted(C.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
            off_pos_raw = float(m.get("off_possessions", 0.0))
            def_pos_raw = float(m.get("def_possessions", 0.0))
            pbp_games = float(m.get("pbp_games", 0.0))
            bart_games = resolve_bart_games(season, team, player, bt_games_map, bt_games_by_player)
            off_pos = adjust_possessions_to_bart_games(off_pos_raw, pbp_games, bart_games)
            def_pos = adjust_possessions_to_bart_games(def_pos_raw, pbp_games, bart_games)
            def per100(v: float) -> float:
                return (100.0 * v / off_pos) if off_pos > 0 else 0.0

            row = {
                "season": season,
                "team": team,
                "player": player,
                "off_possessions": round(off_pos, 1),
                "def_possessions": round(def_pos, 1),
                "pbp_games": round(pbp_games, 1),
                "bart_games": round(bart_games, 1) if bart_games is not None else "",
                "unassisted_rim_makes": round(m.get("unassisted_rim_makes", 0.0), 1),
                "unassisted_mid_makes": round(m.get("unassisted_mid_makes", 0.0), 1),
                "unassisted_3pm": round(m.get("unassisted_3pm", 0.0), 1),
                "rim_assists": round(m.get("rim_assists", 0.0), 1),
                "off_fouls_drawn": round(m.get("off_fouls_drawn", 0.0), 1),
                "fta": round(m.get("fta", 0.0), 1),
                "rim_att": round(m.get("rim_att", 0.0), 1),
                "dunks_made": round(m.get("dunks_made", 0.0), 1),
                "unassisted_rim_makes_100": round(per100(m.get("unassisted_rim_makes", 0.0)), 3),
                "unassisted_mid_makes_100": round(per100(m.get("unassisted_mid_makes", 0.0)), 3),
                "unassisted_3pm_100": round(per100(m.get("unassisted_3pm", 0.0)), 3),
                "rim_assists_100": round(per100(m.get("rim_assists", 0.0)), 3),
                "off_fouls_drawn_100": round(per100(m.get("off_fouls_drawn", 0.0)), 3),
                "fta_100": round(per100(m.get("fta", 0.0)), 3),
                "rim_att_100": round(per100(m.get("rim_att", 0.0)), 3),
                "dunks_100": round(per100(m.get("dunks_made", 0.0)), 3),
            }
            w.writerow(row)

    print(f"wrote {out_csv}")


if __name__ == "__main__":
    main()
