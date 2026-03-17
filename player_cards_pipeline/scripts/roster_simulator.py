#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import importlib.util
import math
import statistics
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

COUNTING_KEYS = ["ppg", "rpg", "apg", "spg", "bpg", "mpg"]

FEATURE_KEYS = [
    "mpg", "ppg", "rpg", "apg", "spg", "bpg", "fg_pct", "tp_pct", "ft_pct",
    "bpm", "usg", "ts_per", "rim_pct", "ast_per", "ast_tov",
    "to_per", "stl_per", "blk_per", "orb_per", "drb_per", "rapm", "net_pts", "onoff_net_rating",
    "ortg", "drtg", "efg",
]
OUT_KEYS = FEATURE_KEYS[:]

TEAM_DISPLAY_METRICS = [
    ("adj_pace_proxy", "Adj Pace (Proxy)"),
    ("off_rating", "Off Rtg"),
    ("def_rating", "Def Rtg"),
    ("net_rating", "Net Rtg"),
    ("orb_per", "Off Reb%"),
    ("assists_per100", "Ast/100"),
    ("turnovers_per100", "TOV/100"),
    ("steals_per100", "Stl/100"),
    ("rebounds_per100", "Reb/100"),
    ("fg_pct", "FG%"),
    ("tp_pct", "3P%"),
    ("ts_per", "TS%"),
]


@dataclass
class InputPlayer:
    player: str
    team: str
    season: int
    minutes: float
    destination_conference: str


@dataclass
class ResolvedPlayer:
    inp: InputPlayer
    bt_row: dict[str, str]
    projected: dict[str, float]
    source_conf: str
    transfer_applied: bool


def _norm_key_name(name: str) -> str:
    return _strip_suffix_tokens(name or "")


def load_module(repo_root: Path):
    mod_path = repo_root / "cbb_player_cards_v1" / "build_player_card.py"
    spec = importlib.util.spec_from_file_location("card_builder_roster_sim", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import module from {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def read_bt_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _num(s: str | None, default: float = 0.0) -> float:
    if s is None:
        return default
    t = str(s).strip().replace("%", "")
    if not t:
        return default
    try:
        return float(t)
    except Exception:
        return default


def read_roster_csv(path: Path, default_season: int, default_minutes: float, default_dest_conf: str) -> list[InputPlayer]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    out: list[InputPlayer] = []
    for r in rows:
        player = (r.get("player") or r.get("name") or "").strip()
        if not player:
            continue
        team = (r.get("team") or "").strip()
        season = int(_num(r.get("season"), float(default_season)))
        minutes = _num(r.get("minutes") or r.get("mpg"), default_minutes)
        dest = (r.get("destination_conference") or r.get("dest_conf") or default_dest_conf or "").strip()
        out.append(InputPlayer(player=player, team=team, season=season, minutes=max(0.0, minutes), destination_conference=dest))
    return out


def _strip_suffix_tokens(name: str) -> str:
    toks = [
        t
        for t in name.replace(".", " ").replace(",", " ").split()
        if t.lower() not in {"jr", "sr", "ii", "iii", "iv", "v"}
    ]
    return " ".join(toks).strip().lower()


def _norm_team_name(mod: Any, s: str) -> str:
    return mod.norm_team(s or "")


def find_bt_row(mod: Any, bt_rows: list[dict[str, str]], player: InputPlayer) -> dict[str, str] | None:
    season_rows = [r for r in bt_rows if mod.norm_season(mod.bt_get(r, ["year"])) == str(player.season)]
    if not season_rows:
        return None

    target_name = _strip_suffix_tokens(mod.norm_player_name(player.player))
    target_team = _norm_team_name(mod, player.team)

    cands = []
    for r in season_rows:
        name = _strip_suffix_tokens(mod.norm_player_name(mod.bt_get(r, ["player_name"])))
        if not name:
            continue
        exact_name = int(name == target_name)
        contains_name = int(target_name in name or name in target_name)
        team = _norm_team_name(mod, mod.bt_get(r, ["team"]))
        team_match = int(bool(target_team and team and (team == target_team)))
        gp = mod.bt_num(r, ["GP", "gp"]) or 0.0
        if exact_name or contains_name:
            cands.append((exact_name, team_match, contains_name, gp, r))

    if not cands:
        return None

    cands.sort(key=lambda t: (t[0], t[1], t[2], t[3]), reverse=True)
    return cands[0][4]


def extract_sim_metrics(mod: Any, row: dict[str, str]) -> dict[str, float]:
    out = dict(mod._row_transfer_metrics(row))

    ortg = mod.bt_num(row, ["ORtg"])
    drtg = mod.bt_num(row, ["drtg", "DRtg", "DRTG"])
    efg = mod.bt_num(row, ["eFG"])
    ts = mod.bt_num(row, ["TS_per"])
    tp = mod.bt_num(row, ["TP_per"])
    fg = mod.bt_num(row, ["FG_per", "FG%"])
    to_per = mod.bt_num(row, ["TO_per"])

    if ortg is not None and math.isfinite(float(ortg)):
        out["ortg"] = float(ortg)
    if drtg is not None and math.isfinite(float(drtg)):
        out["drtg"] = float(drtg)
    def pct_scale(v: float | None) -> float | None:
        if v is None or not math.isfinite(float(v)):
            return None
        x = float(v)
        return x * 100.0 if 0.0 <= x <= 1.0 else x

    efg_p = pct_scale(efg)
    ts_p = pct_scale(ts)
    tp_p = pct_scale(tp)
    fg_p = pct_scale(fg)

    if efg_p is not None:
        out["efg"] = efg_p
    if ts_p is not None:
        out["ts_per"] = ts_p
    if tp_p is not None:
        out["tp_pct"] = tp_p
    if fg_p is not None:
        out["fg_pct"] = fg_p
    if to_per is not None and math.isfinite(float(to_per)):
        out["to_per"] = float(to_per)

    # Possessions/game proxy from ORtg and points.
    ppg = out.get("ppg")
    if ppg is not None and ortg is not None and float(ortg) > 1e-6:
        out["poss_pg_proxy"] = max(0.0, float(ppg) * 100.0 / float(ortg))

    return out


def _clip_metric(key: str, v: float) -> float:
    if key in {"mpg"}:
        return max(0.0, min(40.0, v))
    if key in {"ppg"}:
        return max(0.0, min(45.0, v))
    if key in {"rpg", "apg"}:
        return max(0.0, min(20.0, v))
    if key in {"spg", "bpg"}:
        return max(0.0, min(7.0, v))
    if key in {"fg_pct", "tp_pct", "ft_pct", "ts_per", "rim_pct", "usg", "ast_per", "stl_per", "blk_per", "orb_per", "drb_per", "efg"}:
        return max(0.0, min(100.0, v))
    if key in {"to_per"}:
        return max(0.0, min(60.0, v))
    if key in {"ast_tov"}:
        return max(0.0, min(8.0, v))
    if key in {"bpm", "rapm", "net_pts", "onoff_net_rating"}:
        return max(-20.0, min(25.0, v))
    if key in {"ortg", "drtg"}:
        return max(40.0, min(160.0, v))
    if key in {"poss_pg_proxy"}:
        # Player-level possessions/game proxy should stay on a realistic per-player scale.
        return max(3.0, min(20.0, v))
    return v


def build_transfer_examples(mod: Any, bt_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_player_year: dict[str, dict[int, dict[str, str]]] = {}
    for r in bt_rows:
        p = mod.norm_player_name(mod.bt_get(r, ["player_name"]))
        ys = mod.norm_season(mod.bt_get(r, ["year"]))
        if not p or not ys.isdigit():
            continue
        y = int(ys)
        pm = by_player_year.setdefault(p, {})
        prev = pm.get(y)
        if prev is None:
            pm[y] = r
        else:
            prev_gp = mod.bt_num(prev, ["GP", "gp"]) or 0.0
            cur_gp = mod.bt_num(r, ["GP", "gp"]) or 0.0
            if cur_gp > prev_gp:
                pm[y] = r

    out = []
    for seasons_map in by_player_year.values():
        years = sorted(seasons_map)
        for y in years:
            ny = y + 1
            if ny not in seasons_map:
                continue
            src = seasons_map[y]
            dst = seasons_map[ny]
            src_conf = mod._conference_key(mod.bt_get(src, ["conf", "conference"]))
            dst_conf = mod._conference_key(mod.bt_get(dst, ["conf", "conference"]))
            if mod._conference_tier(dst_conf) != "high":
                continue
            if mod._conference_tier(src_conf) == "high":
                continue
            src_m = extract_sim_metrics(mod, src)
            dst_m = extract_sim_metrics(mod, dst)
            if len(src_m) < 8 or len(dst_m) < 8:
                continue
            out.append({"src": src_m, "dst": dst_m, "src_conf": src_conf, "dst_conf": dst_conf})
    return out


def estimate_pace_scale(mod: Any, bt_rows: list[dict[str, str]], season: int, target_team_pace: float = 68.0) -> float:
    # poss_pg_proxy is player-level and too small in raw form for team pace.
    # Calibrate to a realistic team-possessions baseline for the selected season.
    vals: list[float] = []
    sy = str(season)
    for r in bt_rows:
        if mod.norm_season(mod.bt_get(r, ["year"])) != sy:
            continue
        gp = mod.bt_num(r, ["GP", "gp"]) or 0.0
        mp = mod.bt_num(r, ["mp", "MP"]) or 0.0
        if gp < 5 or mp < 8:
            continue
        m = extract_sim_metrics(mod, r)
        p = m.get("poss_pg_proxy")
        if p is None or not math.isfinite(float(p)):
            continue
        vals.append(float(p))
    if not vals:
        return 1.0
    med = statistics.median(vals)
    if med <= 1e-6:
        return 1.0
    return float(target_team_pace) / float(med)


def project_transfer_metrics(mod: Any, source_row: dict[str, str], dest_conf_raw: str, history_examples: list[dict[str, Any]]) -> tuple[dict[str, float], bool]:
    source = extract_sim_metrics(mod, source_row)
    dest_conf = mod._conference_key(dest_conf_raw)
    src_conf = mod._conference_key(mod.bt_get(source_row, ["conf", "conference"]))

    if not dest_conf or dest_conf == src_conf:
        return source, False

    same_dest = [e for e in history_examples if e["dst_conf"] == dest_conf]
    pool = same_dest if len(same_dest) >= 35 else history_examples
    if not pool:
        return source, False

    scales: dict[str, float] = {}
    for k in FEATURE_KEYS:
        vals = sorted(float(e["src"][k]) for e in pool if k in e["src"] and math.isfinite(float(e["src"][k])))
        if len(vals) >= 12:
            lo = vals[max(0, int(0.1 * (len(vals) - 1)))]
            hi = vals[min(len(vals) - 1, int(0.9 * (len(vals) - 1)))]
            spread = hi - lo
            scales[k] = spread if spread > 1e-6 else 1.0
        else:
            scales[k] = 1.0

    weighted: list[tuple[float, dict[str, Any]]] = []
    for e in pool:
        diffs = []
        for k in FEATURE_KEYS:
            tv = source.get(k)
            ev = e["src"].get(k)
            if tv is None or ev is None or not math.isfinite(float(tv)) or not math.isfinite(float(ev)):
                continue
            diffs.append(abs(float(tv) - float(ev)) / max(1e-6, scales[k]))
        if len(diffs) < 8:
            continue
        d = sum(diffs) / len(diffs)
        if dest_conf and e["dst_conf"] == dest_conf:
            d *= 0.86
        elif dest_conf:
            d *= 1.10
        if src_conf and e["src_conf"] == src_conf:
            d *= 0.92
        w = math.exp(-1.35 * d)
        if w > 1e-9:
            weighted.append((w, e))

    weighted.sort(key=lambda t: t[0], reverse=True)
    weighted = weighted[:450]
    if not weighted:
        return source, False

    predicted: dict[str, float] = {}
    for k in OUT_KEYS:
        num = den = 0.0
        for w, e in weighted:
            sv = e["src"].get(k)
            dv = e["dst"].get(k)
            if sv is None or dv is None or not math.isfinite(float(sv)) or not math.isfinite(float(dv)):
                continue
            if k in source and math.isfinite(float(source[k])):
                val = float(source[k]) + (float(dv) - float(sv))
            else:
                val = float(dv)
            num += w * val
            den += w
        if den > 0:
            predicted[k] = _clip_metric(k, num / den)

    for k, v in source.items():
        if k not in predicted and math.isfinite(float(v)):
            predicted[k] = float(v)

    return predicted, True


def _safe_z(values: list[float], x: float) -> float:
    if not values:
        return 0.0
    m = statistics.mean(values)
    sd = statistics.pstdev(values)
    if sd <= 1e-9:
        return 0.0
    return (x - m) / sd


def _interaction_adjust_players(players: list[ResolvedPlayer]) -> list[dict[str, float]]:
    # Adjust per-player projections for roster context (usage/creation/spacing interaction).
    base = [dict(p.projected) for p in players]
    mins = [max(0.0, p.inp.minutes) for p in players]
    total_min = sum(mins)
    if total_min <= 0:
        return base

    usg_vals = [float(m.get("usg", 20.0)) for m in base]
    ast_vals = [float(m.get("ast_per", 15.0)) for m in base]
    ts_vals = [float(m.get("ts_per", 54.0)) for m in base]
    tp_vals = [float(m.get("tp_pct", 33.0)) for m in base]
    fg_vals = [float(m.get("fg_pct", 45.0)) for m in base]
    rim_vals = [float(m.get("rim_pct", 55.0)) for m in base]

    team_space = sum(tp_vals[i] * mins[i] for i in range(len(base))) / total_min
    team_creation = sum(ast_vals[i] * mins[i] for i in range(len(base))) / total_min
    team_ts = sum(ts_vals[i] * mins[i] for i in range(len(base))) / total_min

    # Reallocate usage share based on player creation/scoring profile.
    load_scores = []
    for i in range(len(base)):
        s = (
            0.55 * _safe_z(usg_vals, usg_vals[i])
            + 0.25 * _safe_z(ts_vals, ts_vals[i])
            + 0.20 * _safe_z(ast_vals, ast_vals[i])
        )
        load_scores.append(math.exp(max(-5.0, min(5.0, s))))
    denom = sum(load_scores)
    if denom <= 0:
        return base

    total_usage_load = sum(usg_vals[i] * mins[i] for i in range(len(base)))
    adjusted: list[dict[str, float]] = []
    for i, m in enumerate(base):
        mm = max(0.1, mins[i])
        share = load_scores[i] / denom
        new_usg = _clip_metric("usg", share * total_usage_load / mm)
        base_usg = max(1e-6, float(m.get("usg", new_usg)))
        usage_factor = max(0.45, min(1.65, new_usg / base_usg))

        ast = float(m.get("ast_per", 15.0))
        ts = float(m.get("ts_per", 54.0))
        tp = float(m.get("tp_pct", 33.0))
        fg = float(m.get("fg_pct", 45.0))
        rim = float(m.get("rim_pct", 55.0))
        ppg = float(m.get("ppg", 0.0))
        apg = float(m.get("apg", 0.0))
        to_per = float(m.get("to_per", 16.0))
        ortg = float(m.get("ortg", 100.0))
        drtg = float(m.get("drtg", 100.0))
        bpm = float(m.get("bpm", 0.0))
        rapm = float(m.get("rapm", 0.0))
        net_pts = float(m.get("net_pts", 0.0))
        onoff = float(m.get("onoff_net_rating", 0.0))

        # Context effects: better surrounding creation/spacing helps efficiency,
        # high usage growth has a mild efficiency tax.
        ts_new = _clip_metric(
            "ts_per",
            ts
            + 0.030 * (team_space - tp)
            + 0.020 * (team_creation - ast)
            - 0.120 * (new_usg - base_usg),
        )
        fg_new = _clip_metric(
            "fg_pct",
            fg + 0.025 * (team_space - tp) + 0.012 * (team_creation - ast) - 0.080 * (new_usg - base_usg),
        )
        tp_new = _clip_metric("tp_pct", tp + 0.018 * (team_creation - ast) - 0.050 * (new_usg - base_usg))
        rim_new = _clip_metric("rim_pct", rim + 0.010 * (team_creation - ast))

        ppg_new = _clip_metric(
            "ppg",
            ppg
            * (usage_factor ** 0.85)
            * (1.0 + 0.002 * (team_space - tp) + 0.0015 * (team_creation - ast)),
        )
        apg_new = _clip_metric("apg", apg * (usage_factor ** 0.45) * (1.0 + 0.0018 * (team_ts - ts)))
        to_per_new = _clip_metric("to_per", to_per + 0.22 * (new_usg - base_usg) - 0.03 * (team_creation - ast))

        ortg_new = _clip_metric(
            "ortg",
            ortg + 0.40 * (ppg_new - ppg) + 0.25 * (ts_new - ts) - 0.18 * (new_usg - base_usg),
        )
        drtg_new = _clip_metric("drtg", drtg + 0.08 * (new_usg - base_usg))
        bpm_new = _clip_metric("bpm", bpm + 0.08 * (ortg_new - ortg) - 0.06 * (drtg_new - drtg))
        rapm_new = _clip_metric("rapm", rapm + 0.05 * (ortg_new - ortg) - 0.04 * (drtg_new - drtg))
        net_pts_new = _clip_metric("net_pts", net_pts + 0.07 * (ortg_new - ortg) - 0.05 * (drtg_new - drtg))
        onoff_new = _clip_metric(
            "onoff_net_rating", onoff + 0.06 * (ortg_new - ortg) - 0.04 * (drtg_new - drtg)
        )

        new_m = dict(m)
        new_m["usg"] = new_usg
        new_m["ts_per"] = ts_new
        new_m["fg_pct"] = fg_new
        new_m["tp_pct"] = tp_new
        new_m["rim_pct"] = rim_new
        new_m["ppg"] = ppg_new
        new_m["apg"] = apg_new
        new_m["to_per"] = to_per_new
        new_m["ortg"] = ortg_new
        new_m["drtg"] = drtg_new
        new_m["bpm"] = bpm_new
        new_m["rapm"] = rapm_new
        new_m["net_pts"] = net_pts_new
        new_m["onoff_net_rating"] = onoff_new
        # Keep possessions proxy coherent with ppg + ORtg.
        if ortg_new > 1e-6:
            new_m["poss_pg_proxy"] = _clip_metric("poss_pg_proxy", ppg_new * 100.0 / ortg_new)

        adjusted.append(new_m)

    return adjusted


def projected_player_metrics(players: list[ResolvedPlayer], interaction_model: bool = False) -> list[dict[str, float]]:
    if interaction_model:
        return _interaction_adjust_players(players)
    return [dict(p.projected) for p in players]


def aggregate_team(
    players: list[ResolvedPlayer],
    interaction_model: bool = False,
    pace_scale: float = 1.0,
) -> tuple[dict[str, float], float]:
    tot_min = sum(p.inp.minutes for p in players)
    if tot_min <= 0:
        return {}, 0.0

    per_player = projected_player_metrics(players, interaction_model=interaction_model)

    out: dict[str, float] = {}

    def wavg(key: str) -> float | None:
        num = den = 0.0
        for idx, p in enumerate(players):
            v = per_player[idx].get(key)
            if v is None or not math.isfinite(float(v)):
                continue
            num += float(v) * p.inp.minutes
            den += p.inp.minutes
        if den <= 0:
            return None
        return num / den

    for k in COUNTING_KEYS + ["bpm", "rapm", "net_pts", "onoff_net_rating", "ts_per", "fg_pct", "tp_pct", "ortg", "drtg", "poss_pg_proxy", "orb_per", "to_per"]:
        v = wavg(k)
        if v is not None:
            out[k] = v

    pace = out.get("poss_pg_proxy")
    if pace is not None and pace > 1e-6:
        pace_adj = max(55.0, min(80.0, float(pace) * float(pace_scale)))
        out["adj_pace_proxy"] = pace_adj
        for src, dst in [("ppg", "points_per100"), ("apg", "assists_per100"), ("spg", "steals_per100"), ("rpg", "rebounds_per100")]:
            v = out.get(src)
            if v is not None:
                out[dst] = float(v) * 100.0 / float(pace_adj)
        tov = out.get("to_per")
        if tov is not None:
            out["turnovers_per100"] = float(tov)

    if "ortg" in out:
        out["off_rating"] = out["ortg"]
    if "drtg" in out:
        out["def_rating"] = out["drtg"]
    if "off_rating" in out and "def_rating" in out:
        out["net_rating"] = out["off_rating"] - out["def_rating"]

    return out, tot_min


def build_current_team_players(mod: Any, bt_rows: list[dict[str, str]], season: int, base_team: str) -> list[ResolvedPlayer]:
    out: list[ResolvedPlayer] = []
    target_team = mod.norm_team(base_team)
    for r in bt_rows:
        y = mod.norm_season(mod.bt_get(r, ["year"]))
        if y != str(season):
            continue
        team = mod.norm_team(mod.bt_get(r, ["team"]))
        if team != target_team:
            continue
        name = (mod.bt_get(r, ["player_name"]) or "").strip()
        if not name:
            continue
        mpg = mod.bt_num(r, ["mp", "MP"]) or 0.0
        inp = InputPlayer(player=name, team=mod.bt_get(r, ["team"]) or base_team, season=season, minutes=max(0.0, float(mpg)), destination_conference="")
        src_conf = mod._conference_key(mod.bt_get(r, ["conf", "conference"]))
        out.append(ResolvedPlayer(inp=inp, bt_row=r, projected=extract_sim_metrics(mod, r), source_conf=src_conf, transfer_applied=False))
    return out


def infer_base_team(mod: Any, players: list[ResolvedPlayer]) -> str:
    teams = [mod.norm_team(p.inp.team) for p in players if p.inp.team]
    if not teams:
        return ""
    most = Counter(teams).most_common(1)[0][0]
    for p in players:
        if mod.norm_team(p.inp.team) == most:
            return p.inp.team
    return ""


def compute_delta(current: dict[str, float], edited: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, _ in TEAM_DISPLAY_METRICS:
        if key in current and key in edited:
            out[key] = edited[key] - current[key]
    return out


def build_season_team_summaries(mod: Any, bt_rows: list[dict[str, str]], season: int) -> dict[str, dict[str, float]]:
    pace_scale = estimate_pace_scale(mod, bt_rows, season)
    teams: dict[str, list[ResolvedPlayer]] = {}
    sy = str(season)
    for r in bt_rows:
        y = mod.norm_season(mod.bt_get(r, ["year"]))
        if y != sy:
            continue
        team = (mod.bt_get(r, ["team"]) or "").strip()
        if not team:
            continue
        name = (mod.bt_get(r, ["player_name"]) or "").strip()
        if not name:
            continue
        mpg = mod.bt_num(r, ["mp", "MP"]) or 0.0
        inp = InputPlayer(player=name, team=team, season=season, minutes=max(0.0, float(mpg)), destination_conference="")
        rp = ResolvedPlayer(
            inp=inp,
            bt_row=r,
            projected=extract_sim_metrics(mod, r),
            source_conf=mod._conference_key(mod.bt_get(r, ["conf", "conference"])),
            transfer_applied=False,
        )
        teams.setdefault(team, []).append(rp)

    out: dict[str, dict[str, float]] = {}
    for team, plist in teams.items():
        summary, _ = aggregate_team(plist, interaction_model=False, pace_scale=pace_scale)
        out[team] = summary
    return out


def metric_rank(value: float | None, pool: list[float], lower_is_better: bool = False) -> str:
    if value is None or not pool:
        return "-"
    if lower_is_better:
        better = sum(1 for x in pool if x < float(value))
    else:
        better = sum(1 for x in pool if x > float(value))
    return f"{better + 1}/{len(pool)}"


def build_in_out_rows(
    mod: Any,
    base_team: str,
    edited_players: list[ResolvedPlayer],
    edited_metrics: list[dict[str, float]],
    current_players: list[ResolvedPlayer],
    current_metrics: list[dict[str, float]],
) -> tuple[list[str], list[str]]:
    base_norm = mod.norm_team(base_team or "")

    selected_base_keys = {
        (_norm_key_name(p.inp.player), mod.norm_team(p.inp.team))
        for p in edited_players
        if mod.norm_team(p.inp.team) == base_norm
    }

    in_rows: list[str] = []
    for p, m in zip(edited_players, edited_metrics):
        is_in = p.transfer_applied or mod.norm_team(p.inp.team) != base_norm
        if not is_in:
            continue
        in_rows.append(
            "<tr>"
            f"<td>{html.escape(p.inp.player)}</td>"
            f"<td>{html.escape(p.inp.team)}</td>"
            f"<td>{p.inp.season}</td>"
            f"<td>{m.get('mpg', 0.0):.1f}</td>"
            f"<td>{m.get('ppg', 0.0):.1f}</td>"
            f"<td>{m.get('rpg', 0.0):.1f}</td>"
            f"<td>{m.get('apg', 0.0):.1f}</td>"
            f"<td>{m.get('spg', 0.0):.1f}</td>"
            f"<td>{m.get('bpg', 0.0):.1f}</td>"
            f"<td>{m.get('fg_pct', 0.0):.1f}</td>"
            f"<td>{m.get('tp_pct', 0.0):.1f}</td>"
            f"<td>{m.get('ft_pct', 0.0):.1f}</td>"
            "</tr>"
        )

    out_rows: list[str] = []
    for p, m in zip(current_players, current_metrics):
        key = (_norm_key_name(p.inp.player), mod.norm_team(p.inp.team))
        if key in selected_base_keys:
            continue
        out_rows.append(
            "<tr>"
            f"<td>{html.escape(p.inp.player)}</td>"
            f"<td>{html.escape(p.inp.team)}</td>"
            f"<td>{p.inp.season}</td>"
            f"<td>{m.get('mpg', 0.0):.1f}</td>"
            f"<td>{m.get('ppg', 0.0):.1f}</td>"
            f"<td>{m.get('rpg', 0.0):.1f}</td>"
            f"<td>{m.get('apg', 0.0):.1f}</td>"
            f"<td>{m.get('spg', 0.0):.1f}</td>"
            f"<td>{m.get('bpg', 0.0):.1f}</td>"
            f"<td>{m.get('fg_pct', 0.0):.1f}</td>"
            f"<td>{m.get('tp_pct', 0.0):.1f}</td>"
            f"<td>{m.get('ft_pct', 0.0):.1f}</td>"
            "</tr>"
        )
    return in_rows, out_rows


def render_html(
    out_path: Path,
    season: int,
    players: list[ResolvedPlayer],
    edited_summary: dict[str, float],
    total_minutes: float,
    current_summary: dict[str, float],
    base_team: str,
    league_team_summaries: dict[str, dict[str, float]],
    in_rows: list[str],
    out_rows: list[str],
    interaction_model: bool = False,
):
    def f1(v: float | None) -> str:
        return "-" if v is None else f"{v:.1f}"

    def f2(v: float | None) -> str:
        return "-" if v is None else f"{v:.2f}"

    delta = compute_delta(current_summary, edited_summary)

    metric_rows = []
    for k, label in TEAM_DISPLAY_METRICS:
        cur = current_summary.get(k)
        new = edited_summary.get(k)
        d = delta.get(k)
        pool = [s[k] for s in league_team_summaries.values() if k in s and math.isfinite(float(s[k]))]
        low_is_better = k in {"def_rating"}
        cur_rank = metric_rank(cur, pool, lower_is_better=low_is_better)
        new_rank = metric_rank(new, pool, lower_is_better=low_is_better)
        metric_rows.append(
            f"<tr><td>{html.escape(label)}</td><td>{f2(cur)}</td><td>{cur_rank}</td><td>{f2(new)}</td><td>{new_rank}</td><td>{f2(d)}</td></tr>"
        )

    player_rows = []
    for p in players:
        m = p.projected
        player_rows.append(
            "<tr>"
            f"<td>{html.escape(p.inp.player)}</td>"
            f"<td>{html.escape(p.inp.team)}</td>"
            f"<td>{p.inp.season}</td>"
            f"<td>{f1(p.inp.minutes)}</td>"
            f"<td>{html.escape(p.source_conf)}</td>"
            f"<td>{'Yes' if p.transfer_applied else 'No'}</td>"
            f"<td>{f2(m.get('off_rating') or m.get('ortg'))}</td>"
            f"<td>{f2(m.get('def_rating') or m.get('drtg'))}</td>"
            f"<td>{f2(m.get('bpm'))}</td>"
            f"<td>{f2(m.get('rapm'))}</td>"
            f"<td>{f1(m.get('ppg'))}</td>"
            "</tr>"
        )

    html_doc = f"""
<!doctype html>
<html>
<head>
<meta charset='utf-8' />
<title>Roster Simulator - {season}</title>
<style>
body {{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin:20px; background:#0f1218; color:#eaf0ff; }}
h1,h2 {{ margin:0 0 10px; }}
.panel {{ border:1px solid #2a3244; background:#161c2a; border-radius:10px; padding:12px; margin-bottom:14px; }}
table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
th,td {{ border:1px solid #2d374b; padding:6px 8px; font-size:12px; text-align:center; }}
th {{ background:#1b2434; }}
.meta {{ color:#a6b4cf; font-size:12px; margin-bottom:8px; }}
</style>
</head>
<body>
  <h1>Roster Simulator</h1>
  <div class='meta'>Base Team: {html.escape(base_team or 'N/A')} | Season: {season} | Total selected minutes: {f1(total_minutes)}</div>
  <div class='meta'>Edited roster uses player-level BT stats and optional transfer-up translation. Team results show edited roster vs current team for the same season.</div>
  <div class='meta'>Interaction Model: {'ON' if interaction_model else 'OFF'} (usage/creation/spacing redistribution)</div>

  <div class='panel'>
    <h2>In</h2>
    <table>
      <thead>
        <tr><th>Player</th><th>From Team</th><th>Season</th><th>MPG</th><th>PPG</th><th>RPG</th><th>APG</th><th>SPG</th><th>BPG</th><th>FG%</th><th>3P%</th><th>FT%</th></tr>
      </thead>
      <tbody>
        {''.join(in_rows) if in_rows else '<tr><td colspan=\"12\">No added players selected.</td></tr>'}
      </tbody>
    </table>
  </div>

  <div class='panel'>
    <h2>Out</h2>
    <table>
      <thead>
        <tr><th>Player</th><th>Team</th><th>Season</th><th>MPG</th><th>PPG</th><th>RPG</th><th>APG</th><th>SPG</th><th>BPG</th><th>FG%</th><th>3P%</th><th>FT%</th></tr>
      </thead>
      <tbody>
        {''.join(out_rows) if out_rows else '<tr><td colspan=\"12\">No removed players.</td></tr>'}
      </tbody>
    </table>
  </div>

  <div class='panel'>
    <h2>Selected Players</h2>
    <table>
      <thead>
        <tr><th>Player</th><th>Current Team</th><th>Season</th><th>Min</th><th>Src Conf</th><th>Transfer Adj</th><th>Off Rtg</th><th>Def Rtg</th><th>BPM</th><th>RAPM</th><th>PPG</th></tr>
      </thead>
      <tbody>
        {''.join(player_rows)}
      </tbody>
    </table>
  </div>

  <div class='panel'>
    <h2>Team Projection: Current vs Edited</h2>
    <table>
      <thead><tr><th>Metric</th><th>Current Team</th><th>Current Rank</th><th>Edited Roster</th><th>Edited Rank</th><th>Delta</th></tr></thead>
      <tbody>{''.join(metric_rows)}</tbody>
    </table>
  </div>
</body>
</html>
"""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Roster simulator using BT + transfer-up translation")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--bt-csv", default="player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv")
    ap.add_argument("--roster-csv", required=True, help="CSV with columns: player,team,season,minutes,destination_conference")
    ap.add_argument("--season", type=int, required=True, help="Season to evaluate")
    ap.add_argument("--default-minutes", type=float, default=20.0)
    ap.add_argument("--default-destination-conference", default="")
    ap.add_argument("--base-team", default="", help="Base team for current-roster comparison. If empty, inferred from selected players.")
    ap.add_argument("--interaction-model", action="store_true", help="Enable interaction-adjusted team simulation.")
    ap.add_argument("--out-html", default="player_cards_pipeline/output/roster_simulator_report.html")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    mod = load_module(root)
    bt_rows = read_bt_rows((root / args.bt_csv).resolve())
    inputs = read_roster_csv((root / args.roster_csv).resolve(), args.season, args.default_minutes, args.default_destination_conference)

    history_examples = build_transfer_examples(mod, bt_rows)

    resolved: list[ResolvedPlayer] = []
    missing: list[InputPlayer] = []
    for p in inputs:
        row = find_bt_row(mod, bt_rows, p)
        if row is None:
            missing.append(p)
            continue
        projected, transfer_applied = project_transfer_metrics(mod, row, p.destination_conference, history_examples)
        src_conf = mod._conference_key(mod.bt_get(row, ["conf", "conference"]))
        resolved.append(ResolvedPlayer(inp=p, bt_row=row, projected=projected, source_conf=src_conf, transfer_applied=transfer_applied))

    if not resolved:
        raise RuntimeError("No players matched from roster CSV. Check player/team/season values.")

    pace_scale = estimate_pace_scale(mod, bt_rows, args.season)
    edited_summary, total_minutes = aggregate_team(
        resolved,
        interaction_model=args.interaction_model,
        pace_scale=pace_scale,
    )

    base_team = args.base_team.strip() or infer_base_team(mod, resolved)
    current_players = build_current_team_players(mod, bt_rows, args.season, base_team) if base_team else []
    current_summary, _ = aggregate_team(current_players, interaction_model=False, pace_scale=pace_scale)
    league_team_summaries = build_season_team_summaries(mod, bt_rows, args.season)
    edited_player_metrics = projected_player_metrics(resolved, interaction_model=args.interaction_model)
    current_player_metrics = projected_player_metrics(current_players, interaction_model=False)
    in_rows, out_rows = build_in_out_rows(
        mod=mod,
        base_team=base_team,
        edited_players=resolved,
        edited_metrics=edited_player_metrics,
        current_players=current_players,
        current_metrics=current_player_metrics,
    )

    out_path = (root / args.out_html).resolve()
    render_html(
        out_path,
        args.season,
        resolved,
        edited_summary,
        total_minutes,
        current_summary,
        base_team,
        league_team_summaries,
        in_rows,
        out_rows,
        interaction_model=args.interaction_model,
    )

    print(f"wrote {out_path}")
    print(f"matched_players={len(resolved)} missing_players={len(missing)}")
    print(f"base_team={base_team} current_roster_players={len(current_players)}")
    for p in missing[:20]:
        print(f"missing: {p.player} | team={p.team} | season={p.season}")


if __name__ == "__main__":
    main()
