#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


BT_CANDIDATES = [
    "player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv",
    "player_cards_pipeline/data/bt/bt_advstats_2019_2026.csv",
    "player_cards_pipeline/data/bt/bt_advstats_2010_2025.csv",
    "player_cards_pipeline/data/bt/bt_advstats_2019_2025.csv",
    "player_cards_pipeline/data/bt/bt_advstats_2026.csv",
]

ENRICHED_SCRIPT_DIR = "player_cards_pipeline/data/manual/enriched_players/by_script_season"
HEIGHT_SCORE_PATTERN = "player_cards_pipeline/output/height_profile_scores_{season}.csv"
HEIGHT_SCORE_BIG_PATTERN = "player_cards_pipeline/output/height_profile_scores_big_{season}.csv"
RIMFLUENCE_DIR = "player_cards_pipeline/data/manual/rimfluence"


@dataclass
class Row:
    season: int
    player_name: str
    team: str
    pid: str
    gp: float
    mpg: float | None
    min_per: float
    position: str
    player_class: str
    draft_pick: str
    age: float | None
    height_inches: float | None
    listed_height: str
    stat_height: str
    height_delta: float | None
    rimfluence: float | None
    rimfluence_off: float | None
    rimfluence_def: float | None
    a_to: float | None
    oreb: float | None
    stl_foul: float | None
    ftr: float | None


def to_float(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    s = str(v).strip().replace("%", "")
    if not s:
        return default
    try:
        x = float(s)
        if math.isfinite(x):
            return x
    except Exception:
        return default
    return default


def norm(s: str) -> str:
    s = (s or "").strip().lower().replace("’", "'")
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", " ", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_pos(raw: str | None) -> str:
    p = (raw or "").strip().upper()
    if not p:
        return ""
    token_map = {
        "PG": "G",
        "SG": "G",
        "G": "G",
        "GUARD": "G",
        "SF": "F",
        "PF": "F",
        "F": "F",
        "FORWARD": "F",
        "WING": "F",
        "C": "C",
        "CENTER": "C",
        "CENTRE": "C",
    }
    parts = [t for t in re.split(r"[^A-Z]+", p) if t]
    for t in parts:
        if t in token_map:
            return token_map[t]
    if "GUARD" in p:
        return "G"
    if "FORWARD" in p or "WING" in p:
        return "F"
    if "CENTER" in p or "CENTRE" in p:
        return "C"
    return ""


def parse_height_inches(raw: str | None) -> float | None:
    s = (raw or "").strip().lower()
    if not s:
        return None
    m = re.match(r"^(\d+)\s*[-' ]\s*(\d+)$", s)
    if m:
        ft = int(m.group(1))
        inch = int(m.group(2))
        if 4 <= ft <= 8 and 0 <= inch <= 11:
            return float(ft * 12 + inch)
    if re.match(r"^\d{2,3}(\.\d+)?$", s):
        x = float(s)
        if 55 <= x <= 100:
            return x
    return None


def inches_to_h(x: float | None) -> str:
    if x is None or not math.isfinite(x):
        return "N/A"
    n = int(round(x))
    return f"{n // 12}'{n % 12}\""


def parse_dob_to_age(dob: str | None, season: int) -> float | None:
    s = (dob or "").strip()
    if not s:
        return None
    fmts = ["%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"]
    d = None
    for f in fmts:
        try:
            from datetime import datetime

            d = datetime.strptime(s, f).date()
            break
        except Exception:
            pass
    if d is None:
        return None
    ref = date(season, 1, 1)
    return (ref - d).days / 365.25


def class_to_age(c: str | None) -> float | None:
    k = (c or "").strip().lower().replace(".", "")
    if not k:
        return None
    lut = {
        "fr": 18.5,
        "freshman": 18.5,
        "rsfr": 19.2,
        "so": 19.5,
        "soph": 19.5,
        "sophomore": 19.5,
        "rsso": 20.2,
        "jr": 20.5,
        "junior": 20.5,
        "rsjr": 21.2,
        "sr": 21.5,
        "senior": 21.5,
        "rssr": 22.2,
        "gr": 22.8,
        "grad": 22.8,
        "graduate": 22.8,
    }
    return lut.get(k)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def resolve_bt_csv(root: Path, requested: str | None) -> Path:
    cands: list[Path] = []
    if requested:
        rp = Path(requested)
        cands.append(rp if rp.is_absolute() else root / rp)
    cands.extend(root / p for p in BT_CANDIDATES)
    for p in cands:
        if p.exists():
            return p
    raise FileNotFoundError("Could not find BT CSV in configured candidates")


def load_enriched_lookup(root: Path, gender: str, seasons: set[int]) -> dict[tuple[int, str, str], dict[str, str]]:
    out: dict[tuple[int, str, str], dict[str, str]] = {}
    d = root / ENRICHED_SCRIPT_DIR
    if not d.exists():
        return out
    g = "Men" if gender.lower().startswith("m") else "Women"
    for season in sorted(seasons):
        matches = sorted(d.glob(f"players_all_{g}_scriptSeason_{season}_fromJsonYear_*.json"))
        if not matches:
            continue
        payload = json.loads(matches[0].read_text(encoding="utf-8"))
        players = payload.get("players", []) if isinstance(payload, dict) else []
        for p in players:
            pid = str(p.get("_id", "")).split("_", 1)[0].strip()
            team = str(p.get("team", "")).strip()
            key = str(p.get("key", "")).strip()
            if not key:
                continue
            rec = p.get("roster", {}) if isinstance(p.get("roster"), dict) else {}
            out[(season, norm(key), norm(team))] = {
                "pos": str(rec.get("pos", "") or "").strip().upper(),
                "class": str(rec.get("year_class", "") or "").strip(),
                "height": str(rec.get("height", "") or "").strip(),
                "pid": pid,
            }
    return out


def load_height_scores(root: Path, seasons: set[int]) -> tuple[dict[tuple[int, str, str], dict[str, Any]], dict[tuple[int, str], dict[str, Any]]]:
    by_key: dict[tuple[int, str, str], dict[str, Any]] = {}
    by_pid: dict[tuple[int, str], dict[str, Any]] = {}
    for season in sorted(seasons):
        p_big = root / HEIGHT_SCORE_BIG_PATTERN.format(season=season)
        p_std = root / HEIGHT_SCORE_PATTERN.format(season=season)
        p = p_big if p_big.exists() else p_std
        if not p.exists():
            continue
        for r in read_csv_rows(p):
            rec = {
                "listed_height": r.get("listed_height", "") or "",
                "predicted_height": r.get("predicted_profile_height", "") or "",
                "delta": to_float(r.get("height_delta_inches")),
            }
            key = (season, norm(r.get("player_name", "")), norm(r.get("team", "")))
            by_key[key] = rec
            pid = str(r.get("pid", "")).strip()
            if pid:
                by_pid[(season, pid)] = rec
    return by_key, by_pid


def _pick_col(header: list[str], candidates: list[str]) -> str | None:
    hmap = {re.sub(r"[^a-z0-9]", "", c.lower()): c for c in header}
    for c in candidates:
        k = re.sub(r"[^a-z0-9]", "", c.lower())
        if k in hmap:
            return hmap[k]
    return None


def load_rimfluence(root: Path, seasons: set[int], gender: str) -> dict[tuple[int, str, str], dict[str, float | None]]:
    out: dict[tuple[int, str, str], dict[str, float | None]] = {}
    search_dirs = [
        root / RIMFLUENCE_DIR,
        root / "player_cards_pipeline/output",
        root / "cbbd_dump/season_2026/tables",
        root,
    ]
    files: list[Path] = []
    seen: set[str] = set()
    for d in search_dirs:
        if not d.exists():
            continue
        for f in sorted(d.glob("*rimfluence*.csv")):
            sf = str(f.resolve())
            if sf in seen:
                continue
            seen.add(sf)
            # Skip raw lineup dump files only (keep rimfluence result exports).
            lname = f.name.lower()
            if lname.startswith("lineups_"):
                continue
            files.append(f)

    if not files:
        return out

    for f in files:
        try:
            rows = read_csv_rows(f)
        except Exception:
            continue
        if not rows:
            continue
        header = list(rows[0].keys())
        season_col = _pick_col(header, ["season", "year"]) or ""
        player_col = _pick_col(header, ["player", "player_name", "name", "key"]) or ""
        team_col = _pick_col(header, ["team", "school"]) or ""
        rim_col = _pick_col(header, ["rimfluence", "rimfluence_score", "rimfluence_value", "rimfluence_pts", "total_rimfluence"])
        rim_off_col = _pick_col(header, ["off_rimfluence", "offensive_rimfluence", "rimfluence_off", "offrimfluence", "o_rimfluence", "orimfluence"])
        rim_def_col = _pick_col(header, ["def_rimfluence", "defensive_rimfluence", "rimfluence_def", "defrimfluence", "d_rimfluence", "drimfluence"])
        gender_col = _pick_col(header, ["gender", "sex"])
        if not season_col or not player_col or (not rim_col and not rim_off_col and not rim_def_col):
            # allow season inference from filename for older exports
            if not player_col or (not rim_col and not rim_off_col and not rim_def_col):
                continue
        inferred_season = None
        sm = re.search(r"(20\d{2})", f.name)
        if sm:
            try:
                inferred_season = int(sm.group(1))
            except Exception:
                inferred_season = None
        for r in rows:
            season = int(to_float(r.get(season_col), 0) or 0) if season_col else int(inferred_season or 0)
            if season not in seasons:
                continue
            if gender_col:
                g = str(r.get(gender_col, "")).strip().lower()
                if g and ((gender.startswith("m") and "men" not in g and g != "m") or (gender.startswith("w") and "women" not in g and g != "w")):
                    continue
            rv = to_float(r.get(rim_col)) if rim_col else None
            rv_off = to_float(r.get(rim_off_col)) if rim_off_col else None
            rv_def = to_float(r.get(rim_def_col)) if rim_def_col else None
            if rv is None and rv_off is not None and rv_def is not None:
                rv = rv_off + rv_def
            if rv is None and rv_off is None and rv_def is None:
                continue
            key = (season, norm(r.get(player_col, "")), norm(r.get(team_col, "")))
            out[key] = {"rimfluence": rv, "rimfluence_off": rv_off, "rimfluence_def": rv_def}
            # fallback key when team labels don't match across sources
            key_no_team = (season, norm(r.get(player_col, "")), "")
            if key_no_team not in out:
                out[key_no_team] = {"rimfluence": rv, "rimfluence_off": rv_off, "rimfluence_def": rv_def}
    return out


def percentile(value: float | None, arr: list[float]) -> float | None:
    if value is None or not arr:
        return None
    s = sorted(x for x in arr if x is not None and math.isfinite(x))
    if not s:
        return None
    lo = sum(1 for x in s if x < value)
    eq = sum(1 for x in s if x == value)
    return 100.0 * (lo + 0.5 * eq) / len(s)


def fit_age_height_adjust(rows: list[Row], raw_scores: list[float]) -> list[float]:
    # OLS for raw ~ 1 + age + height with simple normal equations.
    idx = [i for i, r in enumerate(rows) if r.age is not None and r.height_inches is not None and raw_scores[i] is not None]
    if len(idx) < 12:
        return raw_scores

    # Build XtX, Xty for 3 params.
    xtx = [[0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]]
    xty = [0.0, 0.0, 0.0]
    for i in idx:
        x = [1.0, float(rows[i].age), float(rows[i].height_inches)]
        y = float(raw_scores[i])
        for a in range(3):
            xty[a] += x[a] * y
            for b in range(3):
                xtx[a][b] += x[a] * x[b]

    def solve3(a: list[list[float]], b: list[float]) -> list[float] | None:
        m = [row[:] + [b[i]] for i, row in enumerate(a)]
        n = 3
        for col in range(n):
            pivot = max(range(col, n), key=lambda r: abs(m[r][col]))
            if abs(m[pivot][col]) < 1e-10:
                return None
            m[col], m[pivot] = m[pivot], m[col]
            div = m[col][col]
            for j in range(col, n + 1):
                m[col][j] /= div
            for r in range(n):
                if r == col:
                    continue
                fac = m[r][col]
                for j in range(col, n + 1):
                    m[r][j] -= fac * m[col][j]
        return [m[i][n] for i in range(n)]

    beta = solve3(xtx, xty)
    if beta is None:
        return raw_scores

    mu = sum(raw_scores[i] for i in idx) / len(idx)
    out = raw_scores[:]
    for i in idx:
        pred = beta[0] + beta[1] * float(rows[i].age) + beta[2] * float(rows[i].height_inches)
        out[i] = float(raw_scores[i]) - (pred - mu)
    return out


def build_rows(root: Path, bt_csv: Path, gender: str, seasons: set[int], min_games: float, min_min_per: float, men_rimfluence_cutoff: int) -> list[Row]:
    raw_bt_rows = read_csv_rows(bt_csv)
    dedup: dict[tuple[int, str, str], dict[str, str]] = {}
    for r in raw_bt_rows:
        season = int(to_float(r.get("year"), 0) or 0)
        if season not in seasons:
            continue
        player = str(r.get("player_name", "")).strip()
        team = str(r.get("team", "")).strip()
        if not player or not team:
            continue
        key = (season, norm(player), norm(team))
        prev = dedup.get(key)
        if prev is None:
            dedup[key] = r
            continue
        prev_gp = to_float(prev.get("GP"), 0.0) or 0.0
        cur_gp = to_float(r.get("GP"), 0.0) or 0.0
        prev_min = to_float(prev.get("Min_per"), 0.0) or 0.0
        cur_min = to_float(r.get("Min_per"), 0.0) or 0.0
        if cur_gp > prev_gp or (cur_gp == prev_gp and cur_min > prev_min):
            dedup[key] = r
    bt_rows = list(dedup.values())
    enr = load_enriched_lookup(root, gender, seasons)
    hmap_by_key, hmap_by_pid = load_height_scores(root, seasons)
    rmap = load_rimfluence(root, seasons, gender)

    out: list[Row] = []
    for r in bt_rows:
        season = int(to_float(r.get("year"), 0) or 0)
        if season not in seasons:
            continue
        gp = to_float(r.get("GP"), 0.0) or 0.0
        min_per = to_float(r.get("Min_per"), 0.0) or 0.0
        if gp < min_games or min_per < min_min_per:
            continue

        player = str(r.get("player_name", "")).strip()
        team = str(r.get("team", "")).strip()
        if not player or not team:
            continue
        pid = str(r.get("pid", "")).strip()
        k = (season, norm(player), norm(team))

        em = enr.get(k, {})
        hm = hmap_by_key.get(k, {})
        if not hm and pid:
            hm = hmap_by_pid.get((season, pid), {})

        # Position + class from enriched; fallback to BT fields.
        pos = normalize_pos(em.get("pos", ""))
        if not pos:
            pos = normalize_pos(str(r.get("role", "")))
        if not pos:
            pos = normalize_pos(str(r.get("type", "")))
        cls = (em.get("class", "") or "").strip()
        if not cls:
            cls = str(r.get("yr", "")).strip()
        pick_raw = str(r.get("pick", "") or "").strip()
        draft_pick = ""
        if re.match(r"^\d+$", pick_raw):
            draft_pick = pick_raw

        listed_h_raw = str(r.get("ht", "")).strip() or em.get("height", "") or ""
        listed_h_in = parse_height_inches(listed_h_raw)
        listed_h_fmt = inches_to_h(listed_h_in) if listed_h_in is not None else (str(hm.get("listed_height", "")).strip() or "N/A")

        # age from BT dob; fallback from class.
        age = parse_dob_to_age(r.get("dob"), season)
        if age is None:
            age = class_to_age(cls)

        # Components
        a_to = to_float(r.get("ast/tov"))
        if a_to is None:
            ast_p = to_float(r.get("AST_per"))
            to_p = to_float(r.get("TO_per"))
            if ast_p is not None and to_p not in (None, 0.0):
                a_to = ast_p / to_p

        oreb = to_float(r.get("ORB_per"))

        stl = to_float(r.get("stl_per"))
        fouls = to_float(r.get("pfr"))
        stl_foul = None
        if stl is not None and fouls not in (None, 0.0):
            stl_foul = stl / fouls

        ftr = to_float(r.get("ftr"))

        stat_height = str(hm.get("predicted_height", "")).strip() or "N/A"
        delta = to_float(hm.get("delta"))

        rim_row = rmap.get(k, {})
        if not rim_row:
            rim_row = rmap.get((season, norm(player), ""), {})
        rimfluence = to_float(rim_row.get("rimfluence"))
        rimfluence_off = to_float(rim_row.get("rimfluence_off"))
        rimfluence_def = to_float(rim_row.get("rimfluence_def"))
        # Men requirement: N/A before 2024 regardless.
        if gender.lower().startswith("m") and season < men_rimfluence_cutoff:
            rimfluence = None
            rimfluence_off = None
            rimfluence_def = None

        mp_total = to_float(r.get("mp"))
        mpg = None
        if mp_total is not None and gp > 0 and mp_total > gp:
            mpg = mp_total / gp
        if mpg is None:
            mpg = to_float(r.get("mpg"))
        if mpg is None:
            mpg = ((min_per * 40.0) / 100.0) if min_per is not None else None

        out.append(
            Row(
                season=season,
                player_name=player,
                team=team,
                pid=pid,
                gp=gp,
                mpg=mpg,
                min_per=min_per,
                position=pos,
                player_class=cls,
                draft_pick=draft_pick,
                age=age,
                height_inches=listed_h_in,
                listed_height=listed_h_fmt,
                stat_height=stat_height,
                height_delta=delta,
                rimfluence=rimfluence,
                rimfluence_off=rimfluence_off,
                rimfluence_def=rimfluence_def,
                a_to=a_to,
                oreb=oreb,
                stl_foul=stl_foul,
                ftr=ftr,
            )
        )
    return out


def build_jason_stats(rows: list[Row]) -> list[dict[str, Any]]:
    by_season: dict[int, list[Row]] = {}
    for r in rows:
        by_season.setdefault(r.season, []).append(r)

    out: list[dict[str, Any]] = []
    for season, srows in sorted(by_season.items()):
        def mean(vals: list[float | None]) -> float | None:
            v = [x for x in vals if x is not None and math.isfinite(x)]
            return (sum(v) / len(v)) if v else None

        m_ato = mean([r.a_to for r in srows])
        m_oreb = mean([r.oreb for r in srows])
        m_sf = mean([r.stl_foul for r in srows])
        m_ftr = mean([r.ftr for r in srows])

        raw_scores: list[float | None] = []
        for r in srows:
            comps: list[float] = []
            if r.a_to is not None and m_ato and m_ato > 0:
                comps.append(100.0 * r.a_to / m_ato)
            if r.oreb is not None and m_oreb and m_oreb > 0:
                comps.append(100.0 * r.oreb / m_oreb)
            if r.stl_foul is not None and m_sf and m_sf > 0:
                comps.append(100.0 * r.stl_foul / m_sf)
            if r.ftr is not None and m_ftr and m_ftr > 0:
                comps.append(100.0 * r.ftr / m_ftr)
            raw_scores.append((sum(comps) / len(comps)) if comps else None)

        # age/height adjustment
        adjusted = fit_age_height_adjust(srows, [x if x is not None else float("nan") for x in raw_scores])

        # recenter to season mean 100
        valid_adj = [x for x in adjusted if x is not None and math.isfinite(x)]
        mu_adj = (sum(valid_adj) / len(valid_adj)) if valid_adj else 100.0
        feel_scores = [((x * 100.0 / mu_adj) if x is not None and math.isfinite(x) else None) for x in adjusted]

        feel_vals = [x for x in feel_scores if x is not None]
        rim_vals = [r.rimfluence for r in srows if r.rimfluence is not None]
        h_vals = [r.height_delta for r in srows if r.height_delta is not None]

        for i, r in enumerate(srows):
            feel = feel_scores[i]
            out.append(
                {
                    "season": season,
                    "player_name": r.player_name,
                    "team": r.team,
                    "position": r.position,
                    "class": r.player_class,
                    "draft_pick": r.draft_pick,
                    "age": "" if r.age is None else round(r.age, 2),
                    "listed_height": r.listed_height,
                    "statistical_height": r.stat_height,
                    "height_delta_inches": "" if r.height_delta is None else round(r.height_delta, 2),
                    "height_delta_percentile": "" if r.height_delta is None else round(percentile(r.height_delta, h_vals) or 0.0, 2),
                    "feel_plus": "" if feel is None else round(feel, 2),
                    "feel_plus_percentile": "" if feel is None else round(percentile(feel, feel_vals) or 0.0, 2),
                    "rimfluence": "N/A" if r.rimfluence is None else round(r.rimfluence, 3),
                    "rimfluence_off": "N/A" if r.rimfluence_off is None else round(r.rimfluence_off, 3),
                    "rimfluence_def": "N/A" if r.rimfluence_def is None else round(r.rimfluence_def, 3),
                    "rimfluence_percentile": "" if r.rimfluence is None else round(percentile(r.rimfluence, rim_vals) or 0.0, 2),
                    "gp": round(r.gp, 1),
                    "mpg": "" if r.mpg is None else round(r.mpg, 1),
                    "min_pct": round(r.min_per, 2),
                }
            )
    return out


def parse_seasons(raw: str) -> set[int]:
    out: set[int] = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(p))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Build Jason Created Stats table (Feel+, Rimfluence, Statistical Height) by season")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--bt-csv", default="")
    ap.add_argument("--gender", default="Men", choices=["Men", "Women", "men", "women"])
    ap.add_argument("--seasons", default="2019-2026")
    ap.add_argument("--min-games", type=float, default=5.0)
    ap.add_argument("--min-min-per", type=float, default=5.0)
    ap.add_argument("--men-rimfluence-start-season", type=int, default=2024)
    ap.add_argument("--out-csv", default="player_cards_pipeline/output/jason_created_stats.csv")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    bt_csv = resolve_bt_csv(root, args.bt_csv or None)
    seasons = parse_seasons(args.seasons)
    gender = "Men" if str(args.gender).lower().startswith("m") else "Women"

    rows = build_rows(
        root=root,
        bt_csv=bt_csv,
        gender=gender,
        seasons=seasons,
        min_games=args.min_games,
        min_min_per=args.min_min_per,
        men_rimfluence_cutoff=args.men_rimfluence_start_season,
    )
    stats = build_jason_stats(rows)

    out_csv = (root / args.out_csv).resolve() if not Path(args.out_csv).is_absolute() else Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "season",
        "player_name",
        "team",
        "position",
        "class",
        "draft_pick",
        "age",
        "listed_height",
        "statistical_height",
        "height_delta_inches",
        "height_delta_percentile",
        "feel_plus",
        "feel_plus_percentile",
        "rimfluence",
        "rimfluence_off",
        "rimfluence_def",
        "rimfluence_percentile",
        "gp",
        "mpg",
        "min_pct",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(stats)

    print(f"[jason-stats] gender={gender} seasons={min(seasons)}-{max(seasons)} rows={len(stats)} -> {out_csv}")


if __name__ == "__main__":
    main()
