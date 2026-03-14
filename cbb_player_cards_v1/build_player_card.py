#!/usr/bin/env python3
"""Generate a college basketball player card (HTML) from CBBD-style CSV data.

v1 goals:
- Bio block (name/team/position/class/height/age if available)
- Small per-game strip near top
- Percentile bars from cohort in plays dataset
- Shot chart from shot location x/y in plays dataset

Primary input is a plays CSV with columns similar to:
  participants[0].name, team, season,
  scoringPlay, scoreValue,
  playType, shotInfo.shooter.name, shotInfo.made,
  shotInfo.location.x, shotInfo.location.y
"""

from __future__ import annotations

import argparse
import csv
import difflib
import gzip
import html
import json
import math
import random
import re
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


PLAY_TYPES_FT = {"MadeFreeThrow"}
PLAY_TYPES_REBOUND = {"Defensive Rebound", "Offensive Rebound", "Dead Ball Rebound"}

BIO_ALIAS_MAP = {
    "player": ["Player", "player", "player_name", "Name", "name"],
    "team": ["Team", "team", "team_name", "School", "school"],
    "year": ["Year", "year", "season", "Season"],
    "class": ["Class", "class", "Year In School", "year_in_school"],
    "height": ["Height", "height", "roster.height", "HEIGHT_WO_SHOES_FT_IN", "HEIGHT_W_SHOES_FT_IN"],
    "age": ["DD Age", "Age", "age"],
    "position": ["Role", "POSITION", "Position", "position", "roster.pos", "posClass"],
    "conference": ["Conference", "conference", "Conf", "conf"],
    "dob": ["DOB", "dob", "Birthdate", "birthdate", "Birthday", "birthday", "Date of Birth"],
}

CACHE_SCHEMA_VERSION = 1
ENRICHED_GENDER = "Women"


def enriched_gender_token(raw: str) -> str:
    v = norm_text(raw)
    if v in {"women", "w", "female", "ncaaw"}:
        return "Women"
    return "Men"


def enriched_gender_candidates(raw: str) -> list[str]:
    return [enriched_gender_token(raw)]


@dataclass
class PlayerGameStats:
    player: str
    team: str
    season: str
    games: int
    points: int
    rebounds: int
    assists: int
    steals: int
    blocks: int
    fgm: int
    fga: int
    tpm: int
    tpa: int
    ftm: int
    fta: int

    @property
    def ppg(self) -> float:
        return self.points / self.games if self.games else 0.0

    @property
    def rpg(self) -> float:
        return self.rebounds / self.games if self.games else 0.0

    @property
    def apg(self) -> float:
        return self.assists / self.games if self.games else 0.0

    @property
    def spg(self) -> float:
        return self.steals / self.games if self.games else 0.0

    @property
    def bpg(self) -> float:
        return self.blocks / self.games if self.games else 0.0

    @property
    def fg_pct(self) -> float:
        return (100.0 * self.fgm / self.fga) if self.fga else 0.0

    @property
    def tp_pct(self) -> float:
        return (100.0 * self.tpm / self.tpa) if self.tpa else 0.0

    @property
    def ft_pct(self) -> float:
        return (100.0 * self.ftm / self.fta) if self.fta else 0.0


def norm_text(v: Any) -> str:
    if v is None:
        return ""
    return " ".join(str(v).strip().lower().split())


def norm_team(v: Any) -> str:
    s = norm_text(v)
    s = re.sub(r"[^a-z0-9]+", "", s)
    aliases = {
        "connecticut": "uconn",
        "universityofconnecticut": "uconn",
    }
    s = aliases.get(s, s)
    return s


def norm_player_name(v: Any) -> str:
    s = str(v or "").strip()
    if "," in s:
        last, first = s.split(",", 1)
        s = f"{first.strip()} {last.strip()}".strip()
    s = norm_text(s)
    parts = s.split()
    # Ignore common generational suffixes so user input without suffix still matches.
    suffixes = {
        "jr",
        "sr",
        "ii",
        "iii",
        "iv",
        "v",
        "vi",
        "vii",
        "viii",
        "ix",
        "x",
    }
    while parts and re.sub(r"[^a-z0-9]+", "", parts[-1]) in suffixes:
        parts.pop()
    return " ".join(parts)


def card_cache_key(player: str, team: str, season: str) -> str:
    return f"{norm_player_name(player)}|{norm_team(team)}|{norm_season(season)}"


def default_card_cache_db_path(season: str) -> Path:
    return (
        Path(__file__).resolve().parent.parent
        / "player_cards_pipeline"
        / "data"
        / "cache"
        / "card_sections"
        / f"{norm_season(season)}.sqlite"
    )


def load_cached_card_sections(
    cache_db_path: Path,
    target: PlayerGameStats,
    min_games: int,
) -> dict[str, Any] | None:
    if not cache_db_path.exists():
        return None
    key = card_cache_key(target.player, target.team, target.season)
    try:
        conn = sqlite3.connect(str(cache_db_path))
        conn.row_factory = sqlite3.Row
    except Exception:
        return None
    try:
        row = conn.execute(
            "SELECT value FROM metadata WHERE key='schema_version'"
        ).fetchone()
        if not row or int(str(row["value"])) != CACHE_SCHEMA_VERSION:
            return None
        row = conn.execute(
            "SELECT value FROM metadata WHERE key='min_games'"
        ).fetchone()
        if not row or int(str(row["value"])) != int(min_games):
            return None
        row = conn.execute(
            "SELECT payload_json FROM card_cache WHERE cache_key=?",
            (key,),
        ).fetchone()
        if not row:
            return None
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, dict):
            return None
        return payload
    except Exception:
        return None
    finally:
        conn.close()


def norm_season(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    # 2024/25 -> 2025
    m = re.match(r"^\s*(20\d{2})\s*/\s*(\d{2})\s*$", s)
    if m:
        return f"20{m.group(2)}"
    # 2024-25 -> 2025
    m = re.match(r"^\s*(20\d{2})\s*-\s*(\d{2})\s*$", s)
    if m:
        return f"20{m.group(2)}"
    m = re.search(r"(20\d{2})", s)
    return m.group(1) if m else norm_text(s)


def to_bool(v: Any) -> bool:
    s = norm_text(v)
    return s in {"true", "1", "yes", "y"}


def to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if path.suffix.lower() == ".gz":
        fobj = gzip.open(path, "rt", newline="", encoding="utf-8-sig")
    else:
        fobj = path.open("r", newline="", encoding="utf-8-sig")
    with fobj as f:
        rows = list(csv.reader(f))
    if not rows:
        return [], []

    # Handle files with two header rows (e.g., master scouting docs).
    if len(rows) >= 2 and "Player" in rows[1] and rows[0].count("") > (len(rows[0]) * 0.5):
        header = rows[1]
        data_rows = rows[2:]
    else:
        header = rows[0]
        data_rows = rows[1:]

    out: list[dict[str, str]] = []
    for r in data_rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        out.append({header[i]: r[i] for i in range(len(header))})
    return header, out


def _enriched_nested_value(obj: dict[str, Any], *path: str) -> Any:
    cur: Any = obj
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def load_enriched_lookup_for_script_season(
    season: str,
    base_dir: Path | None = None,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    ys = norm_season(season)
    if not ys or not ys.isdigit():
        return {}
    if base_dir is None:
        base_dir = (
            Path(__file__).resolve().parent.parent
            / "player_cards_pipeline"
            / "data"
            / "manual"
            / "enriched_players"
            / "by_script_season"
        )
    if not base_dir.exists():
        return {}

    year = int(ys)
    obj = None
    for g in enriched_gender_candidates(ENRICHED_GENDER):
        preferred = base_dir / f"players_all_{g}_scriptSeason_{year}_fromJsonYear_{year-1}.json"
        candidates: list[Path] = [preferred] if preferred.exists() else sorted(
            base_dir.glob(f"players_all_{g}_scriptSeason_{year}_fromJsonYear_*.json")
        )
        if not candidates:
            continue
        try:
            obj = json.loads(candidates[0].read_text(encoding="utf-8"))
            break
        except Exception:
            continue
    if obj is None:
        return {}
    players = obj.get("players", []) if isinstance(obj, dict) else []
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in players:
        if not isinstance(r, dict):
            continue
        player = norm_player_name(r.get("key", ""))
        team = norm_team(r.get("team", ""))
        if not player or not team:
            continue
        out[(player, team, ys)] = r
    return out


def load_enriched_players_for_script_season(
    season: str,
    base_dir: Path | None = None,
) -> list[dict[str, Any]]:
    ys = norm_season(season)
    if not ys or not ys.isdigit():
        return []
    if base_dir is None:
        base_dir = (
            Path(__file__).resolve().parent.parent
            / "player_cards_pipeline"
            / "data"
            / "manual"
            / "enriched_players"
            / "by_script_season"
        )
    if not base_dir.exists():
        return []
    year = int(ys)
    obj = None
    for g in enriched_gender_candidates(ENRICHED_GENDER):
        preferred = base_dir / f"players_all_{g}_scriptSeason_{year}_fromJsonYear_{year-1}.json"
        candidates: list[Path] = [preferred] if preferred.exists() else sorted(
            base_dir.glob(f"players_all_{g}_scriptSeason_{year}_fromJsonYear_*.json")
        )
        if not candidates:
            continue
        try:
            obj = json.loads(candidates[0].read_text(encoding="utf-8"))
            break
        except Exception:
            continue
    if obj is None:
        return []
    players = obj.get("players", []) if isinstance(obj, dict) else []
    return [r for r in players if isinstance(r, dict)]


def inject_enriched_fields_into_bt_rows(
    bt_rows: list[dict[str, str]],
    base_dir: Path | None = None,
) -> None:
    cache: dict[str, dict[tuple[str, str, str], dict[str, Any]]] = {}
    for r in bt_rows:
        ys = norm_season(bt_get(r, ["year"]))
        if not ys:
            continue
        if ys not in cache:
            cache[ys] = load_enriched_lookup_for_script_season(ys, base_dir=base_dir)
        lookup = cache[ys]
        if not lookup:
            continue
        k = (
            norm_player_name(bt_get(r, ["player_name"])),
            norm_team(bt_get(r, ["team"])),
            ys,
        )
        er = lookup.get(k)
        if not er:
            continue

        off_team_poss = to_float(_enriched_nested_value(er, "off_team_poss", "value"))
        if off_team_poss is not None and math.isfinite(off_team_poss) and off_team_poss > 0:
            r["off_team_poss.value"] = str(off_team_poss)
            # Override generic possessions with requested source.
            r["possessions"] = str(off_team_poss)

        roster_pos = _enriched_nested_value(er, "roster", "pos")
        if roster_pos is not None and str(roster_pos).strip():
            r["roster.pos"] = str(roster_pos).strip()
            r["role"] = str(roster_pos).strip()

        off_rapm = to_float(_enriched_nested_value(er, "off_adj_rapm", "value"))
        def_rapm = to_float(_enriched_nested_value(er, "def_adj_rapm", "value"))
        if off_rapm is not None and math.isfinite(off_rapm):
            r["off_adj_rapm.value"] = str(off_rapm)
        if def_rapm is not None and math.isfinite(def_rapm):
            r["def_adj_rapm.value"] = str(def_rapm)

        # On/off adjusted PPP fields for requested On/Off Net Rtg impact metric.
        for k, path in [
            ("on.off_adj_ppp.value", ("on", "off_adj_ppp", "value")),
            ("on.def_adj_ppp.value", ("on", "def_adj_ppp", "value")),
            ("off.off_adj_ppp.value", ("off", "off_adj_ppp", "value")),
            ("off.def_adj_ppp.value", ("off", "def_adj_ppp", "value")),
        ]:
            v = to_float(_enriched_nested_value(er, *path))
            if v is not None and math.isfinite(v):
                r[k] = str(v)

        # Team impact fields used for On/Off table in cards.
        for k, path in [
            ("on.off_efg.old_value", ("on", "off_efg", "old_value")),
            ("off.off_efg.old_value", ("off", "off_efg", "old_value")),
            ("on.off_to.value", ("on", "off_to", "value")),
            ("off.off_to.value", ("off", "off_to", "value")),
            ("on.off_2prim.value", ("on", "off_2prim", "value")),
            ("off.off_2prim.value", ("off", "off_2prim", "value")),
            ("on.off_2primr.value", ("on", "off_2primr", "value")),
            ("off.off_2primr.value", ("off", "off_2primr", "value")),
            ("on.off_ftr.value", ("on", "off_ftr", "value")),
            ("off.off_ftr.value", ("off", "off_ftr", "value")),
            ("on.off_3pr.value", ("on", "off_3pr", "value")),
            ("off.off_3pr.value", ("off", "off_3pr", "value")),
            ("on.def_efg.old_value", ("on", "def_efg", "old_value")),
            ("off.def_efg.old_value", ("off", "def_efg", "old_value")),
            ("on.def_to.value", ("on", "def_to", "value")),
            ("off.def_to.value", ("off", "def_to", "value")),
            ("on.def_2prim.value", ("on", "def_2prim", "value")),
            ("off.def_2prim.value", ("off", "def_2prim", "value")),
            ("on.def_2primr.value", ("on", "def_2primr", "value")),
            ("off.def_2primr.value", ("off", "def_2primr", "value")),
            ("on.def_ftr.value", ("on", "def_ftr", "value")),
            ("off.def_ftr.value", ("off", "def_ftr", "value")),
            ("on.def_3pr.value", ("on", "def_3pr", "value")),
            ("off.def_3pr.value", ("off", "def_3pr", "value")),
            ("on.off_orb.value", ("on", "off_orb", "value")),
            ("off.off_orb.value", ("off", "off_orb", "value")),
            ("on.def_orb.value", ("on", "def_orb", "value")),
            ("off.def_orb.value", ("off", "def_orb", "value")),
            ("off_ast_rim.value", ("off_ast_rim", "value")),
            ("off_ast_rim.old_value", ("off_ast_rim", "old_value")),
        ]:
            v = to_float(_enriched_nested_value(er, *path))
            if v is not None and math.isfinite(v):
                r[k] = str(v)

        # Net points summary from enriched field.
        net_o = to_float(_enriched_nested_value(er, "net_pts", "o"))
        net_d = to_float(_enriched_nested_value(er, "net_pts", "d"))
        net_owowy = to_float(_enriched_nested_value(er, "net_pts", "oWowy"))
        net_dwowy = to_float(_enriched_nested_value(er, "net_pts", "dWowy"))
        if net_o is not None and math.isfinite(net_o):
            r["net_pts.o"] = str(net_o)
        if net_d is not None and math.isfinite(net_d):
            r["net_pts.d"] = str(net_d)
        if net_owowy is not None and math.isfinite(net_owowy):
            r["net_pts.oWowy"] = str(net_owowy)
        if net_dwowy is not None and math.isfinite(net_dwowy):
            r["net_pts.dWowy"] = str(net_dwowy)
        if net_o is not None and net_d is not None and math.isfinite(net_o) and math.isfinite(net_d):
            wowy_total = 0.0
            if net_owowy is not None and math.isfinite(net_owowy):
                wowy_total += float(net_owowy)
            if net_dwowy is not None and math.isfinite(net_dwowy):
                wowy_total += float(net_dwowy)
            r["net_pts.value"] = str((float(net_o) + float(net_d)) - wowy_total)


def _shot_range_from_xy_ft(x_ft: float, y_ft: float) -> str:
    d = math.hypot(float(x_ft), float(y_ft))
    if d <= 4.5:
        return "rim"
    if d >= 22.0:
        return "three_pointer"
    return "jumper"


def build_shots_from_enriched_player_row(
    enriched_row: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, int]:
    info = _enriched_nested_value(enriched_row, "shotInfo", "data", "info")
    if not isinstance(info, list):
        return [], 0, 0

    shots: list[dict[str, Any]] = []
    made_total = 0
    att_total = 0
    for rec in info:
        if not isinstance(rec, list) or len(rec) < 4:
            continue
        try:
            x_ft = float(rec[0])
            y_ft = float(rec[1])
            fg_points = float(rec[2])
            fga = int(round(float(rec[3])))
        except Exception:
            continue
        if fga <= 0:
            continue
        att_total += fga
        shot_value = 3.0 if _shot_range_from_xy_ft(x_ft, y_ft) == "three_pointer" else 2.0
        makes = int(round(fg_points / shot_value))
        makes = max(0, min(fga, makes))
        misses = fga - makes
        made_total += makes
        rng = _shot_range_from_xy_ft(x_ft, y_ft)

        # Small jitter so expanded bin shots are visible as distinct points.
        for _ in range(makes):
            shots.append(
                {
                    "x": (x_ft + 4.0) * 10.0 + random.uniform(-2.0, 2.0),
                    "y": (y_ft + 25.0) * 10.0 + random.uniform(-2.0, 2.0),
                    "made": True,
                    "range": rng,
                }
            )
        for _ in range(misses):
            shots.append(
                {
                    "x": (x_ft + 4.0) * 10.0 + random.uniform(-2.0, 2.0),
                    "y": (y_ft + 25.0) * 10.0 + random.uniform(-2.0, 2.0),
                    "made": False,
                    "range": rng,
                }
            )

    return shots, made_total, att_total


def pps_over_expected_from_enriched(
    target: PlayerGameStats,
) -> tuple[float | None, float | None, float | None, float | None]:
    players = load_enriched_players_for_script_season(target.season)
    if not players:
        return None, None, None, None

    # League expected PPS by shot bin key.
    bin_pts: dict[str, float] = defaultdict(float)
    bin_att: dict[str, float] = defaultdict(float)
    for p in players:
        keys = _enriched_nested_value(p, "shotInfo", "data", "keys")
        info = _enriched_nested_value(p, "shotInfo", "data", "info")
        if not isinstance(keys, list) or not isinstance(info, list):
            continue
        n = min(len(keys), len(info))
        for i in range(n):
            rec = info[i]
            if not isinstance(rec, list) or len(rec) < 4:
                continue
            pts = to_float(rec[2])
            att = to_float(rec[3])
            if pts is None or att is None or att <= 0:
                continue
            k = str(keys[i])
            bin_pts[k] += float(pts)
            bin_att[k] += float(att)
    exp_pps_by_key = {k: (bin_pts[k] / bin_att[k]) for k in bin_att if bin_att[k] > 0}
    if not exp_pps_by_key:
        return None, None, None, None

    def player_pps_oe(p: dict[str, Any]) -> tuple[float | None, float | None, float | None]:
        keys = _enriched_nested_value(p, "shotInfo", "data", "keys")
        info = _enriched_nested_value(p, "shotInfo", "data", "info")
        if not isinstance(keys, list) or not isinstance(info, list):
            return None, None, None
        n = min(len(keys), len(info))
        act_pts = 0.0
        att_sum = 0.0
        exp_pts = 0.0
        for i in range(n):
            rec = info[i]
            if not isinstance(rec, list) or len(rec) < 4:
                continue
            pts = to_float(rec[2])
            att = to_float(rec[3])
            if pts is None or att is None or att <= 0:
                continue
            k = str(keys[i])
            exp_pps = exp_pps_by_key.get(k)
            if exp_pps is None:
                continue
            act_pts += float(pts)
            att_sum += float(att)
            exp_pts += float(exp_pps) * float(att)
        if att_sum <= 0:
            return None, None, None
        actual_pps = act_pts / att_sum
        expected_pps = exp_pts / att_sum
        if expected_pps <= 0:
            return actual_pps, expected_pps, None
        pct_change = ((actual_pps - expected_pps) / expected_pps) * 100.0
        return actual_pps, expected_pps, pct_change

    target_row = None
    tk = (norm_player_name(target.player), norm_team(target.team), norm_season(target.season))
    for p in players:
        if (
            norm_player_name(p.get("key", "")) == tk[0]
            and norm_team(p.get("team", "")) == tk[1]
        ):
            target_row = p
            break
    if target_row is None:
        return None, None, None, None

    targ_actual, targ_expected, targ_pct = player_pps_oe(target_row)
    if targ_actual is None or targ_expected is None or targ_pct is None:
        return targ_actual, targ_expected, targ_pct, None

    cohort_vals: list[float] = []
    for p in players:
        _, _, c = player_pps_oe(p)
        if c is not None and math.isfinite(c):
            cohort_vals.append(float(c))
    pctile = percentile(float(targ_pct), cohort_vals) if cohort_vals else None
    return targ_actual, targ_expected, targ_pct, pctile


def find_col(header: list[str], aliases: list[str]) -> str | None:
    hset = {h: norm_text(h) for h in header}
    alias_norm = {norm_text(a) for a in aliases}
    for col, normed in hset.items():
        if normed in alias_norm:
            return col
    return None


def load_bio_lookup(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    def load_trank_fixed_lookup() -> dict[tuple[str, str, str], dict[str, str]]:
        lookup: dict[tuple[str, str, str], dict[str, str]] = {}
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        if not rows:
            return lookup
        for r in rows:
            if len(r) < 67:
                continue
            player = r[0].strip()
            team = r[1].strip()
            year = r[31].strip()  # 1-based col 32
            if not player or not team or not year:
                continue
            key = key_player_team_season(player, team, year)
            if key in lookup:
                continue
            lookup[key] = {
                "class": r[25].strip(),      # 1-based col 26
                "height": r[26].strip(),     # 1-based col 27
                "age": "",
                "position": r[64].strip(),   # 1-based col 65
                "conference": r[2].strip(),  # 1-based col 3
                "dob": r[66].strip(),        # 1-based col 67
            }
        return lookup

    header, rows = read_csv_rows(path)
    if not header:
        return load_trank_fixed_lookup()
    col_player = find_col(header, BIO_ALIAS_MAP["player"])
    col_team = find_col(header, BIO_ALIAS_MAP["team"])
    col_year = find_col(header, BIO_ALIAS_MAP["year"])
    if not col_player or not col_team or not col_year:
        return load_trank_fixed_lookup()

    bio_cols = {
        "class": find_col(header, BIO_ALIAS_MAP["class"]),
        "height": find_col(header, BIO_ALIAS_MAP["height"]),
        "age": find_col(header, BIO_ALIAS_MAP["age"]),
        "position": find_col(header, BIO_ALIAS_MAP["position"]),
        "conference": find_col(header, BIO_ALIAS_MAP["conference"]),
        "dob": find_col(header, BIO_ALIAS_MAP["dob"]),
    }

    lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = key_player_team_season(
            row.get(col_player, ""),
            row.get(col_team, ""),
            row.get(col_year, ""),
        )
        if key in lookup:
            continue
        lookup[key] = {
            k: (row.get(v, "") if v else "")
            for k, v in bio_cols.items()
        }
    return lookup


def key_player_team_season(player: str, team: str, season: str) -> tuple[str, str, str]:
    return norm_player_name(player), norm_team(team), norm_season(season)


def lookup_bio_fallback(
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    player: str,
    team: str,
    season: str,
) -> dict[str, str]:
    exact = bio_lookup.get(key_player_team_season(player, team, season))
    if exact:
        return exact
    np = norm_player_name(player)
    ns = norm_season(season)
    nt = norm_team(team)
    candidates: list[tuple[float, dict[str, str]]] = []
    for (bp, bt, by), bio in bio_lookup.items():
        if bp != np or by != ns:
            continue
        score = difflib.SequenceMatcher(None, nt, bt).ratio()
        candidates.append((score, bio))
    if not candidates:
        return {"class": "", "height": "", "age": "", "position": "", "conference": "", "dob": ""}
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _season_from_row(row: dict[str, str], season_hint: str = "") -> str:
    s = str(row.get("season", "")).strip()
    if s:
        return norm_season(s)
    if season_hint:
        return norm_season(season_hint)
    d = str(row.get("date", "")).strip()
    m = re.match(r"^\s*(\d{4})-(\d{2})-\d{2}\s*$", d)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        # NCAA season year label: Jan-Jun -> same year, Jul-Dec -> next year.
        return str(y if mo <= 6 else y + 1)
    return ""


def _resolve_side_team(row: dict[str, str], side_key: str) -> str:
    raw = str(row.get(side_key, "")).strip()
    side = norm_text(raw)
    if side == "home":
        return str(row.get("home", "")).strip()
    if side == "away":
        return str(row.get("away", "")).strip()
    # Older feeds provide actual team names in shot_team/action_team.
    if raw and side not in {"na", "none", "nan"}:
        return raw
    return ""


def _team_from_row(row: dict[str, str]) -> str:
    team = str(row.get("team", "")).strip()
    if team and norm_text(team) not in {"na", "none", "nan"}:
        return team
    t = _resolve_side_team(row, "shot_team")
    if t:
        return t
    t = _resolve_side_team(row, "action_team")
    if t:
        return t
    t = str(row.get("possession_before", "")).strip()
    if t and norm_text(t) not in {"na", "none", "nan"}:
        return t
    return ""


def _shot_made_from_row(row: dict[str, str]) -> bool:
    made = row.get("shotInfo.made")
    if made is not None and str(made).strip() != "":
        return to_bool(made)
    return norm_text(row.get("shot_outcome", "")) == "made"


def _shot_range_from_row(row: dict[str, str]) -> str:
    rng = norm_text(row.get("shotInfo.range", ""))
    if rng in {"rim", "jumper", "three_pointer"}:
        return rng

    if to_bool(row.get("three_pt", "")):
        return "three_pointer"

    sx = to_float(row.get("shot_x"))
    sy = to_float(row.get("shot_y"))
    if sx is not None and sy is not None:
        # ncaahoopR shot coords are in feet; hoops near x=+/-41.75, y=0.
        d1 = math.hypot(sx - 41.75, sy)
        d2 = math.hypot(sx + 41.75, sy)
        if min(d1, d2) <= 4.5:
            return "rim"
    return "jumper"


def _shot_loc_from_row(row: dict[str, str]) -> tuple[float | None, float | None]:
    def candidate_transforms(sx: float | None, sy: float | None) -> list[tuple[float, float]]:
        if sx is None or sy is None:
            return []
        cands: list[tuple[float, float]] = []
        # 1) Centered feet.
        if -60.0 <= sx <= 60.0 and -35.0 <= sy <= 35.0:
            cands.append(((sx + 47.0) * 10.0, (sy + 25.0) * 10.0))
        # 2) Positive feet (length, width).
        if 0.0 <= sx <= 100.0 and 0.0 <= sy <= 60.0:
            cands.append((sx * 10.0, sy * 10.0))
        # 3) Positive feet swapped (width, length).
        if 0.0 <= sx <= 60.0 and 0.0 <= sy <= 100.0:
            cands.append((sy * 10.0, sx * 10.0))
        # 4) Full-scale.
        if 0.0 <= sx <= 940.0 and 0.0 <= sy <= 650.0:
            cands.append((sx, max(0.0, min(500.0, sy))))
        if not cands:
            cands.append(((sx + 47.0) * 10.0, (sy + 25.0) * 10.0))
        return cands

    def pick_best(cands: list[tuple[float, float]]) -> tuple[float | None, float | None]:
        if not cands:
            return None, None
        three = to_bool(row.get("three_pt", ""))
        desc = (row.get("description", "") or "").lower()
        wants_rim = any(k in desc for k in ["dunk", "layup", "tip in", "tip shot", "alley oop"])
        best = None
        best_score = float("inf")
        for fx, fy in cands:
            score = 0.0
            if fx < 0 or fx > 940:
                score += 100.0 + abs(min(0.0, fx)) + abs(max(0.0, fx - 940.0))
            if fy < 0 or fy > 500:
                score += 100.0 + abs(min(0.0, fy)) + abs(max(0.0, fy - 500.0))
            xft, yft = fx / 10.0, fy / 10.0
            d1 = math.hypot(xft - 4.0, yft - 25.0)
            d2 = math.hypot(xft - 90.0, yft - 25.0)
            d = min(d1, d2)
            if three and d < 19.0:
                score += (19.0 - d) * 3.0
            if wants_rim and d > 10.0:
                score += (d - 10.0) * 1.5
            if ("jumper" in desc or "jump shot" in desc) and (not three) and d < 5.0:
                score += (5.0 - d) * 2.0
            if score < best_score:
                best_score = score
                best = (fx, fy)
        return best if best is not None else (None, None)

    # Prefer shotInfo location when present, but normalize it as well.
    sx = to_float(row.get("shotInfo.location.x"))
    sy = to_float(row.get("shotInfo.location.y"))
    x, y = pick_best(candidate_transforms(sx, sy))
    if x is not None and y is not None:
        return x, y

    sx = to_float(row.get("shot_x"))
    sy = to_float(row.get("shot_y"))
    return pick_best(candidate_transforms(sx, sy))


def _desc_rebound_player(desc: str) -> str:
    m = re.match(r"^(.*?) (Offensive|Defensive) Rebound\.", desc.strip())
    return m.group(1).strip() if m else ""


def _desc_steal_player(desc: str) -> str:
    m = re.match(r"^(.*?) Steal\.", desc.strip())
    return m.group(1).strip() if m else ""


def _desc_block_player(desc: str) -> str:
    s = desc.strip()
    m = re.match(r"^(.*?) Block\.", s)
    if m:
        return m.group(1).strip()
    m = re.search(r"Block by (.*?)\.", s)
    return m.group(1).strip() if m else ""


def _desc_assister(desc: str) -> str:
    m = re.search(r"Assisted by (.*?)\.", desc)
    return m.group(1).strip() if m else ""


def _desc_shot_info(desc: str) -> tuple[str, bool, str, bool]:
    s = (desc or "").strip()
    m = re.match(r"^(.*?) (made|missed) (.*)$", s)
    if not m:
        return "", False, "", False
    player = m.group(1).strip()
    made = m.group(2) == "made"
    tail = m.group(3).lower()
    is_ft = "free throw" in tail
    if is_ft:
        return player, made, "", True
    if "three point" in tail or "3-point" in tail:
        return player, made, "three_pointer", False
    if any(k in tail for k in ["layup", "dunk", "tip in", "tip shot", "alley oop"]):
        return player, made, "rim", False
    return player, made, "jumper", False


def build_player_stats(
    plays_rows: list[dict[str, str]],
    season_hint: str = "",
    team_hint_by_player_season: dict[tuple[str, str], str] | None = None,
) -> tuple[dict[tuple[str, str, str], PlayerGameStats], dict[tuple[str, str, str], set[str]]]:
    stats: dict[tuple[str, str, str], dict[str, Any]] = {}
    games_by_player: dict[tuple[str, str, str], set[str]] = {}

    for row in plays_rows:
        season = _season_from_row(row, season_hint)
        team = _team_from_row(row)
        shooter = (row.get("shotInfo.shooter.name", "") or row.get("shooter", "")).strip()
        participant_0 = row.get("participants[0].name", "").strip()
        description = str(row.get("description", "") or "")
        desc_shooter, desc_made, desc_range, desc_is_ft = _desc_shot_info(description)
        if not shooter and desc_shooter:
            shooter = desc_shooter
        if not participant_0:
            participant_0 = _desc_rebound_player(description) or _desc_steal_player(description) or _desc_block_player(description)
        if team_hint_by_player_season and season:
            explicit_team = any(
                str(row.get(k, "")).strip() and norm_text(row.get(k, "")) not in {"na", "none", "nan"}
                for k in ("team", "shot_team", "action_team")
            )
            # Legacy logs often only provide possession_before/after and can misattribute
            # defensive events (e.g., steals/blocks). Prefer BT team hints in that case.
            if (not explicit_team) or (not team):
                for actor in [shooter, participant_0, _desc_assister(description)]:
                    if not actor:
                        continue
                    hinted = team_hint_by_player_season.get((norm_player_name(actor), norm_season(season)), "")
                    if hinted:
                        team = hinted
                        break
        if not team:
            continue

        game_id = (
            str(row.get("gameId", "")).strip()
            or str(row.get("gameSourceId", "")).strip()
            or str(row.get("id", "")).strip()
            or str(row.get("game_id", "")).strip()
        )
        play_type = (row.get("playType", "") or "").strip()
        shot_range = _shot_range_from_row(row)
        if not shot_range and desc_range:
            shot_range = desc_range
        shot_is_tracked_attempt = shot_range in {"rim", "jumper", "three_pointer"}
        made = _shot_made_from_row(row)
        if (row.get("shotInfo.made") in (None, "")) and (row.get("shot_outcome") in (None, "", "NA")) and desc_shooter:
            made = desc_made

        def get_bucket(player_name: str) -> dict[str, Any]:
            player_key = key_player_team_season(player_name, team, season)
            if game_id:
                games_by_player.setdefault(player_key, set()).add(game_id)
            return stats.setdefault(
                player_key,
                {
                    "player": player_name,
                    "team": team,
                    "season": season,
                    "points": 0,
                    "rebounds": 0,
                    "assists": 0,
                    "steals": 0,
                    "blocks": 0,
                    "fgm": 0,
                    "fga": 0,
                    "tpm": 0,
                    "tpa": 0,
                    "ftm": 0,
                    "fta": 0,
                },
            )

        # Rebounds/steals/blocks from CBBD event types or ncaahoopR descriptions.
        if play_type in PLAY_TYPES_REBOUND and participant_0:
            bucket = get_bucket(participant_0)
            bucket["rebounds"] += 1
        if play_type == "Steal" and participant_0:
            bucket = get_bucket(participant_0)
            bucket["steals"] += 1
        if play_type == "Block Shot" and participant_0:
            bucket = get_bucket(participant_0)
            bucket["blocks"] += 1
        if not play_type:
            rb = _desc_rebound_player(description)
            if rb:
                bucket = get_bucket(rb)
                bucket["rebounds"] += 1
            st = _desc_steal_player(description)
            if st:
                bucket = get_bucket(st)
                bucket["steals"] += 1
            blk = _desc_block_player(description)
            if blk:
                bucket = get_bucket(blk)
                bucket["blocks"] += 1

        # Field-goal attempts/makes and points should be credited to shooter only.
        has_shot_signal = (_shot_loc_from_row(row)[0] is not None) or (row.get("shot_outcome") not in (None, "", "NA")) or bool(desc_shooter and not desc_is_ft)
        if shooter and shot_is_tracked_attempt and has_shot_signal:
            bucket = get_bucket(shooter)
            bucket["fga"] += 1
            if made:
                bucket["fgm"] += 1

            if shot_range == "three_pointer":
                bucket["tpa"] += 1
                if made:
                    bucket["tpm"] += 1

            score_value = to_float(row.get("scoreValue"))
            if score_value is None:
                score_value = to_float(row.get("score_value"))
            scoring_play = to_bool(row.get("scoringPlay")) or to_bool(row.get("scoring_play")) or made
            if scoring_play and score_value is not None and math.isfinite(score_value):
                bucket["points"] += int(round(score_value))
            elif made:
                bucket["points"] += 3 if shot_range == "three_pointer" else 2

            assister = (row.get("shotInfo.assistedBy.name", "") or row.get("assist", "")).strip() or _desc_assister(description)
            if made and assister:
                assist_bucket = get_bucket(assister)
                assist_bucket["assists"] += 1

        # Free-throw attempts/makes and points should also be shooter-only.
        is_ft_event = (play_type in PLAY_TYPES_FT) or to_bool(row.get("free_throw")) or desc_is_ft
        if shooter and is_ft_event:
            bucket = get_bucket(shooter)
            bucket["fta"] += 1
            if made:
                bucket["ftm"] += 1
                bucket["points"] += 1

    out: dict[tuple[str, str, str], PlayerGameStats] = {}
    for key, v in stats.items():
        games = len(games_by_player.get(key, set()))
        if games <= 0:
            games = 1
        out[key] = PlayerGameStats(
            player=v["player"],
            team=v["team"],
            season=v["season"],
            games=games,
            points=v["points"],
            rebounds=v["rebounds"],
            assists=v["assists"],
            steals=v["steals"],
            blocks=v["blocks"],
            fgm=v["fgm"],
            fga=v["fga"],
            tpm=v["tpm"],
            tpa=v["tpa"],
            ftm=v["ftm"],
            fta=v["fta"],
        )
    return out, games_by_player


def percentile(value: float, cohort: list[float]) -> float:
    if not cohort:
        return 0.0
    less = sum(1 for x in cohort if x < value)
    equal = sum(1 for x in cohort if x == value)
    return 100.0 * (less + 0.5 * equal) / len(cohort)


def percentile_safe(value: float | None, cohort: list[float]) -> float | None:
    if value is None:
        return None
    vals = [x for x in cohort if x is not None and math.isfinite(x)]
    if not vals:
        return None
    return percentile(value, vals)


def collect_shots(
    plays_rows: list[dict[str, str]],
    player: str,
    team: str,
    season: str,
    season_hint: str = "",
) -> list[dict[str, Any]]:
    def plausible_coord(row: dict[str, str], x: float, y: float, shot_range: str) -> bool:
        # Filter obvious coordinate outliers in older feeds that project as fake half-court shots.
        # Keep true long-heave descriptions.
        desc = (row.get("description", "") or "").lower()
        if any(k in desc for k in ["half court", "half-court", "heave", "desperation"]):
            return True
        xft = float(x) / 10.0
        yft = float(y) / 10.0
        d1 = math.hypot(xft - 4.0, yft - 25.0)
        d2 = math.hypot(xft - 90.0, yft - 25.0)
        d = min(d1, d2)
        # Normal NCAA attempts are well inside this; outliers above this are usually bad coords.
        if shot_range == "three_pointer":
            return d <= 35.0
        return d <= 32.0

    out: list[dict[str, Any]] = []
    np, nt, ns = norm_text(player), norm_text(team), norm_text(season)
    for row in plays_rows:
        rp = norm_text(row.get("shotInfo.shooter.name", "") or row.get("shooter", "") or row.get("participants[0].name", ""))
        rt = norm_text(_team_from_row(row))
        rs = norm_text(_season_from_row(row, season_hint))
        if (rp, rt, rs) != (np, nt, ns):
            continue
        x, y = _shot_loc_from_row(row)
        if x is None or y is None:
            continue
        made = _shot_made_from_row(row)
        shot_range = _shot_range_from_row(row)
        if not plausible_coord(row, float(x), float(y), shot_range):
            continue
        out.append(
            {
                "x": x,
                "y": y,
                "made": made,
                "range": shot_range,
            }
        )
    return out


def _fold_half_court(full_x: float, full_y: float) -> tuple[float, float]:
    court_len = 940.0
    court_wid = 500.0
    half_len = court_len / 2.0
    x2 = max(0.0, min(court_len, full_x))
    y2 = max(0.0, min(court_wid, full_y))
    x_half = min(x2, court_len - x2)
    return x_half, y2


def _shot_zone(shot: dict[str, Any]) -> str:
    rng = shot.get("range", "")
    xh, yy = _fold_half_court(float(shot["x"]), float(shot["y"]))
    if rng == "rim":
        return "Rim"
    if rng == "jumper":
        if xh <= 125 and 160 <= yy <= 340:
            return "Paint"
        return "Midrange"
    if rng == "three_pointer":
        if yy <= 65 or yy >= 435:
            return "Corner 3"
        if yy < 200:
            return "Wing 3 Left"
        if yy > 300:
            return "Wing 3 Right"
        return "Top 3"
    return "Other"


def _zone_pct_map(shots: list[dict[str, Any]]) -> dict[str, tuple[int, int, float]]:
    counts: dict[str, list[int]] = {}
    for s in shots:
        z = _shot_zone(s)
        made, att = counts.setdefault(z, [0, 0])
        counts[z] = [made + (1 if s.get("made") else 0), att + 1]
    out: dict[str, tuple[int, int, float]] = {}
    for z, (m, a) in counts.items():
        out[z] = (m, a, (100.0 * m / a) if a else 0.0)
    return out


def _color_for_delta(delta: float) -> str:
    # Requested mapping: red = good, blue = bad.
    m = min(1.0, abs(delta) / 15.0)
    if delta >= 0:
        r = int(130 + 120 * m)
        g = int(40 + 30 * (1.0 - m))
        b = int(50 + 40 * (1.0 - m))
    else:
        r = int(35 + 25 * (1.0 - m))
        g = int(80 + 50 * (1.0 - m))
        b = int(140 + 110 * m)
    return f"rgb({r},{g},{b})"


def shot_svg(
    shots: list[dict[str, Any]],
    season_shots: list[dict[str, Any]],
    width: int = 460,
    height: int = 300,
) -> str:
    # NCAA half-court geometry in CBBD's 94x50-ft coordinate scale (10 units per foot).
    court_len = 940.0
    court_wid = 500.0
    half_len = court_len / 2.0
    margin = 20.0

    def map_x(full_y: float) -> float:
        y2 = max(0.0, min(court_wid, full_y))
        return margin + y2 * (width - 2 * margin) / court_wid

    def map_y(full_x: float) -> float:
        x2 = max(0.0, min(court_len, full_x))
        x_half = min(x2, court_len - x2)
        return margin + x_half * (height - 2 * margin) / half_len

    def pt(full_x: float, full_y: float) -> tuple[float, float]:
        return map_x(full_y), map_y(full_x)

    misses: list[str] = []
    makes: list[str] = []
    for s in shots:
        x = float(s["x"])
        y = float(s["y"])
        made = bool(s.get("made"))
        fill = "#22c55e" if made else "#ef4444"
        dot = f'<circle cx="{map_x(y):.1f}" cy="{map_y(x):.1f}" r="4.2" fill="{fill}" fill-opacity="0.8" />'
        if made:
            makes.append(dot)
        else:
            misses.append(dot)

    # Core court anchors (units where 10 = 1 foot).
    hoop_x = 40.0
    hoop_y = 250.0
    lane_x = 190.0
    lane_y_min = 190.0
    lane_y_max = 310.0
    ft_r = 60.0
    restricted_r = 40.0
    three_r = 221.46  # 22' 1.75"
    corner_y_min = 30.0
    corner_y_max = 470.0
    three_join_x = hoop_x + max(0.0, (three_r * three_r - (hoop_y - corner_y_min) ** 2) ** 0.5)

    ox1, oy1 = pt(0.0, 0.0)
    ox2, oy2 = pt(half_len, court_wid)
    lx1, ly1 = pt(0.0, lane_y_min)
    lx2, ly2 = pt(lane_x, lane_y_max)
    hx, hy = pt(hoop_x, hoop_y)
    bb1x, bb1y = pt(40.0 - 7.5, 220.0)
    bb2x, bb2y = pt(40.0 - 7.5, 280.0)
    ftcx, ftcy = pt(lane_x, hoop_y)
    c1x1, c1y1 = pt(0.0, corner_y_min)
    c1x2, c1y2 = pt(three_join_x, corner_y_min)
    c2x1, c2y1 = pt(0.0, corner_y_max)
    c2x2, c2y2 = pt(three_join_x, corner_y_max)
    arc_points: list[str] = []
    for i in range(81):
        yy = corner_y_min + (corner_y_max - corner_y_min) * (i / 80.0)
        dx = math.sqrt(max(0.0, three_r * three_r - (yy - hoop_y) ** 2))
        xx = hoop_x + dx
        px, py = pt(xx, yy)
        arc_points.append(f"{px:.1f},{py:.1f}")
    three_arc_polyline = " ".join(arc_points)

    px_per_unit_y = (width - 2 * margin) / court_wid
    px_per_unit_x = (height - 2 * margin) / half_len
    rr_x = restricted_r * px_per_unit_y
    rr_y = restricted_r * px_per_unit_x
    ft_rx = ft_r * px_per_unit_y
    ft_ry = ft_r * px_per_unit_x

    court = f"""
<rect x="{ox1:.1f}" y="{oy1:.1f}" width="{ox2-ox1:.1f}" height="{oy2-oy1:.1f}" fill="#000000" stroke="#ffffff" stroke-width="2"/>
<rect x="{lx1:.1f}" y="{ly1:.1f}" width="{lx2-lx1:.1f}" height="{ly2-ly1:.1f}" fill="none" stroke="#ffffff" stroke-width="2"/>
<line x1="{bb1x:.1f}" y1="{bb1y:.1f}" x2="{bb2x:.1f}" y2="{bb2y:.1f}" stroke="#ffffff" stroke-width="2"/>
<ellipse cx="{hx:.1f}" cy="{hy:.1f}" rx="6.0" ry="6.0" fill="none" stroke="#ffffff" stroke-width="2"/>
<path d="M {map_x(hoop_y-restricted_r):.1f} {hy:.1f} A {rr_x:.1f} {rr_y:.1f} 0 0 1 {map_x(hoop_y+restricted_r):.1f} {hy:.1f}" fill="none" stroke="#ffffff" stroke-width="2"/>
<ellipse cx="{ftcx:.1f}" cy="{ftcy:.1f}" rx="{ft_rx:.1f}" ry="{ft_ry:.1f}" fill="none" stroke="#ffffff" stroke-width="2"/>
<line x1="{c1x1:.1f}" y1="{c1y1:.1f}" x2="{c1x2:.1f}" y2="{c1y2:.1f}" stroke="#ffffff" stroke-width="2"/>
<line x1="{c2x1:.1f}" y1="{c2y1:.1f}" x2="{c2x2:.1f}" y2="{c2y2:.1f}" stroke="#ffffff" stroke-width="2"/>
<polyline points="{three_arc_polyline}" fill="none" stroke="#ffffff" stroke-width="2"/>
<line x1="{ox1:.1f}" y1="{oy2:.1f}" x2="{ox2:.1f}" y2="{oy2:.1f}" stroke="#ffffff" stroke-width="2"/>
"""
    return f"""
<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  {court}
  {''.join(misses)}
  {''.join(makes)}
</svg>
"""


def fmt(v: float, digits: int = 1) -> str:
    return f"{v:.{digits}f}"


def fmt_percent_source_value(v: float) -> float:
    # Bart percent fields can be on 0..1 or 0..100 scales.
    return v * 100.0 if 0.0 <= v <= 1.0 else v


def parse_date_maybe(v: str) -> datetime | None:
    s = (v or "").strip()
    if not s:
        return None
    for fmt_s in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt_s)
        except ValueError:
            continue
    return None


def age_on_june25_for_season(dob_raw: str, season: str) -> str:
    dob = parse_date_maybe(dob_raw)
    if dob is None:
        return "N/A"
    m = re.search(r"(20\d{2})", str(season))
    if not m:
        return "N/A"
    ref_year = int(m.group(1))
    ref = datetime(ref_year, 6, 25)
    years = (ref - dob).days / 365.2425
    if years <= 0:
        return "N/A"
    return f"{years:.1f}"


def adv_bar(metric: str, value: float | None, pct: float | None, digits: int = 2) -> str:
    if value is None:
        return ""
    pct_num = 0.0 if pct is None else pct
    pct_lbl = "-" if pct is None else f"{pct:.0f}"

    def _lerp(a: float, b: float, t: float) -> int:
        return int(round(a + (b - a) * t))

    def _pct_color(p: float | None) -> str:
        if p is None:
            return "#6b7280"
        x = max(0.0, min(100.0, float(p)))
        if x <= 50.0:
            t = x / 50.0
            r = _lerp(239, 255, t)
            g = _lerp(68, 255, t)
            b = _lerp(68, 255, t)
        else:
            t = (x - 50.0) / 50.0
            r = _lerp(255, 34, t)
            g = _lerp(255, 197, t)
            b = _lerp(255, 94, t)
        return f"rgb({r}, {g}, {b})"

    fill_color = _pct_color(pct)
    return f"""
<div class="metric-row">
  <div class="metric-label">{html.escape(metric)}</div>
  <div class="metric-val">{value:.{digits}f}</div>
  <div class="bar-wrap"><div class="bar-fill" style="width:{pct_num:.1f}%;background:{fill_color};"></div></div>
  <div class="metric-pct">{pct_lbl}</div>
</div>
"""


def lookup_row(rows: list[dict[str, str]], col_player: str, col_team: str, col_year: str, player: str, team: str, season: str) -> dict[str, str] | None:
    k = key_player_team_season(player, team, season)
    for row in rows:
        rk = key_player_team_season(row.get(col_player, ""), row.get(col_team, ""), row.get(col_year, ""))
        if rk == k:
            return row
    return None


def collect_numeric_column(rows: list[dict[str, str]], col: str) -> list[float]:
    out: list[float] = []
    for r in rows:
        v = to_float(r.get(col))
        if v is not None and math.isfinite(v):
            out.append(v)
    return out


def normalize_pct_maybe(v: float) -> float:
    # Some style percentile fields are on 0..1 scale.
    return v * 100.0 if 0.0 <= v <= 1.0 else v


def format_height(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return "N/A"
    if "'" in s or '"' in s:
        return s
    if re.fullmatch(r"\d+\s*-\s*\d+", s):
        a, b = re.split(r"\s*-\s*", s)
        return f"{int(a)}'{int(b)}\""
    v = to_float(s)
    if v is None:
        return s
    inches = int(round(v))
    if 48 <= inches <= 96:
        return f"{inches // 12}'{inches % 12}\""
    return s


def ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def parse_rsci_rank(raw: str) -> int | None:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.search(r"(?i)^t?\s*(\d+)$", s)
    if not m:
        return None
    try:
        v = int(m.group(1))
        return v if v > 0 else None
    except Exception:
        return None


def load_rsci_rankings(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    out: dict[str, int] = {}
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            rank_raw = (row[0] or "").strip()
            player_raw = (row[1] or "").strip()
            if not rank_raw or not player_raw:
                continue
            if norm_text(rank_raw) in {"rank", "rsci"} or norm_text(player_raw) == "player":
                continue
            if "totals" in norm_text(player_raw) or "summary" in norm_text(player_raw):
                continue
            rank = parse_rsci_rank(rank_raw)
            if rank is None:
                continue
            key = norm_player_name(player_raw)
            if not key:
                continue
            prev = out.get(key)
            if prev is None or rank < prev:
                out[key] = rank
    return out


def _name_key_compact(v: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", norm_player_name(v))


def _name_tokens(v: str) -> list[str]:
    s = norm_player_name(v)
    return [t for t in re.split(r"[^a-z0-9]+", s) if t]


def find_rsci_rank(player_name: str, rsci_map: dict[str, int]) -> int | None:
    if not rsci_map:
        return None
    key = norm_player_name(player_name)
    if not key:
        return None
    if key in rsci_map:
        return rsci_map[key]

    compact_key = _name_key_compact(player_name)
    if compact_key:
        by_compact: dict[str, tuple[str, int]] = {}
        for cand, rank in rsci_map.items():
            ckey = _name_key_compact(cand)
            if not ckey:
                continue
            prev = by_compact.get(ckey)
            if prev is None or rank < prev[1]:
                by_compact[ckey] = (cand, rank)
        hit = by_compact.get(compact_key)
        if hit is not None:
            return hit[1]

    # Strong token/last-name fallback (handles initials, punctuation, suffix variance).
    target_tokens = _name_tokens(player_name)
    if not target_tokens:
        return None
    target_last = target_tokens[-1]
    target_first_initial = target_tokens[0][0] if target_tokens[0] else ""
    candidates = list(rsci_map.keys())
    narrowed: list[str] = []
    for cand in candidates:
        ct = _name_tokens(cand)
        if not ct:
            continue
        last_ok = (ct[-1] == target_last)
        init_ok = bool(target_first_initial and ct[0].startswith(target_first_initial))
        overlap = len(set(target_tokens) & set(ct))
        if (last_ok and init_ok) or overlap >= max(1, min(2, len(target_tokens) - 1)):
            narrowed.append(cand)
    pool = narrowed if narrowed else candidates

    scored = sorted(
        ((difflib.SequenceMatcher(None, key, cand).ratio(), cand) for cand in pool),
        key=lambda x: x[0],
        reverse=True,
    )
    if not scored:
        return None
    best_score, best_name = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0
    # Relax threshold to avoid false "Unranked" when obvious match exists.
    if best_score >= 0.82 and (best_score - second_score) >= 0.005:
        return rsci_map.get(best_name)
    return None


DRAFT_BUCKETS: list[tuple[str, int | None, int | None]] = [
    ("Number One Pick", 1, 1),
    ("Top 5", 2, 5),
    ("Top 10", 6, 10),
    ("Top 20", 11, 20),
    ("Outside Top 20", 21, 60),
    ("Undrafted", None, None),
]


def parse_pick_number(raw: str) -> int | None:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    try:
        v = int(m.group(1))
        return v if 1 <= v <= 60 else None
    except Exception:
        return None


def draft_bucket_index_for_pick(pick: int | None) -> int:
    if pick is None:
        return len(DRAFT_BUCKETS) - 1
    if pick == 1:
        return 0
    if 2 <= pick <= 5:
        return 1
    if 6 <= pick <= 10:
        return 2
    if 11 <= pick <= 20:
        return 3
    if 21 <= pick <= 60:
        return 4
    return len(DRAFT_BUCKETS) - 1


def rsci_rank_to_score(rank: int | None) -> float | None:
    if rank is None:
        return 0.0
    # 1 -> ~100, 100 -> ~1
    return max(0.0, min(100.0, 101.0 - float(rank)))


def load_wnba_draft_lookup(path: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 3:
                continue
            pick = parse_pick_number(row[0] if len(row) > 0 else "")
            name = row[2] if len(row) > 2 else ""
            if pick is None:
                continue
            key = norm_player_name(name)
            if not key:
                continue
            prev = out.get(key)
            if prev is None or pick < prev:
                out[key] = pick
    return out


def build_advanced_html(
    target: PlayerGameStats,
    lebron_rows: list[dict[str, str]],
    rim_rows: list[dict[str, str]],
    style_rows: list[dict[str, str]],
) -> str:
    blocks: list[str] = []

    # LEBRON block
    if lebron_rows:
        lr = lookup_row(lebron_rows, "Player", "Team", "Year", target.player, target.team, target.season)
        if lr:
            def lval(col: str) -> float | None:
                return to_float(lr.get(col))

            metrics = [
                ("LEBRON", lval("LEBRON"), collect_numeric_column(lebron_rows, "LEBRON"), 2),
                ("O-LEBRON", lval("O-LEBRON"), collect_numeric_column(lebron_rows, "O-LEBRON"), 2),
                ("D-LEBRON", lval("D-LEBRON"), collect_numeric_column(lebron_rows, "D-LEBRON"), 2),
                ("BPM", lval("BPM"), collect_numeric_column(lebron_rows, "BPM"), 1),
                ("TS", lval("TS"), collect_numeric_column(lebron_rows, "TS"), 1),
                ("Usg", lval("Usg"), collect_numeric_column(lebron_rows, "Usg"), 1),
                ("PRPG!", lval("PRPG!"), collect_numeric_column(lebron_rows, "PRPG!"), 1),
            ]
            body = ""
            for label, v, cohort, digits in metrics:
                body += adv_bar(label, v, percentile_safe(v, cohort), digits)
            if body:
                blocks.append(f'<div class="panel"><h3>Advanced: LEBRON Model</h3>{body}</div>')

    # Rimfluence block
    if rim_rows:
        rr = lookup_row(rim_rows, "player_name", "team_name", "year", target.player, target.team, target.season)
        if rr:
            def rval(col: str) -> float | None:
                return to_float(rr.get(col))

            metrics = [
                ("Rimfluence", rval("Rimfluence"), collect_numeric_column(rim_rows, "Rimfluence"), 2),
                ("Rimfluence z", rval("Rimfluence_z"), collect_numeric_column(rim_rows, "Rimfluence_z"), 2),
                ("Off Pts/100 Poss", rval("off_pts_per100poss"), collect_numeric_column(rim_rows, "off_pts_per100poss"), 1),
                ("Def Pts Saved/100", rval("def_pts_saved_per100poss"), collect_numeric_column(rim_rows, "def_pts_saved_per100poss"), 1),
            ]
            body = ""
            for label, v, cohort, digits in metrics:
                body += adv_bar(label, v, percentile_safe(v, cohort), digits)
            if body:
                blocks.append(f'<div class="panel"><h3>Advanced: Rimfluence</h3>{body}</div>')

    # Style + play type block from master/style sheet
    if style_rows:
        sr = lookup_row(style_rows, "Player", "Team", "Year", target.player, target.team, target.season)
        if sr:
            def sval(col: str) -> float | None:
                return to_float(sr.get(col))

            style_specs = [
                ("Rim Attack PPP", "Rim Attack PPP", "pctile_off_style_rim_attack_pct"),
                ("Attack & Kick PPP", "Attack & Kick PPP", "pctile_off_style_attack_kick_pct"),
                ("Transition PPP", "Transition PPP", "transition_pct"),
                ("PNR Passer PPP", "PNR Passer PPP", "pctile_off_style_pnr_passer_pct"),
                ("PnR Roller PPP", "PnR Roller PPP", "pctile_off_style_big_cut_roll_pct"),
                ("Post Up PPP", "Post Up PPP", "pctile_off_style_post_up_pct"),
            ]
            body = ""
            for label, raw_col, pct_col in style_specs:
                raw_v = sval(raw_col)
                pct_v = sval(pct_col)
                if pct_v is not None:
                    pct_v = normalize_pct_maybe(pct_v)
                # Fallback percentile if style percentile field missing.
                if pct_v is None and raw_v is not None:
                    pct_v = percentile_safe(raw_v, collect_numeric_column(style_rows, raw_col))
                body += adv_bar(label, raw_v, pct_v, 2)
            if body:
                blocks.append(f'<div class="panel"><h3>Advanced: Style + Play Types</h3>{body}</div>')

    if not blocks:
        return ""

    return f"""
      <div class="row" style="margin-top:14px;">
        {''.join(blocks[:2])}
      </div>
      {'<div class="row" style="margin-top:14px;">' + ''.join(blocks[2:4]) + '</div>' if len(blocks) > 2 else ''}
"""


def bt_get(row: dict[str, str], aliases: list[str]) -> str:
    alias_norm = {norm_text(a) for a in aliases}
    for k, v in row.items():
        if norm_text(k) in alias_norm:
            return v
    return ""


def bt_num(row: dict[str, str], aliases: list[str]) -> float | None:
    return to_float(bt_get(row, aliases))


def bt_num_priority(row: dict[str, str], aliases: list[str]) -> float | None:
    # Respect alias order (first alias has highest priority), unlike bt_get/bt_num.
    norm_map = {norm_text(k): v for k, v in row.items()}
    for a in aliases:
        v = norm_map.get(norm_text(a))
        n = to_float(v)
        if n is not None and math.isfinite(n):
            return n
    return None


def bt_find_target_row(rows: list[dict[str, str]], target: PlayerGameStats) -> dict[str, str] | None:
    np = norm_text(target.player)
    nt = norm_team(target.team)
    ny = norm_text(target.season)

    by_name_year = []
    for r in rows:
        rp = norm_text(bt_get(r, ["player_name"]))
        rt = norm_team(bt_get(r, ["team"]))
        ry = norm_text(bt_get(r, ["year"]))
        if rp == np and ry == ny:
            by_name_year.append(r)
            if rt == nt:
                return r
    return by_name_year[0] if by_name_year else None


def bt_cohort_for_year(rows: list[dict[str, str]], season: str) -> list[dict[str, str]]:
    ys = norm_text(season)
    cohort = [r for r in rows if norm_text(bt_get(r, ["year"])) == ys]
    return cohort if cohort else rows


def bt_row_position_bucket(row: dict[str, str]) -> str | None:
    # Prefer explicit enriched roster position when available.
    rp = norm_text(bt_get(row, ["roster.pos"]))
    if rp in {"g", "f", "c"}:
        return rp.upper()

    raw = " ".join(
        [
            str(bt_get(row, ["roster.pos"])),
            str(bt_get(row, ["role"])),
            str(bt_get(row, ["posClass"])),
        ]
    ).upper()
    if not raw.strip():
        return None

    # Tokenize common position labels/codes.
    tokens = [t for t in re.split(r"[^A-Z0-9]+", raw) if t]
    for t in tokens:
        if t in {"PG", "SG", "CG", "WG", "G", "GUARD"}:
            return "G"
        if t in {"SF", "PF", "WF", "F", "FORWARD"}:
            return "F"
        if t in {"C", "CENTER"}:
            return "C"

    # Fallbacks for compact codes like "SPG", "PFC".
    compact = re.sub(r"[^A-Z0-9]+", "", raw)
    if "PG" in compact or "SG" in compact or "CG" in compact or compact.endswith("G"):
        return "G"
    if "SF" in compact or "PF" in compact or "WF" in compact or compact.endswith("F"):
        return "F"
    if "C" in compact:
        return "C"
    return None


def bt_position_filtered_cohort(
    cohort_rows: list[dict[str, str]],
    target_row: dict[str, str],
) -> list[dict[str, str]]:
    target_bucket = bt_row_position_bucket(target_row)
    if not target_bucket:
        return cohort_rows
    filtered = [r for r in cohort_rows if bt_row_position_bucket(r) == target_bucket]
    return filtered if filtered else cohort_rows


def pbp_find_target_row(rows: list[dict[str, str]], target: PlayerGameStats) -> dict[str, str] | None:
    np = norm_text(target.player)
    nt = norm_team(target.team)
    ny = norm_text(target.season)
    for r in rows:
        rp = norm_text(r.get("player", ""))
        rt = norm_team(r.get("team", ""))
        ry = norm_text(r.get("season", ""))
        if rp == np and rt == nt and ry == ny:
            return r
    return None


def pbp_cohort_for_year(rows: list[dict[str, str]], season: str) -> list[dict[str, str]]:
    ys = norm_text(season)
    cohort = [r for r in rows if norm_text(r.get("season", "")) == ys]
    return cohort if cohort else rows


def pbp_metric_percentile(
    target_row: dict[str, str] | None,
    cohort_rows: list[dict[str, str]],
    key: str,
) -> tuple[float | None, float | None]:
    if not target_row:
        return None, None

    def pbp_metric_value(row: dict[str, str], metric_key: str) -> float | None:
        if metric_key == "unassisted_points_100":
            r = to_float(row.get("unassisted_rim_makes_100", ""))
            m = to_float(row.get("unassisted_mid_makes_100", ""))
            t = to_float(row.get("unassisted_3pm_100", ""))
            if r is None or m is None or t is None:
                return None
            return (2.0 * r) + (2.0 * m) + (3.0 * t)
        return to_float(row.get(metric_key, ""))

    val = pbp_metric_value(target_row, key)
    vals: list[float] = []
    for r in cohort_rows:
        v = pbp_metric_value(r, key)
        if v is not None and math.isfinite(v):
            vals.append(v)
    if val is None or not vals:
        return val, None
    return val, percentile(val, vals)


def load_bt_playerstat_rows_from_source(source: str) -> list[dict[str, Any]]:
    if not source:
        return []
    if source.startswith("http://") or source.startswith("https://"):
        req = Request(
            source,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,text/plain,*/*",
            },
            method="GET",
        )
        with urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    else:
        raw = Path(source).read_text(encoding="utf-8")

    arr = json.loads(raw)
    out: list[dict[str, Any]] = []
    for r in arr:
        if not isinstance(r, list) or len(r) < 15:
            continue
        out.append(
            {
                "pid": r[0],
                "player": str(r[1]),
                "team": str(r[2]),
                "rim_made": float(r[3]),
                "rim_miss": float(r[4]),
                "rim_assisted": float(r[5]),
                "mid_made": float(r[6]),
                "mid_miss": float(r[7]),
                "mid_assisted": float(r[8]),
                "three_made": float(r[9]),
                "three_miss": float(r[10]),
                "three_assisted": float(r[11]),
                "dunks_made": float(r[12]),
                "dunks_miss": float(r[13]),
                "dunks_assisted": float(r[14]),
            }
        )
    return out


def bt_playerstat_metrics_from_row(ps_row: dict[str, Any], possessions: float | None) -> dict[str, float] | None:
    if possessions is None or possessions <= 0:
        return None
    un_rim = max(0.0, float(ps_row.get("rim_made", 0.0)) - float(ps_row.get("rim_assisted", 0.0)))
    un_mid = max(0.0, float(ps_row.get("mid_made", 0.0)) - float(ps_row.get("mid_assisted", 0.0)))
    un_3 = max(0.0, float(ps_row.get("three_made", 0.0)) - float(ps_row.get("three_assisted", 0.0)))
    un_dunks = max(0.0, float(ps_row.get("dunks_made", 0.0)) - float(ps_row.get("dunks_assisted", 0.0)))
    mul = 100.0 / float(possessions)
    return {
        "unassisted_dunks_100": un_dunks * mul,
        "unassisted_rim_makes_100": un_rim * mul,
        "unassisted_mid_makes_100": un_mid * mul,
        "unassisted_3pm_100": un_3 * mul,
        "unassisted_points_100": ((2.0 * un_rim) + (2.0 * un_mid) + (3.0 * un_3)) * mul,
    }


def find_bt_playerstat_row(
    rows: list[dict[str, Any]],
    player: str,
    team: str,
) -> dict[str, Any] | None:
    np = norm_player_name(player)
    nt = norm_team(team)
    exact = [r for r in rows if norm_player_name(r.get("player", "")) == np and norm_team(r.get("team", "")) == nt]
    if exact:
        return exact[0]
    by_name = [r for r in rows if norm_player_name(r.get("player", "")) == np]
    if not by_name:
        return None
    if len(by_name) == 1:
        return by_name[0]
    scored = sorted(
        ((difflib.SequenceMatcher(None, nt, norm_team(r.get("team", ""))).ratio(), r) for r in by_name),
        key=lambda x: x[0],
        reverse=True,
    )
    return scored[0][1] if scored and scored[0][0] >= 0.55 else by_name[0]


def build_pbp_off_possessions_map(pbp_rows: list[dict[str, str]]) -> dict[tuple[str, str, str], float]:
    out: dict[tuple[str, str, str], float] = defaultdict(float)
    for r in pbp_rows:
        p = norm_player_name(r.get("player", ""))
        t = norm_team(r.get("team", ""))
        y = norm_season(r.get("season", ""))
        poss = to_float(r.get("off_possessions", ""))
        if not p or not t or not y or poss is None or not math.isfinite(poss):
            continue
        out[(p, t, y)] = float(poss)
    return dict(out)


def adjust_possessions_to_bart_games(
    pbp_off_possessions: float | None,
    pbp_games: float | None,
    bart_games: float | None,
) -> float | None:
    if pbp_off_possessions is None or not math.isfinite(pbp_off_possessions) or pbp_off_possessions <= 0:
        return None
    if pbp_games is None or bart_games is None or pbp_games <= 0 or bart_games <= 0:
        return pbp_off_possessions
    return (float(pbp_off_possessions) / float(pbp_games)) * float(bart_games)


def bt_metric_value(row: dict[str, str], key: str) -> float | None:
    def bt_possessions_estimate(r: dict[str, str]) -> float | None:
        # Only use enrichedPlayers possessions for all possession-normalized BT stats.
        poss = bt_num(r, ["off_team_poss.value"])
        if poss is not None and poss > 0:
            return float(poss)
        return None

    if key == "net_rating":
        ortg = bt_num(row, ["ORtg"])
        drtg = bt_num(row, ["drtg", "DRtg", " drtg"])
        if ortg is None or drtg is None:
            return None
        return ortg - drtg
    if key == "rapm":
        # Interpreted as net RAPM: offense minus defense.
        off_rapm = bt_num(row, ["off_adj_rapm.value"])
        def_rapm = bt_num(row, ["def_adj_rapm.value"])
        if off_rapm is None or def_rapm is None:
            return None
        return float(off_rapm) - float(def_rapm)
    if key == "onoff_net_rating":
        on_off = bt_num(row, ["on.off_adj_ppp.value"])
        on_def = bt_num(row, ["on.def_adj_ppp.value"])
        off_off = bt_num(row, ["off.off_adj_ppp.value"])
        off_def = bt_num(row, ["off.def_adj_ppp.value"])
        if on_off is None or on_def is None or off_off is None or off_def is None:
            return None
        return (float(on_off) - float(on_def)) - (float(off_off) - float(off_def))
    if key == "net_pts":
        v = bt_num(row, ["net_pts.value"])
        if v is not None:
            return v
        o = bt_num(row, ["net_pts.o"])
        d = bt_num(row, ["net_pts.d"])
        if o is None or d is None:
            return None
        return float(o) + float(d)
    if key == "rim_pct":
        return bt_num(row, ["rimmade/(rimmade+rimmiss)", " rimmade/(rimmade+rimmiss)"])
    if key == "mid_pct":
        return bt_num(row, ["midmade/(midmade+midmiss)", " midmade/(midmade+midmiss)"])
    if key == "fta100_bt":
        fta = bt_num(row, ["FTA"])
        poss = bt_possessions_estimate(row)
        if fta is None or poss is None or poss <= 0:
            return None
        return 100.0 * float(fta) / float(poss)
    if key == "rim_att_100_bt":
        rim_att = bt_num(row, ["rimmade+rimmiss", " rimmade+rimmiss", "rimatt", " rimatt"])
        poss = bt_possessions_estimate(row)
        if rim_att is None or poss is None or poss <= 0:
            return None
        return 100.0 * float(rim_att) / float(poss)
    if key == "dunks_100_bt":
        dunks_made = bt_num(row, ["dunksmade", " dunksmade"])
        poss = bt_possessions_estimate(row)
        if dunks_made is None or poss is None or poss <= 0:
            return None
        return 100.0 * float(dunks_made) / float(poss)
    if key == "bpm":
        # Use game-BPM columns from Bart exports per user preference.
        return bt_num_priority(row, ["gbpm", "GBPM", " gbpm", "bpm", "BPM", " bpm"])
    if key == "obpm":
        return bt_num(row, ["obpm", "OBPM", "Obpm", " obpm"])
    if key == "dbpm":
        # Use defensive game-BPM columns from Bart exports per user preference.
        return bt_num_priority(row, ["dgbpm", "DGBPM", " dgbpm", "dbpm", "DBPM", "Dbpm", " dbpm"])
    if key == "rim_assists_100_btposs":
        poss = bt_possessions_estimate(row)
        if poss is None or poss <= 0:
            return None

        ast_total = bt_num(row, ["AST_total", "ast_total", "assists", "AST", "ast"])
        gp = bt_num(row, ["GP", "gp"])
        if ast_total is not None and gp is not None and gp > 0:
            # In this pipeline AST/ast is typically per-game; convert to season total assists.
            ast_total = float(ast_total) * float(gp)
        if ast_total is None:
            return None

        rim_ast_pct = bt_num(row, ["off_ast_rim.value", "off_ast_rim.old_value", "off_ast_rim"])
        if rim_ast_pct is None:
            return None
        p = float(rim_ast_pct) / 100.0 if float(rim_ast_pct) > 1.0 else float(rim_ast_pct)
        p = max(0.0, min(1.0, p))
        rim_ast_total = float(ast_total) * p
        return 100.0 * rim_ast_total / float(poss)
    key_aliases = {
        "bpm": ["bpm", " bpm"],
        "obpm": ["obpm", " obpm"],
        "dbpm": ["dbpm", " dbpm"],
        "usg": ["usg"],
        "ts_per": ["TS_per"],
        "twop_per": ["twoP_per"],
        "dunksmade": ["dunksmade", " dunksmade"],
        "tp_per": ["TP_per"],
        "threepa100": ["3p/100?"],
        "ft_per": ["FT_per"],
        "ftr": ["ftr"],
        "ast_per": ["AST_per"],
        "to_per": ["TO_per"],
        "ast_tov": ["ast/tov", " ast/tov"],
        "stl_per": ["stl_per"],
        "blk_per": ["blk_per"],
        "orb_per": ["ORB_per"],
        "drb_per": ["DRB_per"],
        "possessions": ["possessions", " possessions"],
    }
    aliases = key_aliases.get(key, [key])
    return bt_num(row, aliases)


def bt_metric_percentile(
    target_row: dict[str, str],
    cohort_rows: list[dict[str, str]],
    key: str,
) -> tuple[float | None, float | None]:
    val = bt_metric_value(target_row, key)
    vals: list[float] = []
    for r in cohort_rows:
        v = bt_metric_value(r, key)
        if v is not None and math.isfinite(v):
            vals.append(v)
    if val is None or not vals:
        return val, None
    p = percentile(val, vals)
    if key == "to_per":
        p = 100.0 - p
    return val, p


def bt_display_stl_pct(value: float | None) -> float | None:
    if value is None:
        return None
    # Some exports carry STL% as basis-points-like values.
    return (value * 0.01) if abs(value) >= 10.0 else value


def bt_display_blk_pct(value: float | None) -> float | None:
    if value is None:
        return None
    # Keep true 0..1 block rates as-is (e.g. 0.3 -> 0.3, not 30.0).
    if -1.0 <= value <= 1.0:
        return value
    # Some exports carry BLK% as basis-points-like values.
    if abs(value) >= 10.0:
        return value * 0.01
    return value


def bt_row_html(
    label: str,
    value: float | None,
    pct: float | None,
    is_percent: bool = False,
    digits: int = 2,
    scale: float = 1.0,
    truncate: bool = False,
) -> str:
    if value is None:
        return ""
    shown = fmt_percent_source_value(value) if is_percent else value
    shown = shown * scale
    if truncate:
        factor = 10 ** digits
        shown = math.trunc(shown * factor) / factor
    else:
        shown = round(shown, digits)
    return adv_bar(label, shown, pct, digits=digits)


def build_bpm_trend_svg(target: PlayerGameStats, adv_rows: list[dict[str, str]]) -> str:
    if not adv_rows:
        return '<div class="shot-meta">No per-game BPM file loaded.</div>'
    np = norm_player_name(target.player)
    nt = norm_text(target.team)
    ys = norm_season(target.season)

    rows_py: list[dict[str, str]] = []
    for r in adv_rows:
        if norm_player_name(r.get("pp", "")) != np:
            continue
        if norm_season(r.get("year", "")) != ys:
            continue
        rows_py.append(r)

    if not rows_py:
        return '<div class="shot-meta">No per-game BPM rows found for this player/season.</div>'

    # Prefer exact team match. If missing, fall back to best fuzzy team match for this player-season.
    rows_team = [r for r in rows_py if norm_text(r.get("tt", "")) == nt]
    if not rows_team:
        team_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows_py:
            team_groups[norm_text(r.get("tt", ""))].append(r)
        if len(team_groups) == 1:
            rows_team = next(iter(team_groups.values()))
        else:
            scored = sorted(
                (
                    (difflib.SequenceMatcher(None, nt, t).ratio(), rows)
                    for t, rows in team_groups.items()
                    if t
                ),
                key=lambda x: x[0],
                reverse=True,
            )
            if scored and scored[0][0] >= 0.55:
                rows_team = scored[0][1]
            else:
                rows_team = []

    points_raw: list[tuple[int, str, float]] = []
    for r in rows_team:
        nd = (r.get("numdate", "") or "").strip()
        bpm = to_float(r.get("bpm", ""))
        if not nd or bpm is None:
            continue
        try:
            ndi = int(nd)
        except ValueError:
            continue
        points_raw.append((ndi, r.get("datetext", ""), float(bpm)))

    points_raw.sort(key=lambda x: x[0])
    if len(points_raw) < 2:
        return '<div class="shot-meta">Not enough game-level BPM points for chart.</div>'

    w, h = 330, 130
    ml, mr, mt, mb = 38, 10, 10, 24
    xs = [i for i in range(len(points_raw))]
    ys_v = [p[2] for p in points_raw]
    ymin, ymax = min(ys_v), max(ys_v)
    # Keep zero in frame so color/sign context is absolute, not only relative to this player's range.
    ymin = min(ymin, 0.0)
    ymax = max(ymax, 0.0)
    if abs(ymax - ymin) < 1e-9:
        ymax = ymin + 1.0
    pad = 0.08 * (ymax - ymin)
    ymin -= pad
    ymax += pad

    def xpx(i: int) -> float:
        span = max(1, len(points_raw) - 1)
        return ml + i * (w - ml - mr) / span

    def ypx(v: float) -> float:
        return mt + (ymax - v) * (h - mt - mb) / (ymax - ymin)

    # Segment colors by absolute sign relative to 0 BPM.
    segs: list[str] = []
    for i in range(1, len(points_raw)):
        v0 = points_raw[i - 1][2]
        v1 = points_raw[i][2]
        c = "#22c55e" if ((v0 + v1) / 2.0) >= 0 else "#ef4444"
        segs.append(
            f'<line x1="{xpx(i-1):.1f}" y1="{ypx(v0):.1f}" x2="{xpx(i):.1f}" y2="{ypx(v1):.1f}" '
            f'stroke="{c}" stroke-width="2" />'
        )

    # Show more small date labels across the axis.
    n = len(points_raw)
    tick_target = 7
    tick_idx = sorted({int(round(i * (n - 1) / (tick_target - 1))) for i in range(tick_target)})
    x_ticks = "".join(
        f'<text x="{xpx(i):.1f}" y="{h-8}" text-anchor="middle" font-size="9" fill="var(--muted)">{html.escape(points_raw[i][1] or str(points_raw[i][0]))}</text>'
        for i in tick_idx
    )
    y_vals = [ymin + k * (ymax - ymin) / 4.0 for k in range(5)]
    y_ticks = "".join(
        f'<text x="12" y="{ypx(v)+3:.1f}" text-anchor="start" font-size="9" fill="var(--muted)">{v:.1f}</text>'
        for v in y_vals
    )
    y_grid = "".join(
        f'<line x1="{ml}" y1="{ypx(v):.1f}" x2="{w-mr}" y2="{ypx(v):.1f}" stroke="var(--line)" stroke-width="0.8" stroke-dasharray="2 2"/>'
        for v in y_vals
    )
    dots = "".join(
        f'<circle cx="{xpx(i):.1f}" cy="{ypx(v):.1f}" r="2.4" fill="{"#22c55e" if v >= 0 else "#ef4444"}" />'
        for i, (_, _, v) in enumerate(points_raw)
    )
    return f"""
<div class="trend-wrap">
<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">
  <rect x="{ml}" y="{mt}" width="{w-ml-mr}" height="{h-mt-mb}" fill="var(--panel-alt)" stroke="var(--line)" stroke-width="1"/>
  {y_grid}
  <line x1="{ml}" y1="{ypx(0):.1f}" x2="{w-mr}" y2="{ypx(0):.1f}" stroke="var(--line)" stroke-width="1" stroke-dasharray="3 3"/>
  {''.join(segs)}
  {dots}
  {x_ticks}
  {y_ticks}
  <text x="{w/2:.1f}" y="{h-1}" text-anchor="middle" font-size="9" fill="var(--muted)">Date</text>
  <text x="6" y="{h/2:.1f}" text-anchor="start" font-size="9" fill="var(--muted)" transform="rotate(-90 6 {h/2:.1f})">BPM</text>
</svg>
</div>
"""


def grade_from_percentile(p: float | None) -> str:
    if p is None:
        return "--"
    if p >= 97:
        return "A+"
    if p >= 93:
        return "A"
    if p >= 90:
        return "A-"
    if p >= 87:
        return "B+"
    if p >= 83:
        return "B"
    if p >= 80:
        return "B-"
    if p >= 77:
        return "C+"
    if p >= 73:
        return "C"
    if p >= 70:
        return "C-"
    if p >= 67:
        return "D+"
    if p >= 63:
        return "D"
    if p >= 60:
        return "D-"
    return "F"


def bt_category_percentile(
    target_row: dict[str, str],
    cohort_rows: list[dict[str, str]],
    metric_keys: list[str],
) -> float | None:
    vals_by_key: dict[str, list[float]] = {}
    for key in metric_keys:
        vals: list[float] = []
        for r in cohort_rows:
            v = bt_metric_value(r, key)
            if v is not None and math.isfinite(v):
                vals.append(v)
        if vals:
            vals_by_key[key] = vals
    if not vals_by_key:
        return None

    def row_score(r: dict[str, str]) -> float | None:
        pcts: list[float] = []
        for key in metric_keys:
            vals = vals_by_key.get(key)
            if not vals:
                continue
            v = bt_metric_value(r, key)
            if v is None or not math.isfinite(v):
                continue
            p = percentile(v, vals)
            if key == "to_per":
                p = 100.0 - p
            pcts.append(p)
        if not pcts:
            return None
        return sum(pcts) / len(pcts)

    target_score = row_score(target_row)
    if target_score is None:
        return None
    cohort_scores = [s for s in (row_score(r) for r in cohort_rows) if s is not None]
    if not cohort_scores:
        return None
    return percentile(target_score, cohort_scores)


def build_grade_boxes_html(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> str:
    categories: list[tuple[str, list[str]]] = [
        ("Impact", ["bpm", "rapm", "net_pts", "onoff_net_rating"]),
        ("Scoring", ["usg", "ts_per", "twop_per", "dunksmade", "rim_pct", "mid_pct", "tp_per", "threepa100", "ft_per", "ftr"]),
        ("Playmaking", ["ast_per", "to_per", "ast_tov", "rim_assists_100_btposs"]),
        ("Defense", ["stl_per", "blk_per", "dbpm"]),
        ("Rebounding", ["orb_per", "drb_per"]),
    ]
    if not bt_rows:
        return "".join(
            f'<div class="grade-chip"><div class="grade-k">{html.escape(label)}</div><div class="grade-v">--</div></div>'
            for label, _ in categories
        )

    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return "".join(
            f'<div class="grade-chip"><div class="grade-k">{html.escape(label)}</div><div class="grade-v">--</div></div>'
            for label, _ in categories
        )
    cohort = bt_position_filtered_cohort(bt_cohort_for_year(bt_rows, target.season), target_row)
    chips = []
    for label, keys in categories:
        p = bt_category_percentile(target_row, cohort, keys)
        g = grade_from_percentile(p)
        chips.append(
            f'<div class="grade-chip"><div class="grade-k">{html.escape(label)}</div><div class="grade-v">{g}</div></div>'
        )
    return "".join(chips)


def build_bt_percentile_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    adv_rows: list[dict[str, str]],
    pbp_rows: list[dict[str, str]],
) -> str:
    if not bt_rows:
        return '<div class="panel" style="margin-top:14px;"><h3>Advanced Percentiles</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'

    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return '<div class="panel" style="margin-top:14px;"><h3>Advanced Percentiles</h3><div class="shot-meta">No matching Bart Torvik row found for this player/team/season.</div></div>'

    cohort = bt_position_filtered_cohort(bt_cohort_for_year(bt_rows, target.season), target_row)
    pbp_target = pbp_find_target_row(pbp_rows, target) if pbp_rows else None
    pbp_cohort = pbp_cohort_for_year(pbp_rows, target.season) if pbp_rows else []
    pbp_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    for r in pbp_rows:
        k = (norm_player_name(r.get("player", "")), norm_team(r.get("team", "")), norm_season(r.get("season", "")))
        if k[0] and k[1] and k[2]:
            pbp_lookup[k] = r

    sections = {
        "Impact": [
            ("BPM", "bpm", False, 1),
            ("RAPM", "rapm", False, 1),
            ("Net Pts", "net_pts", False, 1),
            ("On/Off NetR", "onoff_net_rating", False, 1),
        ],
        "Scoring": [
            ("Usage", "usg", False, 1),
            ("TS%", "ts_per", True, 1),
            ("2P%", "twop_per", True, 1),
            ("Dunks/100", "dunks_100_bt", False, 2),
            ("Rim Att/100", "rim_att_100_bt", False, 1),
            ("Rim%", "rim_pct", True, 1),
            ("Mid%", "mid_pct", True, 1),
            ("3P%", "tp_per", True, 1),
            ("3PA/100", "threepa100", False, 1),
            ("FTA/100", "fta100_bt", False, 1),
            ("FT%", "ft_per", True, 1),
            ("FTr", "ftr", False, 1),
        ],
        "Playmaking": [
            ("AST%", "ast_per", True, 1),
            ("TO%", "to_per", True, 1),
            ("A/TO", "ast_tov", False, 2),
            ("Rim Ast/100", "rim_assists_100_btposs", False, 2),
        ],
        "Defense": [
            ("STL%", "stl_per", True, 1),
            ("BLK%", "blk_per", True, 1),
            ("DBPM", "dbpm", False, 1),
        ],
        "Rebounding": [
            ("OREB%", "orb_per", True, 1),
            ("DREB%", "drb_per", True, 1),
        ],
    }

    def section_rows(rows: list[tuple[str, str, bool, int]]) -> str:
        rows_html = ""
        for label, key, is_pct, digits in rows:
            if key.startswith("pbp_"):
                pbp_key = key.replace("pbp_", "")
                value, pct = pbp_metric_percentile(pbp_target, pbp_cohort, pbp_key)
                rows_html += bt_row_html(label, value, pct, is_percent=False, digits=digits)
                continue
            if key == "rim_assists_100_btposs":
                def rate_for_bt_row(br: dict[str, str]) -> float | None:
                    return bt_metric_value(br, "rim_assists_100_btposs")

                value = rate_for_bt_row(target_row)
                cohort_vals = [v for v in (rate_for_bt_row(r) for r in cohort) if v is not None and math.isfinite(v)]
                pct = percentile(value, cohort_vals) if value is not None and cohort_vals else None
                rows_html += bt_row_html(label, value, pct, is_percent=False, digits=digits)
                continue
            value, pct = bt_metric_percentile(target_row, cohort, key)
            if label == "STL%":
                stl_val = bt_display_stl_pct(value)
                rows_html += bt_row_html(label, stl_val, pct, is_percent=is_pct, digits=1, truncate=True)
            elif label == "BLK%":
                blk_val = bt_display_blk_pct(value)
                rows_html += bt_row_html(label, blk_val, pct, is_percent=False, digits=1, truncate=True)
            else:
                rows_html += bt_row_html(label, value, pct, is_percent=is_pct, digits=digits)
        return rows_html

    impact_html = section_rows(sections["Impact"])
    scoring_html = section_rows(sections["Scoring"])
    playmaking_html = section_rows(sections["Playmaking"])
    defense_html = section_rows(sections["Defense"])
    rebounding_html = section_rows(sections["Rebounding"])

    return f"""
      <div class="panel" style="margin-top:14px;">
        <h3>Advanced Percentiles</h3>
        <div class="shot-meta">Season: {html.escape(target.season)}</div>
        <div class="section-grid">
          <div class="section-card"><h4>Impact</h4>{impact_html}{build_bpm_trend_svg(target, adv_rows)}</div>
          <div class="section-card"><h4>Scoring</h4>{scoring_html}</div>
          <div class="section-card">
            <h4>Playmaking</h4>
            {playmaking_html}
            <h4 style="margin-top:4px;">Defense</h4>
            {defense_html}
            <h4 style="margin-top:4px;">Rebounding</h4>
            {rebounding_html}
          </div>
        </div>
      </div>
"""


def build_self_creation_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    bt_playerstat_rows: list[dict[str, Any]],
    pbp_rows: list[dict[str, str]],
    pbp_games_map: dict[tuple[str, str, str], float] | None = None,
) -> str:
    if not bt_playerstat_rows:
        return '<div class="panel"><h3>Self Creation</h3><div class="shot-meta">No Bart playerstat JSON loaded.</div></div>'
    target_bt = bt_find_target_row(bt_rows, target) if bt_rows else None
    target_ps = find_bt_playerstat_row(bt_playerstat_rows, target.player, target.team)
    if not target_bt or not target_ps:
        return '<div class="panel"><h3>Self Creation</h3><div class="shot-meta">No matching player/team/season in Bart playerstat JSON.</div></div>'

    enriched_lookup = load_enriched_lookup_for_script_season(target.season)

    def enriched_off_poss(player_name: str, team_name: str, season: str) -> float | None:
        k = (norm_player_name(player_name), norm_team(team_name), norm_season(season))
        er = enriched_lookup.get(k)
        if not er:
            return None
        v = to_float(_enriched_nested_value(er, "off_team_poss", "value"))
        if v is None or not math.isfinite(v) or v <= 0:
            return None
        return float(v)

    target_poss = enriched_off_poss(target.player, target.team, target.season)
    target_metrics = bt_playerstat_metrics_from_row(target_ps, target_poss)
    if not target_metrics:
        return '<div class="panel"><h3>Self Creation</h3><div class="shot-meta">Missing enriched off_team_poss.value for self-creation normalization.</div></div>'

    cohort_bt = bt_cohort_for_year(bt_rows, target.season)
    metric_vals: dict[str, list[float]] = defaultdict(list)
    for r in cohort_bt:
        ps = find_bt_playerstat_row(bt_playerstat_rows, bt_get(r, ["player_name"]), bt_get(r, ["team"]))
        if not ps:
            continue
        poss = enriched_off_poss(bt_get(r, ["player_name"]), bt_get(r, ["team"]), bt_get(r, ["year"]))
        m = bt_playerstat_metrics_from_row(ps, poss)
        if not m:
            continue
        for k, v in m.items():
            if math.isfinite(v):
                metric_vals[k].append(v)

    rows_html = ""
    specs = [
        ("UAsst'd Dunks/100", "unassisted_dunks_100"),
        ("UAsst'd Rim FGM/100", "unassisted_rim_makes_100"),
        ("UAsst'd Mid FGM/100", "unassisted_mid_makes_100"),
        ("UAsst'd 3PM/100", "unassisted_3pm_100"),
        ("Unassisted Pts/100", "unassisted_points_100"),
    ]
    for label, key in specs:
        value = target_metrics.get(key)
        cohort = metric_vals.get(key, [])
        pct = percentile(value, cohort) if value is not None and cohort else None
        rows_html += bt_row_html(label, value, pct, is_percent=False, digits=2)

    return f"""
      <div class="panel">
        <h3>Self Creation</h3>
        {rows_html}
      </div>
"""


def build_playstyles_html(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> str:
    enriched_lookup = load_enriched_lookup_for_script_season(target.season)
    ek = (norm_player_name(target.player), norm_team(target.team), norm_season(target.season))
    erow = enriched_lookup.get(ek)
    if not erow:
        return '<div class="panel"><h3>Playstyles</h3><div class="shot-meta">No matching enriched playstyle row found for this player/team/season.</div></div>'

    cohort_players = load_enriched_players_for_script_season(target.season)
    if not cohort_players:
        return '<div class="panel"><h3>Playstyles</h3><div class="shot-meta">No enriched cohort loaded for this season.</div></div>'

    specs: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = [
        ("Drives", ("style", "Rim Attack", "adj_pts", "value"), ("style", "Rim Attack", "possPctUsg", "value")),
        ("Spotup 3", ("style", "Perimeter Sniper", "adj_pts", "value"), ("style", "Perimeter Sniper", "possPctUsg", "value")),
        ("OTD 3", ("style", "Dribble Jumper", "adj_pts", "value"), ("style", "Dribble Jumper", "possPctUsg", "value")),
        ("Mid Range", ("style", "Mid-Range", "adj_pts", "value"), ("style", "Mid-Range", "possPctUsg", "value")),
        ("PnR Passer", ("style", "PnR Passer", "adj_pts", "value"), ("style", "PnR Passer", "possPctUsg", "value")),
        ("PnR Roller", ("style", "Big Cut & Roll", "adj_pts", "value"), ("style", "Big Cut & Roll", "possPctUsg", "value")),
        ("Pick & Pop", ("style", "Pick & Pop", "adj_pts", "value"), ("style", "Pick & Pop", "possPctUsg", "value")),
        ("Post Up", ("style", "Post-Up", "adj_pts", "value"), ("style", "Post-Up", "possPctUsg", "value")),
        ("Cutter", ("style", "Backdoor Cut", "adj_pts", "value"), ("style", "Backdoor Cut", "possPctUsg", "value")),
        ("Transition", ("style", "Transition", "adj_pts", "value"), ("style", "Transition", "possPctUsg", "value")),
    ]

    rows_html = ""
    shown_rows = 0
    for label, ppp_path, vol_path in specs:
        ppp_raw = to_float(_enriched_nested_value(erow, *ppp_path))
        vol_raw = to_float(_enriched_nested_value(erow, *vol_path))
        ppp_v = float(ppp_raw) if ppp_raw is not None and math.isfinite(ppp_raw) else 0.0
        vol_v = (float(vol_raw) * 100.0) if vol_raw is not None and math.isfinite(vol_raw) else 0.0

        ppp_vals: list[float] = []
        vol_vals: list[float] = []
        for r in cohort_players:
            rv_ppp = to_float(_enriched_nested_value(r, *ppp_path))
            rv_vol = to_float(_enriched_nested_value(r, *vol_path))
            if rv_ppp is not None and math.isfinite(rv_ppp):
                ppp_vals.append(float(rv_ppp))
            if rv_vol is not None and math.isfinite(rv_vol):
                vol_vals.append(float(rv_vol) * 100.0)

        ppp_pct = 0.0 if ppp_v <= 0 else (percentile(ppp_v, ppp_vals) if ppp_vals else 0.0)
        vol_pct = 0.0 if vol_v <= 0 else (percentile(vol_v, vol_vals) if vol_vals else 0.0)

        ppp_w = max(0.0, min(100.0, float(ppp_pct)))
        vol_w = max(0.0, min(100.0, float(vol_pct)))
        ppp_badge = f"{int(round(ppp_w))}"
        vol_badge = f"{int(round(vol_w))}"
        ppp_txt = f"{ppp_v:.2f} PPP"
        vol_txt = f"{vol_v:.2f} poss/100"

        rows_html += f"""
        <div class="play-row">
          <div class="play-name">{html.escape(label)}</div>
          <div class="play-stack">
            <div class="play-line">
              <div class="play-track">
                <div class="play-fill play-vol" style="width:{vol_w:.1f}%"></div>
                <span class="play-badge" style="left:{vol_w:.1f}%">{vol_badge}</span>
              </div>
              <div class="play-tag">{vol_txt}</div>
            </div>
            <div class="play-line">
              <div class="play-track">
                <div class="play-fill play-ppp" style="width:{ppp_w:.1f}%"></div>
                <span class="play-badge" style="left:{ppp_w:.1f}%">{ppp_badge}</span>
              </div>
              <div class="play-tag">{ppp_txt}</div>
            </div>
          </div>
        </div>
        """
        shown_rows += 1

    if shown_rows == 0:
        return '<div class="panel"><h3>Playstyles</h3><div class="shot-meta">No playstyle values available for this player.</div></div>'

    return f"""
      <div class="panel">
        <div class="play-head">
          <h3>Playstyles</h3>
          <div class="play-legend">
            <div class="play-legend-item"><span class="play-legend-dot play-vol"></span>Volume</div>
            <div class="play-legend-item"><span class="play-legend-dot play-ppp"></span>PPP</div>
          </div>
        </div>
        <div class="play-grid">
          {rows_html}
        </div>
      </div>
"""


def build_shot_diet_html(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> str:
    if not bt_rows:
        return '<div class="panel"><h3>Shot Diet</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'

    row = bt_find_target_row(bt_rows, target)
    if not row:
        return '<div class="panel"><h3>Shot Diet</h3><div class="shot-meta">No matching Bart Torvik row found for this player/team/season.</div></div>'

    rim_att = bt_num(row, ["rimatt", " rimatt", "rimmade+rimmiss", " rimmade+rimmiss"])
    if rim_att is None:
        rm = bt_num(row, ["rimmade", " rimmade"]) or 0.0
        rmiss = bt_num(row, ["rimmiss", " rimmiss"]) or 0.0
        rim_att = rm + rmiss

    mid_att = bt_num(row, ["midatt", " midatt", "midmade+midmiss", " midmade+midmiss"])
    if mid_att is None:
        mm = bt_num(row, ["midmade", " midmade"]) or 0.0
        mmiss = bt_num(row, ["midmiss", " midmiss"]) or 0.0
        mid_att = mm + mmiss

    three_att = bt_num(row, ["TPA", " TPA", "tpa", " tpa"]) or 0.0
    total = rim_att + mid_att + three_att
    if total <= 0:
        return '<div class="panel"><h3>Shot Diet</h3><div class="shot-meta">No attempt data available.</div></div>'

    rim_pct = 100.0 * rim_att / total
    mid_pct = 100.0 * mid_att / total
    three_pct = 100.0 * three_att / total

    return f"""
      <div class="panel">
        <h3>Shot Diet</h3>
        <div class="shotdiet-bar">
          <div class="shotdiet-seg shotdiet-rim" style="width:{rim_pct:.2f}%"></div>
          <div class="shotdiet-seg shotdiet-mid" style="width:{mid_pct:.2f}%"></div>
          <div class="shotdiet-seg shotdiet-three" style="width:{three_pct:.2f}%;background:#60a5fa !important;"></div>
        </div>
        <div class="shotdiet-legend">
          <div class="shotdiet-key"><span class="shotdiet-dot shotdiet-rim"></span> Rim ({rim_pct:.1f}%)</div>
          <div class="shotdiet-key"><span class="shotdiet-dot shotdiet-mid"></span> Non-Rim 2 ({mid_pct:.1f}%)</div>
          <div class="shotdiet-key"><span class="shotdiet-dot shotdiet-three"></span> 3PA ({three_pct:.1f}%)</div>
        </div>
      </div>
"""


def build_team_impact_html(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> str:
    if not bt_rows:
        return '<div class="panel"><h3>Team Impact</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'

    row = bt_find_target_row(bt_rows, target)
    if not row:
        return '<div class="panel"><h3>Team Impact</h3><div class="shot-meta">No matching Bart row found for this player/team/season.</div></div>'

    def val(key: str) -> float | None:
        v = to_float(row.get(key, ""))
        if v is None or not math.isfinite(v):
            return None
        return fmt_percent_source_value(v)

    def fmt_cell(v: float | None) -> str:
        return "-" if v is None else f"{v:.1f}"

    def fmt_diff(v: float | None) -> str:
        return "-" if v is None else f"{v:+.1f}"

    def diff_color(diff: float | None, good_positive: bool) -> str:
        if diff is None or abs(diff) < 1e-12:
            return "var(--muted)"
        if good_positive:
            return "#22c55e" if diff > 0 else "#ef4444"
        return "#22c55e" if diff < 0 else "#ef4444"

    def render_rows(
        title: str,
        specs: list[tuple[str, str, str, bool]],
    ) -> str:
        rows_html = ""
        for label, on_key, off_key, good_positive in specs:
            on_v = val(on_key)
            off_v = val(off_key)
            diff_v = (on_v - off_v) if (on_v is not None and off_v is not None) else None
            rows_html += (
                f'<tr>'
                f'<td class="ti-metric">{html.escape(label)}</td>'
                f'<td class="ti-num">{fmt_cell(on_v)}</td>'
                f'<td class="ti-num">{fmt_cell(off_v)}</td>'
                f'<td class="ti-num" style="color:{diff_color(diff_v, good_positive)};font-weight:700;">{fmt_diff(diff_v)}</td>'
                f'</tr>'
            )
        return f"""
        <div class="ti-section">
          <table class="ti-table">
            <colgroup>
              <col style="width:46%">
              <col style="width:18%">
              <col style="width:18%">
              <col style="width:18%">
            </colgroup>
            <thead>
              <tr><th class="ti-subhead">{html.escape(title)}</th><th>On</th><th>Off</th><th>Diff</th></tr>
            </thead>
            <tbody>
              {rows_html}
            </tbody>
          </table>
        </div>
        """

    offense_specs = [
        ("eFG%", "on.off_efg.old_value", "off.off_efg.old_value", True),
        ("TO%", "on.off_to.value", "off.off_to.value", False),
        ("Rim%", "on.off_2prim.value", "off.off_2prim.value", True),
        ("Rim Rate", "on.off_2primr.value", "off.off_2primr.value", True),
        ("FTr", "on.off_ftr.value", "off.off_ftr.value", True),
        ("3Pr", "on.off_3pr.value", "off.off_3pr.value", True),
    ]
    defense_specs = [
        ("Opp eFG%", "on.def_efg.old_value", "off.def_efg.old_value", False),
        ("Opp TO%", "on.def_to.value", "off.def_to.value", True),
        ("Opp Rim%", "on.def_2prim.value", "off.def_2prim.value", False),
        ("Opp Rim Rate", "on.def_2primr.value", "off.def_2primr.value", False),
        ("Opp FTr", "on.def_ftr.value", "off.def_ftr.value", False),
        ("Opp 3Pr", "on.def_3pr.value", "off.def_3pr.value", False),
    ]
    reb_specs = [
        ("OREB%", "on.off_orb.value", "off.off_orb.value", True),
        ("Opp OREB%", "on.def_orb.value", "off.def_orb.value", False),
    ]

    return f"""
      <div class="panel">
        <h3>Team Impact</h3>
        {render_rows("Offense", offense_specs)}
        {render_rows("Defense", defense_specs)}
        {render_rows("Rebounding", reb_specs)}
      </div>
"""


def build_draft_projection_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    rsci_map: dict[str, int],
    wnba_draft_map: dict[str, int] | None = None,
) -> str:
    league_label = "WNBA" if ENRICHED_GENDER == "Women" else "NBA"
    model_tag = "wnba" if ENRICHED_GENDER == "Women" else "nba"
    if not bt_rows:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'
    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">No matching Bart row found for this player/team/season.</div></div>'

    ys = norm_season(target.season)
    if not ys.isdigit():
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">Invalid season for projection.</div></div>'
    target_year = int(ys)

    metric_keys = [
        "bpm", "dbpm", "usg", "ts_per", "twop_per", "tp_per",
        "ast_per", "to_per", "stl_per", "blk_per", "orb_per", "drb_per",
        "ftr", "threepa100", "rim_att_100_bt", "dunks_100_bt",
        "rim_assists_100_btposs", "rapm", "net_pts", "onoff_net_rating",
    ]

    # Build per-year percentile maps once for comparability across eras.
    rows_for_maps: list[dict[str, str]] = []
    for r in bt_rows:
        y = norm_season(bt_get(r, ["year"]))
        if not y.isdigit():
            continue
        yi = int(y)
        if 2010 <= yi <= target_year:
            rows_for_maps.append(r)
    by_year: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows_for_maps:
        by_year[norm_season(bt_get(r, ["year"]))].append(r)

    def build_pct_lookup(items: list[tuple[int, float]]) -> dict[int, float]:
        if not items:
            return {}
        n = len(items)
        s = sorted(items, key=lambda x: x[1])
        out: dict[int, float] = {}
        i = 0
        while i < n:
            j = i + 1
            while j < n and s[j][1] == s[i][1]:
                j += 1
            p = 100.0 * (i + 0.5 * (j - i)) / n
            for k in range(i, j):
                out[s[k][0]] = p
            i = j
        return out

    metric_pct_map: dict[tuple[str, str], dict[int, float]] = {}
    for year, rows in by_year.items():
        for key in metric_keys:
            vals: list[tuple[int, float]] = []
            for r in rows:
                v = bt_metric_value(r, key)
                if v is None or not math.isfinite(v):
                    continue
                vals.append((id(r), float(v)))
            mp = build_pct_lookup(vals)
            if key == "to_per":
                mp = {rk: 100.0 - pv for rk, pv in mp.items()}
            metric_pct_map[(year, key)] = mp

    def metric_pct_for_row(r: dict[str, str], key: str) -> float | None:
        year = norm_season(bt_get(r, ["year"]))
        return metric_pct_map.get((year, key), {}).get(id(r))

    rsci_exact = {norm_player_name(k): v for k, v in rsci_map.items()}

    def row_age_height(r: dict[str, str]) -> tuple[float | None, float | None]:
        return _bio_age_height_for_row(r, bio_lookup)

    def row_rsci_score(r: dict[str, str]) -> float | None:
        n = norm_player_name(bt_get(r, ["player_name"]))
        rank = rsci_exact.get(n)
        return rsci_rank_to_score(rank)

    target_vec: dict[str, float] = {}
    for key in metric_keys:
        p = metric_pct_for_row(target_row, key)
        if p is not None and math.isfinite(p):
            target_vec[key] = float(p)
    t_age, t_hgt = row_age_height(target_row)
    t_rsci = rsci_rank_to_score(find_rsci_rank(target.player, rsci_map))

    if len(target_vec) < 6:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">Not enough data to build projection.</div></div>'

    def feature_components(
        vec: dict[str, float],
        age: float | None,
        hgt: float | None,
        rsci_score: float | None,
    ) -> dict[str, float]:
        out: dict[str, float] = {}
        for k in metric_keys:
            v = vec.get(k)
            if v is None or not math.isfinite(v):
                continue
            out[k] = float(v)
        if rsci_score is not None and math.isfinite(rsci_score):
            out["rsci"] = float(rsci_score)
        if hgt is not None and math.isfinite(hgt):
            h_score = max(0.0, min(100.0, 50.0 + (float(hgt) - 72.0) * 6.5))
            out["height"] = float(h_score)
        if age is not None and math.isfinite(age):
            a_score = max(0.0, min(100.0, 90.0 - (float(age) - 19.0) * 14.0))
            out["age"] = float(a_score)
        return out

    def score_from_components(comps: dict[str, float], weights: dict[str, float]) -> float | None:
        num = 0.0
        den = 0.0
        for k, v in comps.items():
            w = float(weights.get(k, 0.0))
            if w <= 0.0 or not math.isfinite(v):
                continue
            num += w * float(v)
            den += w
        if den <= 0.0:
            return None
        return num / den

    # Build candidate historical set (exclude current season and likely still-active undrafted players).
    candidates_raw: list[dict[str, Any]] = []
    for r in bt_rows:
        y = norm_season(bt_get(r, ["year"]))
        if not y.isdigit():
            continue
        yi = int(y)
        if yi < 2010 or yi >= target_year:
            continue

        pick: int | None = None
        if ENRICHED_GENDER == "Women" and wnba_draft_map:
            pick = wnba_draft_map.get(norm_player_name(bt_get(r, ["player_name"])))
        if pick is None:
            pick = parse_pick_number(bt_get(r, ["pick"]))
        age, hgt = row_age_height(r)

        vec: dict[str, float] = {}
        for key in metric_keys:
            p = metric_pct_for_row(r, key)
            if p is not None and math.isfinite(p):
                vec[key] = float(p)
        comps = feature_components(vec, age, hgt, row_rsci_score(r))
        if len(comps) < 6:
            continue
        candidates_raw.append(
            {
                "comps": comps,
                "pick": pick,
                "bucket": draft_bucket_index_for_pick(pick),
                "age": age,
                "hgt": hgt,
                "pos": bt_row_position_bucket(r),
            }
        )

    if len(candidates_raw) < 500:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">Insufficient historical sample for projection.</div></div>'

    def corr(xs: list[float], ys: list[float]) -> float:
        if len(xs) < 10 or len(ys) < 10 or len(xs) != len(ys):
            return 0.0
        mx = sum(xs) / len(xs)
        my = sum(ys) / len(ys)
        num = 0.0
        dx2 = 0.0
        dy2 = 0.0
        for x, y in zip(xs, ys):
            dx = x - mx
            dy = y - my
            num += dx * dy
            dx2 += dx * dx
            dy2 += dy * dy
        den = math.sqrt(dx2 * dy2)
        if den <= 1e-12:
            return 0.0
        return num / den

    model_path = (
        Path(__file__).resolve().parent.parent
        / "player_cards_pipeline"
        / "data"
        / "models"
        / f"stat_draft_weights_{model_tag}_v1.json"
    )

    learned_weights: dict[str, float] = {}
    if model_path.exists():
        try:
            payload = json.loads(model_path.read_text(encoding="utf-8"))
            wobj = payload.get("weights", {}) if isinstance(payload, dict) else {}
            if isinstance(wobj, dict):
                for k, v in wobj.items():
                    fv = to_float(v)
                    if fv is not None and math.isfinite(fv) and fv > 0.0:
                        learned_weights[str(k)] = float(fv)
        except Exception:
            learned_weights = {}

    if not learned_weights:
        all_feature_keys = sorted({k for c in candidates_raw for k in c["comps"].keys()})
        for fk in all_feature_keys:
            xs_d: list[float] = []
            ys_d: list[float] = []
            xs_p: list[float] = []
            ys_p: list[float] = []
            for c in candidates_raw:
                v = c["comps"].get(fk)
                if v is None or not math.isfinite(v):
                    continue
                picked = c["pick"] is not None
                xs_d.append(float(v))
                ys_d.append(1.0 if picked else 0.0)
                if picked:
                    pnum = int(c["pick"])
                    xs_p.append(float(v))
                    ys_p.append((61.0 - float(pnum)) / 60.0)
            c_d = corr(xs_d, ys_d)
            c_p = corr(xs_p, ys_p)
            w = max(0.0, 0.35 * c_d + 0.65 * c_p)
            if math.isfinite(w) and w > 0.0:
                learned_weights[fk] = w
        # Fallback for cohorts where directional correlations wash out (common in smaller/noisier samples):
        # use absolute-signal weighting so we still learn a usable feature set.
        if not learned_weights:
            for fk in all_feature_keys:
                xs_d = []
                ys_d = []
                xs_p = []
                ys_p = []
                for c in candidates_raw:
                    v = c["comps"].get(fk)
                    if v is None or not math.isfinite(v):
                        continue
                    picked = c["pick"] is not None
                    xs_d.append(float(v))
                    ys_d.append(1.0 if picked else 0.0)
                    if picked:
                        pnum = int(c["pick"])
                        xs_p.append(float(v))
                        ys_p.append((61.0 - float(pnum)) / 60.0)
                c_d = abs(corr(xs_d, ys_d))
                c_p = abs(corr(xs_p, ys_p))
                w = 0.35 * c_d + 0.65 * c_p
                if math.isfinite(w) and w > 0.0:
                    learned_weights[fk] = w
        # Last-resort deterministic prior so projection still runs.
        if not learned_weights:
            for fk in metric_keys:
                learned_weights[fk] = 1.0
            learned_weights["height"] = 0.8
            learned_weights["rsci"] = 0.9
        if learned_weights:
            try:
                model_path.parent.mkdir(parents=True, exist_ok=True)
                payload = {
                    "model": f"stat_draft_projection_{model_tag}",
                    "version": 1,
                    "created_utc": datetime.now(timezone.utc).isoformat(),
                    "feature_count": len(learned_weights),
                    "weights": learned_weights,
                }
                model_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            except Exception:
                pass

    if not learned_weights:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">Unable to learn feature weights for projection.</div></div>'

    target_comps = feature_components(target_vec, t_age, t_hgt, t_rsci)
    target_score = score_from_components(target_comps, learned_weights)
    if target_score is None:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">Not enough core metrics to build projection.</div></div>'

    for c in candidates_raw:
        c["score"] = score_from_components(c["comps"], learned_weights)

    drafted_bucket_count = len(DRAFT_BUCKETS) - 1
    drafted_candidates = [
        c for c in candidates_raw
        if c["pick"] is not None and int(c["bucket"]) < drafted_bucket_count and c.get("score") is not None
    ]
    min_drafted_required = 350
    if ENRICHED_GENDER == "Women":
        # WNBA historical draft sample is much smaller than NBA.
        min_drafted_required = 30
    if len(drafted_candidates) < min_drafted_required:
        return (
            f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3>'
            f'<div class="shot-meta">Not enough drafted history to build projection '
            f'({len(drafted_candidates)} found; need {min_drafted_required}).</div></div>'
        )

    # Step 1: drafted-only comps for conditional pick-range distribution.
    drafted_candidates.sort(key=lambda c: abs(float(c["score"]) - float(target_score)))
    drafted_neighbors = drafted_candidates[:900]
    target_pos = bt_row_position_bucket(target_row)
    score_sigma = 4.2
    drafted_neighbor_weights: list[tuple[float, int, float]] = []
    for c in drafted_neighbors:
        idx = int(c["bucket"])
        d = (float(c["score"]) - float(target_score)) / score_sigma
        wt = math.exp(-0.5 * d * d)
        c_age = c["age"]
        c_hgt = c["hgt"]
        if t_age is not None and c_age is not None and math.isfinite(t_age) and math.isfinite(c_age):
            wt *= math.exp(-0.5 * ((float(c_age) - float(t_age)) / 1.1) ** 2)
        if t_hgt is not None and c_hgt is not None and math.isfinite(t_hgt) and math.isfinite(c_hgt):
            wt *= math.exp(-0.5 * ((float(c_hgt) - float(t_hgt)) / 2.0) ** 2)
        if target_pos and c["pos"] and target_pos != c["pos"]:
            wt *= 0.78
        if wt > 1e-10:
            drafted_neighbor_weights.append((wt, idx, abs(float(c["score"]) - float(target_score))))

    total_w = sum(w for w, _, _ in drafted_neighbor_weights)
    if total_w <= 0:
        return f'<div class="panel"><h3>Statistical {league_label} Draft Projection</h3><div class="shot-meta">Could not compute projection weights.</div></div>'

    # Drafted-only conditional bucket mix.
    drafted_bucket_w = [0.0 for _ in range(drafted_bucket_count)]
    for w, idx, _gap in drafted_neighbor_weights:
        drafted_bucket_w[idx] += w
    drafted_total = sum(drafted_bucket_w)
    if drafted_total <= 0:
        drafted_mix = [1.0 / drafted_bucket_count for _ in range(drafted_bucket_count)]
    else:
        drafted_mix = [
            (drafted_bucket_w[i] + 0.10) / (drafted_total + drafted_bucket_count * 0.10)
            for i in range(drafted_bucket_count)
        ]

    # Step 2: undrafted gate based on "how drafted-like" this profile is.
    drafted_scores = [float(c["score"]) for c in drafted_candidates if c.get("score") is not None and math.isfinite(float(c["score"]))]
    drafted_score_pct = percentile(float(target_score), drafted_scores) if drafted_scores else 0.0
    nearest_gap = min((gap for _w, _idx, gap in drafted_neighbor_weights), default=999.0)
    nearest_similarity = math.exp(-0.5 * (nearest_gap / 5.0) ** 2)
    drafted_like = 0.65 * (drafted_score_pct / 100.0) + 0.35 * nearest_similarity

    # Convert drafted-like signal into drafted probability; keeps elite profiles from flattening.
    drafted_prob = max(0.05, min(0.99, (drafted_like - 0.18) / 0.72))

    # Elite-profile floor:
    # prevent clearly elite younger prospects from getting implausibly high undrafted odds.
    elite_keys = ["bpm", "dbpm", "rapm", "net_pts", "onoff_net_rating", "ts_per", "usg", "ast_per", "tp_per"]
    elite_vals = [target_vec[k] for k in elite_keys if k in target_vec and math.isfinite(target_vec[k])]
    elite_stat_score = (sum(elite_vals) / len(elite_vals)) if elite_vals else 0.0
    elite_age = (t_age is not None and math.isfinite(t_age) and float(t_age) <= 20.8)
    elite_rsci = (t_rsci is not None and math.isfinite(t_rsci) and float(t_rsci) >= 82.0)

    # Intra-drafted calibration:
    # shift drafted bucket mix toward earlier slots for elite profiles,
    # and toward later slots for weaker drafted profiles.
    top_signal = max(
        0.0,
        min(
            1.0,
            0.60 * (drafted_score_pct / 100.0)
            + 0.40 * (elite_stat_score / 100.0),
        ),
    )
    # Positive tilt => earlier picks, negative tilt => later picks.
    tilt = (top_signal - 0.5) * 1.1
    # For buckets [Number one, Top5, Top10, Top20, Outside Top20]
    bucket_axis = [2.0, 1.0, 0.0, -1.0, -2.0]
    if drafted_bucket_count == len(bucket_axis):
        tilted = []
        for i, p in enumerate(drafted_mix):
            factor = math.exp(tilt * bucket_axis[i])
            tilted.append(max(1e-9, p * factor))
        zt = sum(tilted)
        if zt > 0:
            drafted_mix = [v / zt for v in tilted]

    undrafted_cap: float | None = None
    if elite_stat_score >= 95.0:
        undrafted_cap = 0.05
    elif elite_stat_score >= 92.0 and (elite_age or elite_rsci):
        undrafted_cap = 0.08
    elif elite_stat_score >= 89.0 and elite_age and elite_rsci:
        undrafted_cap = 0.12

    if undrafted_cap is not None:
        drafted_prob = max(drafted_prob, 1.0 - undrafted_cap)

    probs = [drafted_prob * m for m in drafted_mix]
    probs.append(max(0.0, 1.0 - drafted_prob))

    drafted_prob = sum(probs[:drafted_bucket_count])
    top20_prob = sum(probs[:4])
    target_yr_raw = norm_text(bt_get(target_row, ["yr"]))
    target_return_profile = (
        ("fr" in target_yr_raw)
        or ("freshman" in target_yr_raw)
        or ("so" in target_yr_raw)
        or ("soph" in target_yr_raw)
        or ("jr" in target_yr_raw)
        or ("junior" in target_yr_raw)
        or (t_age is not None and math.isfinite(t_age) and t_age < 22.0)
    )
    # Projected range should be based on cumulative draft-range probability,
    # not single disjoint bucket maxima.
    cum_probs: list[float] = []
    csum = 0.0
    for i in range(drafted_bucket_count):
        csum += probs[i]
        cum_probs.append(csum)

    # Requested women-card buckets:
    # Number one pick, Top 5, Top 10, Top 20, Outside Top 20.
    if cum_probs[0] >= 0.5:
        proj_label = "Number One Pick"
    elif cum_probs[1] >= 0.5:
        proj_label = "Top 5"
    elif cum_probs[2] >= 0.5:
        proj_label = "Top 10"
    elif cum_probs[3] >= 0.5:
        proj_label = "Top 20"
    else:
        proj_label = "Outside Top 20"

    # Display cumulative for requested ranges; Outside Top 20 is complement of Top 20.
    display_labels = [
        "Number One Pick",
        "Top 5",
        "Top 10",
        "Top 20",
        "Outside Top 20",
    ]
    display_probs = [
        cum_probs[0],
        cum_probs[1],
        cum_probs[2],
        cum_probs[3],
        max(0.0, min(1.0, 1.0 - cum_probs[3])),
    ]

    rows_html = ""
    for i, lbl_disp in enumerate(display_labels):
        pct = 100.0 * display_probs[i]
        rows_html += (
            f'<div class="draft-odd-row">'
            f'<div class="draft-odd-k">{html.escape(lbl_disp)}</div>'
            f'<div class="draft-odd-v">{pct:.1f}%</div>'
            f'</div>'
        )

    return f"""
      <div class="panel draft-proj-panel">
        <h3>Statistical {league_label} Draft Projection</h3>
        <div class="draft-proj-main">{html.escape(proj_label)}</div>
        <div class="draft-proj-sub">Drafted: {100.0 * drafted_prob:.1f}% | Top 20: {100.0 * top20_prob:.1f}%</div>
        <div class="draft-odds-grid">
          {rows_html}
        </div>
        <div class="draft-proj-sub">Projections based solely on statistical profile in an average draft</div>
      </div>
"""


def _height_to_inches(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d+)\s*'\s*(\d+)\s*\"?\s*$", s)
    if m:
        return float(int(m.group(1)) * 12 + int(m.group(2)))
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", s)
    if m:
        return float(int(m.group(1)) * 12 + int(m.group(2)))
    v = to_float(s)
    if v is None:
        return None
    if 48 <= v <= 96:
        return float(v)
    return None


def _bio_age_height_for_row(row: dict[str, str], bio_lookup: dict[tuple[str, str, str], dict[str, str]]) -> tuple[float | None, float | None]:
    player = bt_get(row, ["player_name"])
    team = bt_get(row, ["team"])
    season = bt_get(row, ["year"])
    bio = dict(lookup_bio_fallback(bio_lookup, player, team, season))

    age_val: float | None = None
    if bio:
        age_val = to_float(bio.get("age", ""))
    if age_val is None:
        # Fallback to BT age fields when bio lookup is missing/incomplete.
        age_val = bt_num(row, ["DD Age", " DD Age", "Age", " age", "age"])

    height_val: float | None = None
    if bio:
        height_val = _height_to_inches(bio.get("height", ""))
    if height_val is None:
        height_val = bt_num(row, ["inches", " inches"])

    return age_val, height_val


def build_player_comparisons_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    top_n: int = 5,
) -> str:
    if not bt_rows:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'
    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">No matching Bart row for comparisons.</div></div>'

    metric_keys = [
        # Impact
        "bpm", "rapm", "onoff_net_rating",
        # Scoring
        "usg", "ts_per", "twop_per", "dunks_100_bt", "rim_att_100_bt", "rim_pct", "mid_pct",
        "tp_per", "threepa100", "fta100_bt", "ft_per", "ftr",
        # Playmaking
        "ast_per", "to_per", "ast_tov", "rim_assists_100_btposs",
        # Defense/Rebounding
        "stl_per", "blk_per", "dbpm", "orb_per", "drb_per",
    ]

    # Build per-season cohorts once (comparison pool: 2019+).
    bt_rows_pool = [r for r in bt_rows if (norm_season(bt_get(r, ["year"])).isdigit() and int(norm_season(bt_get(r, ["year"]))) >= 2019)]
    if target_row not in bt_rows_pool:
        bt_rows_pool.append(target_row)
    # Keep player comps position-consistent (G/F/C) now that percentiles are position-based.
    target_bucket = bt_row_position_bucket(target_row)
    if target_bucket:
        by_pos = [r for r in bt_rows_pool if bt_row_position_bucket(r) == target_bucket]
        if by_pos:
            bt_rows_pool = by_pos
    by_year: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in bt_rows_pool:
        by_year[norm_season(bt_get(r, ["year"]))].append(r)

    def build_pct_lookup(items: list[tuple[int, float]]) -> dict[int, float]:
        # Percentile with midrank tie handling in O(n log n).
        if not items:
            return {}
        n = len(items)
        s = sorted(items, key=lambda x: x[1])
        out: dict[int, float] = {}
        i = 0
        while i < n:
            j = i + 1
            while j < n and s[j][1] == s[i][1]:
                j += 1
            p = 100.0 * (i + 0.5 * (j - i)) / n
            for k in range(i, j):
                out[s[k][0]] = p
            i = j
        return out

    # Precompute metric percentile lookup maps by year/key.
    metric_pct_map: dict[tuple[str, str], dict[int, float]] = {}
    for year, rows in by_year.items():
        for key in metric_keys:
            vals: list[tuple[int, float]] = []
            for r in rows:
                v = bt_metric_value(r, key)
                if v is None or not math.isfinite(v):
                    continue
                vals.append((id(r), float(v)))
            mp = build_pct_lookup(vals)
            if key == "to_per":
                mp = {rk: 100.0 - pv for rk, pv in mp.items()}
            metric_pct_map[(year, key)] = mp

    def metric_pct_for_row(r: dict[str, str], key: str) -> float | None:
        year = norm_season(bt_get(r, ["year"]))
        return metric_pct_map.get((year, key), {}).get(id(r))

    hgt_by_row: dict[int, float] = {}
    for r in bt_rows_pool:
        _age_v, h_v = _bio_age_height_for_row(r, bio_lookup)
        if h_v is not None and math.isfinite(h_v):
            hgt_by_row[id(r)] = h_v

    hgt_pct_map: dict[str, dict[int, float]] = {}
    for year, rows in by_year.items():
        hgt_items = [(id(r), hgt_by_row[id(r)]) for r in rows if id(r) in hgt_by_row]
        hgt_pct_map[year] = build_pct_lookup(hgt_items)

    def hgt_pct_for_row(r: dict[str, str]) -> float | None:
        year = norm_season(bt_get(r, ["year"]))
        return hgt_pct_map.get(year, {}).get(id(r))

    target_vec: dict[str, float] = {}
    for k in metric_keys:
        p = metric_pct_for_row(target_row, k)
        if p is not None:
            target_vec[k] = p
    tp_hgt = hgt_pct_for_row(target_row)
    if tp_hgt is not None:
        target_vec["height_pct"] = tp_hgt

    if len(target_vec) < 8:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">Not enough data to compute comparisons.</div></div>'

    def similarity(other: dict[str, str]) -> float | None:
        # Exclude exact same player-season.
        if (
            norm_player_name(bt_get(other, ["player_name"])) == norm_player_name(target.player)
            and norm_team(bt_get(other, ["team"])) == norm_team(target.team)
            and norm_season(bt_get(other, ["year"])) == norm_season(target.season)
        ):
            return None

        keys = list(metric_keys)
        ov: dict[str, float] = {}
        for k in keys:
            tv = target_vec.get(k)
            if tv is None:
                continue
            pv = metric_pct_for_row(other, k)
            if pv is None:
                continue
            ov[k] = pv

        if "height_pct" in target_vec:
            pv = hgt_pct_for_row(other)
            if pv is not None:
                ov["height_pct"] = pv

        shared = [k for k in ov if k in target_vec]
        if len(shared) < 8:
            return None

        # Percentile-space similarity: 100 - average absolute percentile gap.
        diffs = [abs(float(target_vec[k]) - float(ov[k])) for k in shared]
        score = 100.0 - (sum(diffs) / len(diffs))
        return max(0.0, min(100.0, score))

    ranked: list[tuple[float, dict[str, str]]] = []
    for r in bt_rows_pool:
        s = similarity(r)
        if s is None:
            continue
        ranked.append((s, r))
    ranked.sort(key=lambda x: x[0], reverse=True)
    top = ranked[:top_n]
    if not top:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">No comparable players found.</div></div>'

    rows_html = ""
    for score, r in top:
        pname = bt_get(r, ["player_name"]) or "Unknown"
        pyear = bt_get(r, ["year"]) or "?"
        rows_html += f'<div class="comp-row"><span class="comp-name">{html.escape(pname)}</span><span class="comp-year">{html.escape(str(pyear))}</span><span class="comp-score">{score:.1f}</span></div>'

    return f"""
      <div class="panel">
        <h3>Player Comparisons</h3>
        <div class="comp-table">
          {rows_html}
        </div>
      </div>
"""


def bt_fg_totals_for_target(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> tuple[int | None, int | None]:
    if not bt_rows:
        return None, None
    row = bt_find_target_row(bt_rows, target)
    if not row:
        return None, None
    twom = bt_num(row, ["twoPM", " twoPM"])
    twoa = bt_num(row, ["twoPA", " twoPA"])
    threem = bt_num(row, ["TPM", " TPM", "tpm", " tpm"])
    threea = bt_num(row, ["TPA", " TPA", "tpa", " tpa"])
    if twom is None or twoa is None or threem is None or threea is None:
        return None, None
    fgm = int(round(twom + threem))
    fga = int(round(twoa + threea))
    if fga <= 0:
        return None, None
    return fgm, fga


def bt_per_game_overrides(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> dict[str, float]:
    row = bt_find_target_row(bt_rows, target) if bt_rows else None
    if not row:
        return {}
    gp = bt_num(row, ["GP"])
    if gp is None or gp <= 0:
        gp = 0.0

    two_pm = bt_num(row, ["twoPM", " twoPM"])
    two_pa = bt_num(row, ["twoPA", " twoPA"])
    tp_m = bt_num(row, ["TPM", " TPM"])
    tp_a = bt_num(row, ["TPA", " TPA"])
    ft_m = bt_num(row, ["FTM"])
    ft_a = bt_num(row, ["FTA"])

    out: dict[str, float] = {}
    for k, aliases in [
        ("ppg", ["pts"]),
        ("rpg", ["treb"]),
        ("apg", ["ast"]),
        ("spg", ["stl"]),
        ("bpg", ["blk"]),
    ]:
        # Bart's box columns are already per-game for these fields.
        pg = bt_num(row, aliases)
        if pg is not None:
            out[k] = float(pg)

    if two_pm is not None and two_pa is not None and tp_m is not None and tp_a is not None:
        fgm = two_pm + tp_m
        fga = two_pa + tp_a
        if fga > 0:
            out["fg_pct"] = 100.0 * fgm / fga
        if tp_a > 0:
            out["tp_pct"] = 100.0 * tp_m / tp_a
    if ft_m is not None and ft_a is not None and ft_a > 0:
        out["ft_pct"] = 100.0 * ft_m / ft_a
    return out


def render_card(
    stats: PlayerGameStats,
    bio: dict[str, str],
    shots: list[dict[str, Any]],
    season_shots: list[dict[str, Any]],
    per_game_pcts: dict[str, float | None],
    grade_boxes_html: str,
    bt_percentiles_html: str,
    self_creation_html: str,
    playstyles_html: str,
    team_impact_html: str,
    shot_diet_html: str,
    player_comparisons_html: str,
    advanced_html: str,
    shot_header_makes: int | None,
    shot_header_attempts: int | None,
    shot_pps_oe_line: str,
    draft_projection_html: str,
    per_game_overrides: dict[str, float] | None,
    out_path: Path,
) -> None:
    name = stats.player
    team = stats.team
    season = stats.season

    height = format_height(bio.get("height", ""))
    position = bio.get("position", "") or "N/A"
    subtitle = f"{team} | {season} | Position: {position} | Height: {height}"

    # Use full event-derived FG totals for header stats, not only plotted (x/y) shots.
    shot_makes = shot_header_makes if shot_header_makes is not None else stats.fgm
    shot_att = shot_header_attempts if shot_header_attempts is not None else stats.fga
    shot_pct = (100.0 * shot_makes / shot_att) if shot_att else 0.0

    pg = {
        "ppg": stats.ppg,
        "rpg": stats.rpg,
        "apg": stats.apg,
        "spg": stats.spg,
        "bpg": stats.bpg,
        "fg_pct": stats.fg_pct,
        "tp_pct": stats.tp_pct,
        "ft_pct": stats.ft_pct,
    }
    if per_game_overrides:
        pg.update({k: v for k, v in per_game_overrides.items() if v is not None and math.isfinite(v)})

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{html.escape(name)} - Player Card</title>
<style>
:root {{
  --bg: #0a0a0a;
  --panel: #141414;
  --line: #3b3b3b;
  --text: #f5f5f5;
  --muted: #d4d4d4;
  --accent: #ffffff;
  --bar: #22c55e;
  --panel-alt: #1f1f1f;
  --bar-track: #2a2a2a;
  --shot-mid: #ef4444;
}}
body {{
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: "Segoe UI", Arial, sans-serif;
}}
.wrap {{
  max-width: 1100px;
  margin: 18px auto;
  padding: 16px;
}}
.card {{
  border: 2px solid var(--line);
  border-radius: 12px;
  background: #000000;
  padding: 16px;
}}
.title {{
  font-size: 44px;
  line-height: 1;
  font-weight: 800;
  color: var(--accent);
  margin: 0 0 8px 0;
}}
.title-row {{
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}}
.grade-strip {{
  display: grid;
  grid-template-columns: repeat(5, minmax(96px, 1fr));
  gap: 8px;
  min-width: 560px;
}}
.grade-chip {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 6px 8px;
  text-align: center;
  background: var(--panel-alt);
}}
.grade-k {{
  color: var(--muted);
  font-size: 11px;
  line-height: 1.1;
}}
.grade-v {{
  font-size: 22px;
  font-weight: 800;
  line-height: 1.1;
  color: var(--accent);
}}
.sub {{
  color: var(--muted);
  margin-bottom: 0;
  font-size: 15px;
}}
.row {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 14px;
}}
.panel {{
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 12px;
  background: var(--panel);
}}
.per-game-panel {{
  margin-top: 10px;
}}
.panel h3 {{
  margin: 0 0 4px 0;
  font-size: 14px;
}}
.section-grid {{
  display: grid;
  grid-template-columns: repeat(3, minmax(210px, 1fr));
  gap: 10px;
}}
.section-card {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 8px;
  background: var(--panel-alt);
}}
.section-card h4 {{
  margin: 0 0 4px 0;
  font-size: 12px;
  color: var(--text);
  letter-spacing: 0.1px;
}}
.kv {{
  display: grid;
  grid-template-columns: repeat(3, minmax(120px, 1fr));
  gap: 8px;
  font-size: 14px;
}}
.stat-strip {{
  margin-top: 10px;
  display: grid;
  grid-template-columns: repeat(8, 1fr);
  gap: 8px;
}}
.shot-wrap {{
  display: flex;
  justify-content: flex-start;
  gap: 12px;
  align-items: stretch;
}}
.left-wrap {{
  flex: 0 0 33%;
  min-width: 320px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}}
.shot-panel {{
  min-width: 0;
}}
.shot-panel svg {{
  display: block;
  margin: 0 auto;
  transform: translateX(-8px);
}}
.shot-chart-col {{ min-width: 0; }}
.chip {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 8px;
  text-align: center;
  background: var(--panel-alt);
}}
.chip .k {{
  color: var(--muted);
  font-size: 12px;
}}
.chip .v {{
  font-size: 20px;
  font-weight: 700;
}}
.chip .p {{
  margin-top: 3px;
  color: var(--muted);
  font-size: 10px;
  line-height: 1;
}}
.metric-row {{
  display: grid;
  grid-template-columns: 72px 58px 1fr 34px;
  gap: 6px;
  align-items: center;
  margin-bottom: 5px;
}}
.metric-label {{
  color: var(--muted);
  font-size: 12px;
}}
.metric-val {{
  font-weight: 700;
  font-size: 12px;
}}
.bar-wrap {{
  height: 12px;
  border-radius: 999px;
  background: var(--bar-track);
  overflow: hidden;
}}
.bar-fill {{
  height: 12px;
  background: var(--bar);
}}
.metric-pct {{
  text-align: right;
  font-weight: 700;
  font-size: 12px;
}}
.shot-meta {{
  font-size: 13px;
  color: var(--muted);
  margin-bottom: 8px;
}}
.trend-wrap {{
  margin-top: 8px;
}}
.shotdiet-bar {{
  width: 100%;
  height: 16px;
  border-radius: 999px;
  overflow: hidden;
  background: var(--bar-track);
  border: 1px solid var(--line);
  display: flex;
}}
.shotdiet-seg {{
  height: 100%;
  display: block;
  flex: 0 0 auto;
}}
.shotdiet-rim {{
  background: var(--bar);
}}
.shotdiet-mid {{
  background: var(--shot-mid);
}}
.shotdiet-three {{
  background: #60a5fa !important;
  box-shadow: none;
}}
.shotdiet-legend {{
  margin-top: 8px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  font-size: 12px;
  color: var(--muted);
}}
.shotdiet-key {{
  display: flex;
  align-items: center;
  gap: 7px;
  white-space: nowrap;
}}
.shotdiet-dot {{
  width: 9px;
  height: 9px;
  border-radius: 999px;
  display: inline-block;
}}
.right-wrap {{
  flex: 1 1 auto;
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  align-items: stretch;
  margin-top: 14px;
}}
.right-col {{
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-height: 100%;
}}
.comp-table {{
  display: grid;
  gap: 6px;
}}
.comp-row {{
  display: grid;
  grid-template-columns: 1fr 42px 42px;
  gap: 8px;
  font-size: 12px;
  align-items: center;
  border: 1px solid var(--line);
  border-radius: 7px;
  padding: 6px 8px;
  background: var(--panel-alt);
}}
.comp-name {{
  font-weight: 600;
  color: var(--text);
}}
.comp-year {{
  color: var(--muted);
  text-align: right;
}}
.comp-score {{
  color: var(--accent);
  text-align: right;
  font-weight: 700;
}}
.play-grid {{
  display: grid;
  gap: 11px;
}}
.play-row {{
  display: grid;
  grid-template-columns: 74px 1fr;
  gap: 6px;
  align-items: center;
}}
.play-name {{
  font-size: 12px;
  font-weight: 700;
  color: var(--text);
}}
.play-stack {{
  display: grid;
  gap: 6px;
}}
.play-line {{
  display: grid;
  grid-template-columns: 1fr 82px;
  gap: 6px;
  align-items: center;
}}
.play-track {{
  position: relative;
  height: 10px;
  background: var(--bar-track);
  border: 1px solid var(--line);
  border-radius: 999px;
  overflow: visible;
}}
.play-fill {{
  height: 100%;
  border-radius: 999px;
}}
.play-vol {{
  background: #60a5fa;
}}
.play-ppp {{
  background: var(--bar);
}}
.play-badge {{
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  width: 15px;
  height: 15px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: #0e0e0e;
  color: #fff;
  font-size: 8px;
  font-weight: 700;
  line-height: 15px;
  text-align: center;
}}
.play-tag {{
  color: var(--muted);
  font-size: 9px;
  white-space: nowrap;
}}
.play-head {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 6px;
}}
.play-head h3 {{
  margin: 0;
}}
.play-legend {{
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 11px;
  color: var(--muted);
}}
.play-legend-item {{
  display: inline-flex;
  align-items: center;
  gap: 6px;
}}
.play-legend-dot {{
  width: 9px;
  height: 9px;
  border-radius: 999px;
  display: inline-block;
}}
.playstyles-wrap {{
  flex: 1 1 auto;
  display: flex;
}}
.playstyles-wrap .panel {{
  width: 100%;
}}
.team-impact-wrap {{
  margin-top: 2px;
}}
.comp-bottom {{
  margin-top: 0;
}}
.draft-proj-main {{
  font-size: 22px;
  font-weight: 800;
  color: var(--accent);
  margin-top: 2px;
}}
.draft-proj-panel {{
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
}}
.draft-proj-sub {{
  margin-top: 4px;
  color: var(--muted);
  font-size: 11px;
}}
.draft-odds-grid {{
  margin-top: 8px;
  display: grid;
  gap: 4px;
  flex: 1 1 auto;
  align-content: start;
}}
.draft-odd-row {{
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 8px;
  align-items: center;
  font-size: 11px;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 4px 6px;
  background: var(--panel-alt);
}}
.draft-odd-k {{
  color: var(--muted);
}}
.draft-odd-v {{
  font-weight: 700;
  color: var(--text);
}}
.ti-comp-stack {{
  display: flex;
  flex-direction: column;
  gap: 0;
}}
.ti-section {{
  margin-top: 8px;
}}
.ti-subhead {{
  font-size: 13px;
  color: var(--text);
  font-weight: 700;
  margin: 0;
  text-align: left !important;
}}
.ti-table {{
  width: 100%;
  border-collapse: separate;
  border-spacing: 0;
  table-layout: fixed;
  margin: 0 0 6px 0;
  font-size: 11px;
}}
.ti-table th, .ti-table td {{
  border: none;
  padding: 2px 4px;
}}
.ti-table th {{
  color: var(--muted);
  font-weight: 700;
  text-align: right;
}}
.ti-table th:first-child {{
  text-align: left;
}}
.ti-metric {{
  color: var(--muted);
  text-align: left;
  white-space: nowrap;
}}
.ti-num {{
  text-align: right;
  font-variant-numeric: tabular-nums;
}}
@media (max-width: 920px) {{
  .title-row {{ flex-direction: column; }}
  .grade-strip {{ min-width: 0; width: 100%; grid-template-columns: repeat(2, minmax(130px, 1fr)); }}
  .row {{ grid-template-columns: 1fr; }}
  .stat-strip {{ grid-template-columns: repeat(3, 1fr); }}
  .section-grid {{ grid-template-columns: 1fr; }}
  .left-wrap {{ width: 100%; flex: 1 1 auto; min-width: 0; }}
  .shot-panel {{ width: 100%; min-width: 0; }}
  .shot-chart-col {{ min-width: 0; }}
  .right-wrap {{ width: 100%; margin-top: 14px; }}
  .right-wrap {{ grid-template-columns: 1fr; }}
  .right-col {{ width: 100%; }}
}}
</style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="title-row">
        <h1 class="title">{html.escape(name)}</h1>
        <div class="grade-strip">{grade_boxes_html}</div>
      </div>
      <div class="sub">{html.escape(subtitle)}</div>

      <div class="panel per-game-panel">
        <h3>Per Game</h3>
        <div class="stat-strip">
          <div class="chip"><div class="k">PPG</div><div class="v">{fmt(pg['ppg'])}</div><div class="p">{(f"{per_game_pcts['ppg']:.0f}%" if per_game_pcts.get('ppg') is not None else "")}</div></div>
          <div class="chip"><div class="k">RPG</div><div class="v">{fmt(pg['rpg'])}</div><div class="p">{(f"{per_game_pcts['rpg']:.0f}%" if per_game_pcts.get('rpg') is not None else "")}</div></div>
          <div class="chip"><div class="k">APG</div><div class="v">{fmt(pg['apg'])}</div><div class="p">{(f"{per_game_pcts['apg']:.0f}%" if per_game_pcts.get('apg') is not None else "")}</div></div>
          <div class="chip"><div class="k">SPG</div><div class="v">{fmt(pg['spg'])}</div><div class="p">{(f"{per_game_pcts['spg']:.0f}%" if per_game_pcts.get('spg') is not None else "")}</div></div>
          <div class="chip"><div class="k">BPG</div><div class="v">{fmt(pg['bpg'])}</div><div class="p">{(f"{per_game_pcts['bpg']:.0f}%" if per_game_pcts.get('bpg') is not None else "")}</div></div>
          <div class="chip"><div class="k">FG%</div><div class="v">{fmt(pg['fg_pct'])}</div><div class="p">{(f"{per_game_pcts['fg_pct']:.0f}%" if per_game_pcts.get('fg_pct') is not None else "")}</div></div>
          <div class="chip"><div class="k">3P%</div><div class="v">{fmt(pg['tp_pct'])}</div><div class="p">{(f"{per_game_pcts['tp_pct']:.0f}%" if per_game_pcts.get('tp_pct') is not None else "")}</div></div>
          <div class="chip"><div class="k">FT%</div><div class="v">{fmt(pg['ft_pct'])}</div><div class="p">{(f"{per_game_pcts['ft_pct']:.0f}%" if per_game_pcts.get('ft_pct') is not None else "")}</div></div>
        </div>
      </div>

      {bt_percentiles_html}

      <div class="shot-wrap">
        <div class="left-wrap">
          <div class="panel shot-panel shot-chart-col" style="margin-top:14px;">
            <h3>Shot Chart</h3>
            <div class="shot-meta">Attempts: {shot_att} | Made: {shot_makes} | FG%: {fmt(shot_pct)}%</div>
            <div class="shot-meta">{html.escape(shot_pps_oe_line)}</div>
            {shot_svg(shots, season_shots, width=355, height=250)}
          </div>
          {draft_projection_html}
        </div>
        <div class="right-wrap">
          <div class="right-col">
            {self_creation_html}
            <div class="playstyles-wrap">{playstyles_html}</div>
          </div>
          <div class="right-col">
            {shot_diet_html}
            <div class="ti-comp-stack">
              <div class="team-impact-wrap">{team_impact_html}</div>
              <div class="comp-bottom">{player_comparisons_html}</div>
            </div>
          </div>
        </div>
      </div>
      {advanced_html}
    </div>
  </div>
</body>
</html>
"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")

def choose_player(
    players: list[PlayerGameStats],
    player: str,
    team: str | None,
    season: str | None,
) -> PlayerGameStats:
    np = norm_player_name(player)
    nt = norm_team(team or "")
    ns = norm_season(season or "")

    candidates = [p for p in players if norm_player_name(p.player) == np]
    if nt:
        candidates = [
            p
            for p in candidates
            if nt in norm_team(p.team) or norm_team(p.team) in nt
        ]
    if ns:
        candidates = [p for p in candidates if norm_season(p.season) == ns]
    if not candidates:
        raise RuntimeError("No player match found with the supplied player/team/season filters.")
    if len(candidates) > 1:
        candidates = sorted(candidates, key=lambda x: (x.team, x.season))
    return candidates[0]


def build_per_game_percentiles(
    players: list[PlayerGameStats],
    target: PlayerGameStats,
    min_games: int,
    bt_rows: list[dict[str, str]] | None = None,
) -> dict[str, float | None]:
    cohort = [p for p in players if norm_text(p.season) == norm_text(target.season) and p.games >= min_games]
    if bt_rows:
        pos_map: dict[tuple[str, str, str], str] = {}
        for r in bt_rows:
            p = norm_player_name(bt_get(r, ["player_name"]))
            t = norm_team(bt_get(r, ["team"]))
            y = norm_season(bt_get(r, ["year"]))
            b = bt_row_position_bucket(r)
            if p and t and y and b:
                pos_map[(p, t, y)] = b

        tk = (norm_player_name(target.player), norm_team(target.team), norm_season(target.season))
        tb = pos_map.get(tk)
        if tb:
            pos_cohort = [
                p for p in cohort
                if pos_map.get((norm_player_name(p.player), norm_team(p.team), norm_season(p.season))) == tb
            ]
            if pos_cohort:
                cohort = pos_cohort
    if not cohort:
        cohort = [p for p in players if p.games >= min_games]
    metrics = {
        "ppg": [p.ppg for p in cohort],
        "rpg": [p.rpg for p in cohort],
        "apg": [p.apg for p in cohort],
        "spg": [p.spg for p in cohort],
        "bpg": [p.bpg for p in cohort],
        "fg_pct": [p.fg_pct for p in cohort],
        "tp_pct": [p.tp_pct for p in cohort],
        "ft_pct": [p.ft_pct for p in cohort],
    }
    return {
        "ppg": percentile_safe(target.ppg, metrics["ppg"]),
        "rpg": percentile_safe(target.rpg, metrics["rpg"]),
        "apg": percentile_safe(target.apg, metrics["apg"]),
        "spg": percentile_safe(target.spg, metrics["spg"]),
        "bpg": percentile_safe(target.bpg, metrics["bpg"]),
        "fg_pct": percentile_safe(target.fg_pct, metrics["fg_pct"]),
        "tp_pct": percentile_safe(target.tp_pct, metrics["tp_pct"]),
        "ft_pct": percentile_safe(target.ft_pct, metrics["ft_pct"]),
    }


def build_player_team_hint_map(bt_rows: list[dict[str, str]]) -> dict[tuple[str, str], str]:
    choices: dict[tuple[str, str], set[str]] = defaultdict(set)
    for r in bt_rows:
        p = norm_player_name(bt_get(r, ["player_name"]))
        y = norm_season(bt_get(r, ["year"]))
        t = bt_get(r, ["team"]).strip()
        if p and y and t:
            choices[(p, y)].add(t)
    out: dict[tuple[str, str], str] = {}
    for k, ts in choices.items():
        if len(ts) == 1:
            out[k] = next(iter(ts))
    return out


def build_player_pool_from_bt(bt_rows: list[dict[str, str]]) -> list[PlayerGameStats]:
    out: list[PlayerGameStats] = []
    for r in bt_rows:
        player = bt_get(r, ["player_name"]).strip()
        team = bt_get(r, ["team"]).strip()
        season = norm_season(bt_get(r, ["year"]))
        if not player or not team or not season:
            continue

        gp = bt_num(r, ["GP", "gp"]) or 0.0
        if gp <= 0:
            gp = 1.0

        # Per-game box-score stats from BT.
        ppg = bt_num(r, ["pts", "PTS"]) or 0.0
        oreb = bt_num(r, ["oreb", "OREB"]) or 0.0
        dreb = bt_num(r, ["dreb", "DREB"]) or 0.0
        apg = bt_num(r, ["ast", "AST"]) or 0.0
        spg = bt_num(r, ["stl", "STL"]) or 0.0
        bpg = bt_num(r, ["blk", "BLK"]) or 0.0

        # Build FG/3P/FT from make/attempt pairs when available.
        two_m = bt_num(r, ["twoPM", "2PM", "2PM_per_g"]) or 0.0
        two_a = bt_num(r, ["twoPA", "2PA", "2PA_per_g"]) or 0.0
        three_m = bt_num(r, ["TPM", "3PM"]) or 0.0
        three_a = bt_num(r, ["TPA", "3PA"]) or 0.0
        ft_m = bt_num(r, ["FTM"]) or 0.0
        ft_a = bt_num(r, ["FTA"]) or 0.0

        fgm_pg = two_m + three_m
        fga_pg = two_a + three_a
        if fga_pg <= 0:
            fg_pct = bt_num(r, ["eFG", "FG_per", "fg_per"])
            if fg_pct is not None and fg_pct > 1.0 and ppg > 0:
                # Approximate FGA from points and FG%; better than zeros for percentile cohorts.
                fga_pg = ppg / (2.0 * max(0.01, fg_pct / 100.0))
                fgm_pg = fga_pg * (fg_pct / 100.0)

        out.append(
            PlayerGameStats(
                player=player,
                team=team,
                season=season,
                games=max(1, int(round(gp))),
                points=max(0, int(round(ppg * gp))),
                rebounds=max(0, int(round((oreb + dreb) * gp))),
                assists=max(0, int(round(apg * gp))),
                steals=max(0, int(round(spg * gp))),
                blocks=max(0, int(round(bpg * gp))),
                fgm=max(0, int(round(fgm_pg * gp))),
                fga=max(0, int(round(fga_pg * gp))),
                tpm=max(0, int(round(three_m * gp))),
                tpa=max(0, int(round(three_a * gp))),
                ftm=max(0, int(round(ft_m * gp))),
                fta=max(0, int(round(ft_a * gp))),
            )
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a college basketball player card HTML.")
    ap.add_argument("--plays-csv", default="", help="Optional CBBD plays CSV (regular/fullseason).")
    ap.add_argument("--player", required=True, help="Player name.")
    ap.add_argument("--team", default="", help="Optional team filter.")
    ap.add_argument("--season", default="", help="Optional season filter (e.g. 2025).")
    ap.add_argument("--bio-csv", default="", help="Optional CSV for bio/class/height/age/position.")
    ap.add_argument("--bt-csv", default="", help="Optional Bart Torvik advanced stats CSV.")
    ap.add_argument("--lebron-csv", default="", help="Optional CSV with LEBRON/O-LEBRON/D-LEBRON/BPM.")
    ap.add_argument("--rimfluence-csv", default="", help="Optional Rimfluence model output CSV.")
    ap.add_argument("--style-csv", default="", help="Optional style/playtype CSV (e.g., master sheet).")
    ap.add_argument("--advgames-csv", default="", help="Optional per-game labeled advgames CSV for BPM trend.")
    ap.add_argument("--pbp-metrics-csv", default="", help="Optional player metrics CSV derived from ncaahoopR pbp logs.")
    ap.add_argument("--rsci-csv", default="", help="Optional RSCI rankings CSV path.")
    ap.add_argument("--wnba-draft-csv", default="", help="Optional WNBA draft CSV path (pick in col 1, player in col 3).")
    ap.add_argument("--gender", default="Women", help="Enriched dataset gender token: Women or Men.")
    ap.add_argument("--bt-playerstat-json", default="", help="Optional Bart playerstat JSON file path or URL.")
    ap.add_argument(
        "--bt-playerstat-url-template",
        default="https://barttorvik.com/ncaaw/{year}_pbp_playerstat_array.json",
        help="Bart playerstat URL template; {year} is replaced with target season year.",
    )
    ap.add_argument("--card-cache-db", default="", help="Optional precomputed season cache sqlite path.")
    ap.add_argument("--disable-card-cache", action="store_true", help="Disable reading precomputed card-section cache.")
    ap.add_argument("--out-html", required=True, help="Output HTML path.")
    ap.add_argument("--min-games", type=int, default=5, help="Min games for percentile cohort.")
    args = ap.parse_args()
    global ENRICHED_GENDER
    ENRICHED_GENDER = enriched_gender_token(args.gender)
    t0 = time.perf_counter()
    t_last = t0

    def stage(label: str) -> None:
        nonlocal t_last
        now = time.perf_counter()
        print(f"[timing] {label}: +{(now - t_last):.3f}s (total {(now - t0):.3f}s)")
        t_last = now

    plays_rows: list[dict[str, str]] = []
    if args.plays_csv:
        plays_path = Path(args.plays_csv)
        if plays_path.exists():
            _, plays_rows = read_csv_rows(plays_path)
    stage("Loaded plays CSV")

    # Optional advanced sources.
    bt_rows: list[dict[str, str]] = []
    lebron_rows: list[dict[str, str]] = []
    rim_rows: list[dict[str, str]] = []
    style_rows: list[dict[str, str]] = []
    adv_rows: list[dict[str, str]] = []
    pbp_rows: list[dict[str, str]] = []
    bt_playerstat_rows: list[dict[str, Any]] = []

    if args.bt_csv:
        _, bt_rows = read_csv_rows(Path(args.bt_csv))
    if args.lebron_csv:
        _, lebron_rows = read_csv_rows(Path(args.lebron_csv))
    if args.rimfluence_csv:
        _, rim_rows = read_csv_rows(Path(args.rimfluence_csv))
    if args.style_csv:
        _, style_rows = read_csv_rows(Path(args.style_csv))
    if args.advgames_csv:
        _, adv_rows = read_csv_rows(Path(args.advgames_csv))
    if args.pbp_metrics_csv:
        _, pbp_rows = read_csv_rows(Path(args.pbp_metrics_csv))
    stage("Loaded optional CSV inputs")

    # NCAAW cards currently do not use RSCI.
    rsci_map: dict[str, int] = {}
    stage("Skipped RSCI for NCAAW")
    wnba_draft_map: dict[str, int] = {}
    if ENRICHED_GENDER == "Women":
        draft_csv = args.wnba_draft_csv.strip()
        if not draft_csv:
            default_draft_csv = (
                Path(__file__).resolve().parent.parent
                / "player_cards_pipeline"
                / "data"
                / "manual"
                / "wnba_draft"
                / "wnba_draft.csv"
            )
            if default_draft_csv.exists():
                draft_csv = str(default_draft_csv)
        if draft_csv:
            try:
                wnba_draft_map = load_wnba_draft_lookup(Path(draft_csv))
            except Exception:
                wnba_draft_map = {}

    if bt_rows:
        inject_enriched_fields_into_bt_rows(bt_rows)
    stage("Injected enriched fields into BT rows")

    games_by_player: dict[tuple[str, str, str], set[str]] = {}
    players: list[PlayerGameStats] = build_player_pool_from_bt(bt_rows) if bt_rows else []
    if not players and plays_rows:
        team_hint_map = build_player_team_hint_map(bt_rows) if bt_rows else {}
        stats_map, games_by_player = build_player_stats(
            plays_rows,
            season_hint=args.season or "",
            team_hint_by_player_season=team_hint_map,
        )
        players = list(stats_map.values())
    if not players:
        raise RuntimeError("Could not build player pool from BT data (and no usable plays fallback).")
    stage("Built player pool")

    target = choose_player(players, args.player, args.team or None, args.season or None)
    stage("Selected target player")

    bio_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    if args.bio_csv:
        bio_lookup = load_bio_lookup(Path(args.bio_csv))
    bio = dict(lookup_bio_fallback(bio_lookup, target.player, target.team, target.season))
    target_bt_row = bt_find_target_row(bt_rows, target) if bt_rows else None
    if target_bt_row:
        pos_from_bt = bt_get(target_bt_row, ["roster.pos", "role"])
        if pos_from_bt.strip():
            bio["position"] = pos_from_bt
        if not (bio.get("height", "") or "").strip():
            bio["height"] = bt_get(target_bt_row, ["ht"])
        if not (bio.get("age", "") or "").strip():
            bt_age = bt_num(target_bt_row, ["DD Age", " DD Age", "Age", " age", "age"])
            if bt_age is not None and math.isfinite(bt_age):
                bio["age"] = f"{float(bt_age):.1f}"
    if not (bio.get("height", "") or "").strip() and target_bt_row:
        inches = bt_num(target_bt_row, ["inches", " inches"])
        if inches is not None and math.isfinite(inches):
            bio["height"] = str(int(round(inches)))
    stage("Loaded/merged bio fields")

    if args.bt_playerstat_json:
        bt_playerstat_rows = load_bt_playerstat_rows_from_source(args.bt_playerstat_json)
    else:
        ys = norm_season(target.season)
        local_ps = (
            Path(__file__).resolve().parent.parent
            / "player_cards_pipeline"
            / "data"
            / "bt"
            / "raw_playerstat_json"
            / f"{ys}_pbp_playerstat_array.json"
        )
        if local_ps.exists():
            try:
                bt_playerstat_rows = load_bt_playerstat_rows_from_source(str(local_ps))
            except Exception:
                bt_playerstat_rows = []
        if bt_playerstat_rows:
            pass
        else:
            bt_ps_url = args.bt_playerstat_url_template.format(year=ys)
            try:
                bt_playerstat_rows = load_bt_playerstat_rows_from_source(bt_ps_url)
            except Exception:
                bt_playerstat_rows = []
    stage("Loaded Bart playerstat JSON")

    bt_percentiles_html = ""
    grade_boxes_html = ""
    self_creation_html = ""
    playstyles_html = ""
    team_impact_html = ""
    shot_diet_html = ""
    player_comparisons_html = ""
    draft_projection_html = ""
    pps_line = "Points per Shot Over Expectation: N/A"
    bt_fgm, bt_fga = bt_fg_totals_for_target(target, bt_rows)
    per_game_pcts: dict[str, float] = {}
    cached_payload: dict[str, Any] | None = None

    if not args.disable_card_cache:
        cache_db_path = Path(args.card_cache_db) if args.card_cache_db else default_card_cache_db_path(target.season)
        cached_payload = load_cached_card_sections(cache_db_path, target, args.min_games)

    if cached_payload:
        bt_percentiles_html = str(cached_payload.get("bt_percentiles_html", ""))
        grade_boxes_html = str(cached_payload.get("grade_boxes_html", ""))
        self_creation_html = str(cached_payload.get("self_creation_html", ""))
        playstyles_html = str(cached_payload.get("playstyles_html", ""))
        team_impact_html = str(cached_payload.get("team_impact_html", ""))
        shot_diet_html = str(cached_payload.get("shot_diet_html", ""))
        player_comparisons_html = str(cached_payload.get("player_comparisons_html", ""))
        draft_projection_html = str(cached_payload.get("draft_projection_html", ""))
        pps_line = str(cached_payload.get("pps_line", pps_line))
        per_game_pcts = dict(cached_payload.get("per_game_pcts", {}) or {})
        cached_fgm = to_float(cached_payload.get("bt_fgm"))
        cached_fga = to_float(cached_payload.get("bt_fga"))
        if cached_fgm is not None and cached_fga is not None:
            bt_fgm, bt_fga = int(round(cached_fgm)), int(round(cached_fga))
    else:
        bt_percentiles_html = build_bt_percentile_html(target, bt_rows, adv_rows, pbp_rows)
        grade_boxes_html = build_grade_boxes_html(target, bt_rows)
        pbp_games_map = {k: float(len(v)) for k, v in games_by_player.items() if v}
        self_creation_html = build_self_creation_html(target, bt_rows, bt_playerstat_rows, pbp_rows, pbp_games_map=pbp_games_map)
        playstyles_html = build_playstyles_html(target, bt_rows)
        team_impact_html = build_team_impact_html(target, bt_rows)
        shot_diet_html = build_shot_diet_html(target, bt_rows)
        player_comparisons_html = build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5)
        draft_projection_html = build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map, wnba_draft_map)
        act_pps, exp_pps, pps_oe, pps_oe_pct = pps_over_expected_from_enriched(target)
        if pps_oe is not None:
            if pps_oe_pct is not None:
                p_rank = max(1, min(99, int(round(pps_oe_pct))))
                pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% ({ordinal(p_rank)} Percentile)"
            else:
                pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% (Percentile N/A)"
        per_game_pcts = build_per_game_percentiles(players, target, args.min_games, bt_rows=bt_rows)
    advanced_html = build_advanced_html(target, lebron_rows, rim_rows, style_rows)
    stage("Built card section HTML blocks")
    per_game_override = bt_per_game_overrides(target, bt_rows)

    shots: list[dict[str, Any]] = []
    season_shots: list[dict[str, Any]] = []
    if plays_rows:
        shots = collect_shots(plays_rows, target.player, target.team, target.season, season_hint=args.season or "")
        for row in plays_rows:
            if norm_text(_season_from_row(row, args.season or "")) != norm_text(target.season):
                continue
            x, y = _shot_loc_from_row(row)
            rng = _shot_range_from_row(row)
            if x is None or y is None or rng not in {"rim", "jumper", "three_pointer"}:
                continue
            season_shots.append(
                {
                    "x": x,
                    "y": y,
                    "made": _shot_made_from_row(row),
                    "range": rng,
                }
            )
    stage("Built shot data from plays/enriched")

    # Prefer enriched shot bins for chart plotting when available.
    enriched_lookup = load_enriched_lookup_for_script_season(target.season)
    ek = (norm_player_name(target.player), norm_team(target.team), norm_season(target.season))
    erow = enriched_lookup.get(ek)
    if erow:
        enr_shots, enr_makes, enr_att = build_shots_from_enriched_player_row(erow)
        if enr_shots:
            shots = enr_shots
            season_shots = enr_shots
            bt_fgm, bt_fga = enr_makes, enr_att

    stage("Computed percentiles")
    render_card(
        target,
        bio,
        shots,
        season_shots,
        per_game_pcts,
        grade_boxes_html,
        bt_percentiles_html,
        self_creation_html,
        playstyles_html,
        team_impact_html,
        shot_diet_html,
        player_comparisons_html,
        advanced_html,
        bt_fgm,
        bt_fga,
        pps_line,
        draft_projection_html,
        per_game_override,
        Path(args.out_html),
    )
    stage("Rendered HTML card")

    print(f"Wrote card: {args.out_html}")
    print(f"Player: {target.player} | Team: {target.team} | Season: {target.season}")
    if bt_rows:
        bt_cohort = bt_cohort_for_year(bt_rows, target.season)
        print(f"Bart Torvik cohort size: {len(bt_cohort)}")
    print(f"Shot points plotted: {len(shots)}")
    if shots:
        xs = [float(s["x"]) for s in shots]
        ys = [float(s["y"]) for s in shots]
        print(f"Shot x range: {min(xs):.1f}..{max(xs):.1f} | y range: {min(ys):.1f}..{max(ys):.1f}")


if __name__ == "__main__":
    main()
