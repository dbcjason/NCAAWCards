#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
import sys
import re
from pathlib import Path
from typing import Any


def load_card_module(project_root: Path):
    mod_path = project_root / "cbb_player_cards_v1" / "build_player_card.py"
    spec = importlib.util.spec_from_file_location("build_player_card", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def resolve_bt_csv(project_root: Path, requested: Path) -> Path:
    if requested.is_absolute():
        if requested.exists():
            return requested
    else:
        p = project_root / requested
        if p.exists():
            return p
    candidates = [
        project_root / "player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv",
        project_root / "player_cards_pipeline/data/bt/bt_advstats_2019_2026.csv",
        project_root / "player_cards_pipeline/data/bt/bt_advstats_2010_2025.csv",
        project_root / "player_cards_pipeline/data/bt/bt_advstats_2019_2025.csv",
        project_root / "player_cards_pipeline/data/bt/bt_advstats_2026.csv",
    ]
    found = next((p for p in candidates if p.exists()), None)
    if found is None:
        raise RuntimeError(
            f"BT CSV not found. Tried requested path: {requested} and {len(candidates)} fallback paths."
        )
    print(f"[batch-transfer] BT CSV fallback: {found}", flush=True)
    return found


def build_transfer_examples(bpc: Any, bt_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    by_player_year: dict[str, dict[int, dict[str, str]]] = {}
    for r in bt_rows:
        p = bpc.norm_player_name(bpc.bt_get(r, ["player_name"]))
        ys = bpc.norm_season(bpc.bt_get(r, ["year"]))
        if not p or not ys.isdigit():
            continue
        y = int(ys)
        season_map = by_player_year.setdefault(p, {})
        prev = season_map.get(y)
        if prev is None:
            season_map[y] = r
            continue
        prev_gp = bpc.bt_num(prev, ["GP", "gp"]) or 0.0
        cur_gp = bpc.bt_num(r, ["GP", "gp"]) or 0.0
        if cur_gp > prev_gp:
            season_map[y] = r

    examples: list[dict[str, Any]] = []
    for season_map in by_player_year.values():
        years = sorted(season_map.keys())
        for y in years:
            if (y + 1) not in season_map:
                continue
            src = season_map[y]
            dst = season_map[y + 1]
            src_conf = bpc._conference_key(bpc.bt_get(src, ["conf", "conference"]))
            dst_conf = bpc._conference_key(bpc.bt_get(dst, ["conf", "conference"]))
            src_m = bpc._row_transfer_metrics(src)
            dst_m = bpc._row_transfer_metrics(dst)
            if len(src_m) < 8 or len(dst_m) < 8:
                continue
            if not dst_conf:
                continue
            examples.append({"src_conf": src_conf, "dst_conf": dst_conf, "src": src_m, "dst": dst_m})
    return examples


def build_target_row_index(bpc: Any, bt_rows: list[dict[str, str]]) -> dict[tuple[str, str, str], dict[str, str]]:
    idx: dict[tuple[str, str, str], dict[str, str]] = {}
    for r in bt_rows:
        key = (
            bpc.norm_player_name(bpc.bt_get(r, ["player_name"])),
            bpc.norm_team(bpc.bt_get(r, ["team"])),
            bpc.norm_season(bpc.bt_get(r, ["year"])),
        )
        if not key[0] or not key[1] or not key[2]:
            continue
        prev = idx.get(key)
        if prev is None:
            idx[key] = r
            continue
        prev_gp = bpc.bt_num(prev, ["GP", "gp"]) or 0.0
        cur_gp = bpc.bt_num(r, ["GP", "gp"]) or 0.0
        if cur_gp > prev_gp:
            idx[key] = r
    return idx


def project_transfer_grade(
    bpc: Any,
    source: dict[str, float],
    source_conf: str,
    dest_conf: str,
    model: dict[str, Any],
    feat_keys: list[str],
    impact_keys: list[str],
) -> str:
    if len(source) < 8:
        return ""

    pool = model["pool"]
    scales = model["scales"]
    cohort_scores = model["cohort_scores"]
    if not pool or not cohort_scores:
        return ""

    weighted_examples: list[tuple[float, dict[str, Any]]] = []
    for e in pool:
        diffs: list[float] = []
        for k in feat_keys:
            tv = source.get(k)
            ev = e["src"].get(k)
            if tv is None or ev is None:
                continue
            if not (isinstance(tv, float | int) and isinstance(ev, float | int)):
                continue
            if not (float(tv) == float(tv) and float(ev) == float(ev)):
                continue
            s = scales.get(k, 1.0)
            diffs.append(abs(float(tv) - float(ev)) / max(1e-6, s))
        if len(diffs) < 8:
            continue
        d = sum(diffs) / len(diffs)
        if dest_conf and e["dst_conf"] == dest_conf:
            d *= 0.86
        elif dest_conf:
            d *= 1.10
        if source_conf and e["src_conf"] == source_conf:
            d *= 0.92
        w = bpc.math.exp(-1.35 * d)
        if w > 1e-9:
            weighted_examples.append((w, e))

    weighted_examples.sort(key=lambda x: x[0], reverse=True)
    weighted_examples = weighted_examples[:450]
    if not weighted_examples:
        return ""

    pred_impact_vals: list[float] = []
    for k in impact_keys:
        num = 0.0
        den = 0.0
        for w, e in weighted_examples:
            sv = e["src"].get(k)
            dv = e["dst"].get(k)
            if sv is None or dv is None:
                continue
            if not (float(sv) == float(sv) and float(dv) == float(dv)):
                continue
            if k in source and (float(source[k]) == float(source[k])):
                val = float(source[k]) + (float(dv) - float(sv))
            else:
                val = float(dv)
            num += w * val
            den += w
        if den > 0:
            pred_impact_vals.append(float(num / den))
    if not pred_impact_vals:
        return ""

    pred_impact_score = sum(pred_impact_vals) / len(pred_impact_vals)
    impact_pct = bpc.percentile(pred_impact_score, cohort_scores)
    return bpc._transfer_grade_from_percentile(impact_pct)


def conference_display(key: str) -> str:
    labels = {
        "acc": "ACC",
        "bigeast": "Big East",
        "bigten": "Big Ten",
        "big12": "Big 12",
        "sec": "SEC",
        "pac12": "Pac-12",
        "mountainwest": "Mountain West",
        "wcc": "WCC",
        "a10": "A10",
        "aac": "AAC",
        "mvc": "MVC",
        "mac": "MAC",
        "cusa": "CUSA",
        "sunbelt": "Sun Belt",
        "bigwest": "Big West",
        "wac": "WAC",
        "horizon": "Horizon",
        "socon": "SoCon",
        "ivy": "Ivy",
    }
    return labels.get(key, key.upper())


def player_class_from_row(bpc: Any, row: dict[str, str]) -> str:
    raw = (bpc.bt_get(row, ["class", "yr", "year", "cls", "eligibility", "roster.class"]) or "").strip()
    if not raw:
        return ""
    k = raw.lower().replace(".", "").strip()
    mapping = {
        "fr": "Freshman",
        "freshman": "Freshman",
        "rsfr": "Freshman",
        "so": "Sophomore",
        "soph": "Sophomore",
        "sophomore": "Sophomore",
        "rsso": "Sophomore",
        "jr": "Junior",
        "junior": "Junior",
        "rsjr": "Junior",
        "sr": "Senior",
        "senior": "Senior",
        "rssr": "Senior",
        "gr": "Graduate",
        "grad": "Graduate",
        "graduate": "Graduate",
        "super senior": "Graduate",
    }
    return mapping.get(k, raw)


def player_min_pct_from_row(bpc: Any, row: dict[str, str]) -> float | None:
    v = bpc.bt_num(
        row,
        [
            "Min%",
            "min%",
            "min_pct",
            "minpct",
            "min_per",
            "minutes_pct",
            "mpct",
        ],
    )
    if v is None:
        return None
    vv = float(v)
    # Some sources store rate stats as 0..1 fractions.
    if 0.0 <= vv <= 1.0:
        vv *= 100.0
    return vv




MANUAL_EXCLUDE_PLAYERS: set[tuple[str, str]] = {
    ("liamdaycogreen", ""),
    ("tjdrain", ""),
    ("rileysaunders", "northdakotast"),
    ("ianimegwu", "cornell"),
    ("alexmcfadden", "delaware"),
}


def _norm_player_key(v: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())


def is_manually_excluded_player(player_norm: str, team_norm: str) -> bool:
    pkey = _norm_player_key(player_norm)
    for pnorm, tnorm in MANUAL_EXCLUDE_PLAYERS:
        if pkey != pnorm:
            continue
        if not tnorm or tnorm == team_norm:
            return True
    return False

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Batch-run transfer projection grade matrix for all destination conferences.",
    )
    ap.add_argument("--project-root", default=".", help="Repo root (default: current dir).")
    ap.add_argument(
        "--bt-csv",
        default="player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv",
        help="BT advstats CSV path (relative to project-root unless absolute).",
    )
    ap.add_argument("--season", required=True, help="Target season (script season, e.g. 2026).")
    ap.add_argument("--out-csv", required=True, help="Output CSV path.")
    ap.add_argument("--min-games", type=int, default=5, help="Minimum GP to include.")
    ap.add_argument("--min-pct", type=float, default=5.0, help="Exclude players with Min% <= this value.")
    ap.add_argument("--min-mpg", type=float, default=3.0, help="Exclude players with MPG <= this value.")
    ap.add_argument("--team", default="", help="Optional source team filter.")
    ap.add_argument("--limit", type=int, default=0, help="Optional cap on players (0 = no cap).")
    ap.add_argument(
        "--conferences",
        default="",
        help="Optional comma-separated destination conferences (e.g. SEC,ACC,Big 12). Blank = all.",
    )
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    bt_csv = resolve_bt_csv(project_root, Path(args.bt_csv))
    out_csv = Path(args.out_csv)
    if not out_csv.is_absolute():
        out_csv = project_root / out_csv
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    bpc = load_card_module(project_root)
    _, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    row_idx = build_target_row_index(bpc, bt_rows)

    players = bpc.build_player_pool_from_bt(bt_rows)
    season_norm = bpc.norm_season(args.season)
    team_norm = bpc.norm_team(args.team) if args.team else ""

    filtered: list[tuple[Any, dict[str, str]]] = []
    seen_player_keys: set[tuple[str, str, str]] = set()
    for p in players:
        if bpc.norm_season(p.season) != season_norm:
            continue
        if team_norm and bpc.norm_team(p.team) != team_norm:
            continue
        key = (
            bpc.norm_player_name(p.player),
            bpc.norm_team(p.team),
            bpc.norm_season(p.season),
        )
        if is_manually_excluded_player(key[0], key[1]):
            continue
        row = row_idx.get(key)
        if not row:
            continue
        klass = player_class_from_row(bpc, row)
        if klass.strip().lower() == "senior":
            continue
        gp = bpc.bt_num(row, ["GP", "gp"])
        if gp is not None and gp < args.min_games:
            continue
        min_pct = player_min_pct_from_row(bpc, row)
        if min_pct is None or min_pct <= float(args.min_pct):
            continue
        mpg = bpc.bt_num(row, ["MPG", "mpg", "minutes_per_game", "min_per_game", "min per game"])
        if mpg is None and min_pct is not None:
            # Approximate MPG from minute share when explicit MPG is missing.
            mpg = float(min_pct) * 0.4
        if mpg is None:
            try:
                mpg = float(getattr(p, "mpg"))
            except Exception:
                mpg = None
        if mpg is None or mpg <= float(args.min_mpg):
            continue
        # Hard de-dupe guard: keep one row per normalized (season, player, team).
        dedupe_key = (bpc.norm_season(p.season), bpc.norm_player_name(p.player), bpc.norm_team(p.team))
        if dedupe_key in seen_player_keys:
            continue
        seen_player_keys.add(dedupe_key)
        filtered.append((p, row))
    filtered.sort(key=lambda x: (bpc.norm_team(x[0].team), bpc.norm_player_name(x[0].player)))
    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]
    print(f"[batch-transfer] eligible players: {len(filtered)}", flush=True)

    examples = build_transfer_examples(bpc, bt_rows)
    if len(examples) < 200:
        raise RuntimeError(f"Not enough historical transfer samples ({len(examples)}).")

    all_dest_keys = sorted({str(e["dst_conf"]) for e in examples if str(e["dst_conf"]).strip()})
    if args.conferences.strip():
        requested = [bpc._conference_key(x.strip()) for x in args.conferences.split(",") if x.strip()]
        dest_keys = [k for k in all_dest_keys if k in set(requested)]
    else:
        dest_keys = all_dest_keys
    if not dest_keys:
        raise RuntimeError("No destination conferences resolved for projection.")
    print(f"[batch-transfer] destinations={len(dest_keys)}", flush=True)

    feat_keys = [
        "mpg", "ppg", "rpg", "apg", "spg", "bpg", "fg_pct", "tp_pct", "ft_pct",
        "bpm", "usg", "ts_per", "rim_pct", "ast_per", "ast_tov",
        "stl_per", "blk_per", "orb_per", "drb_per", "rapm", "net_pts", "onoff_net_rating",
    ]
    impact_keys = ["bpm", "rapm", "net_pts"]

    conf_models: dict[str, dict[str, Any]] = {}
    for conf in dest_keys:
        same_dest = [e for e in examples if e["dst_conf"] == conf]
        pool = same_dest if len(same_dest) >= 35 else examples
        scales: dict[str, float] = {}
        for k in feat_keys:
            vals = sorted(
                [float(e["src"][k]) for e in pool if k in e["src"] and float(e["src"][k]) == float(e["src"][k])]
            )
            if len(vals) >= 12:
                lo = vals[max(0, int(0.1 * (len(vals) - 1)))]
                hi = vals[min(len(vals) - 1, int(0.9 * (len(vals) - 1)))]
                spread = hi - lo
                scales[k] = spread if spread > 1e-6 else 1.0
            else:
                scales[k] = 1.0
        grade_pool = same_dest if len(same_dest) >= 20 else pool
        cohort_scores: list[float] = []
        for e in grade_pool:
            vals = [e["dst"][k] for k in impact_keys if k in e["dst"]]
            if vals:
                cohort_scores.append(sum(float(v) for v in vals) / len(vals))
        conf_models[conf] = {"pool": pool, "scales": scales, "cohort_scores": cohort_scores}

    conf_cols = [conference_display(k) for k in dest_keys]
    headers = ["season", "player", "team", "source_conference", "class"] + conf_cols
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        total = len(filtered)
        for idx, (p, row) in enumerate(filtered, start=1):
            source = bpc._row_transfer_metrics(row)
            source_conf = bpc._conference_key(bpc.bt_get(row, ["conf", "conference"]))
            rec: dict[str, Any] = {
                "season": bpc.norm_season(p.season),
                "player": p.player,
                "team": p.team,
                "source_conference": bpc.bt_get(row, ["conf", "conference"]),
                "class": player_class_from_row(bpc, row),
            }
            for conf_key, col_name in zip(dest_keys, conf_cols):
                rec[col_name] = project_transfer_grade(
                    bpc=bpc,
                    source=source,
                    source_conf=source_conf,
                    dest_conf=conf_key,
                    model=conf_models[conf_key],
                    feat_keys=feat_keys,
                    impact_keys=impact_keys,
                )
            w.writerow(rec)
            if idx == 1 or idx % 100 == 0 or idx == total:
                print(f"[batch-transfer] {idx}/{total} written")

    print(f"[batch-transfer] done: rows={len(filtered)} out={out_csv}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BrokenPipeError:
        raise SystemExit(0)
    except KeyboardInterrupt:
        print("\n[batch-transfer] interrupted", file=sys.stderr)
        raise SystemExit(130)
