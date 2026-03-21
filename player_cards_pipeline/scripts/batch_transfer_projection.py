#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib.util
import sys
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


def project_player_transfer(
    bpc: Any,
    source: dict[str, float],
    source_conf: str,
    pool: list[dict[str, Any]],
    same_dest: list[dict[str, Any]],
    dest_conf: str,
    feat_keys: list[str],
    out_keys: list[str],
    scales: dict[str, float],
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "status": "ok",
        "error": "",
        "transfer_grade": "",
        "comps_weighted": "",
        "proj_ppg": "",
        "proj_rpg": "",
        "proj_apg": "",
        "proj_spg": "",
        "proj_bpg": "",
        "proj_fg_pct": "",
        "proj_tp_pct": "",
        "proj_ft_pct": "",
    }

    if len(source) < 8:
        out["status"] = "error"
        out["error"] = "insufficient_source_stats"
        return out

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
        out["status"] = "error"
        out["error"] = "no_similar_transfer_comps"
        return out

    predicted: dict[str, float] = {}
    for k in out_keys:
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
            predicted[k] = bpc._clip_transfer_metric(k, num / den)
    if not predicted:
        out["status"] = "error"
        out["error"] = "prediction_failed"
        return out

    impact_keys = ["bpm", "rapm", "net_pts"]
    pred_impact = [predicted[k] for k in impact_keys if k in predicted]
    impact_pct = None
    if pred_impact:
        pred_impact_score = sum(pred_impact) / len(pred_impact)
        grade_pool = same_dest if len(same_dest) >= 20 else pool
        cohort_scores: list[float] = []
        for e in grade_pool:
            vals = [e["dst"][k] for k in impact_keys if k in e["dst"]]
            if vals:
                cohort_scores.append(sum(float(v) for v in vals) / len(vals))
        if cohort_scores:
            impact_pct = bpc.percentile(pred_impact_score, cohort_scores)

    out["transfer_grade"] = bpc._transfer_grade_from_percentile(impact_pct)
    out["comps_weighted"] = str(len(weighted_examples))
    if "ppg" in predicted:
        out["proj_ppg"] = f"{predicted['ppg']:.1f}"
    if "rpg" in predicted:
        out["proj_rpg"] = f"{predicted['rpg']:.1f}"
    if "apg" in predicted:
        out["proj_apg"] = f"{predicted['apg']:.1f}"
    if "spg" in predicted:
        out["proj_spg"] = f"{predicted['spg']:.1f}"
    if "bpg" in predicted:
        out["proj_bpg"] = f"{predicted['bpg']:.1f}"
    if "fg_pct" in predicted:
        out["proj_fg_pct"] = f"{predicted['fg_pct']:.1f}"
    if "tp_pct" in predicted:
        out["proj_tp_pct"] = f"{predicted['tp_pct']:.1f}"
    if "ft_pct" in predicted:
        out["proj_ft_pct"] = f"{predicted['ft_pct']:.1f}"
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Batch-run transfer projection for all players in a season to one destination conference.",
    )
    ap.add_argument("--project-root", default=".", help="Repo root (default: current dir).")
    ap.add_argument(
        "--bt-csv",
        default="player_cards_pipeline/data/bt/bt_advstats_2019_2026.csv",
        help="BT advstats CSV path (relative to project-root unless absolute).",
    )
    ap.add_argument("--season", required=True, help="Target season (script season, e.g. 2026).")
    ap.add_argument("--destination-conference", required=True, help="Destination conference label.")
    ap.add_argument("--out-csv", required=True, help="Output CSV path.")
    ap.add_argument("--min-games", type=int, default=5, help="Minimum GP to include.")
    ap.add_argument("--team", default="", help="Optional team filter.")
    ap.add_argument("--limit", type=int, default=0, help="Optional cap on players (0 = no cap).")
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    bt_csv = Path(args.bt_csv)
    if not bt_csv.is_absolute():
        bt_csv = project_root / bt_csv
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

    filtered = []
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
        row = row_idx.get(key)
        if not row:
            continue
        gp = bpc.bt_num(row, ["GP", "gp"])
        if gp is not None and gp < args.min_games:
            continue
        filtered.append((p, row))

    filtered.sort(key=lambda x: (bpc.norm_team(x[0].team), bpc.norm_player_name(x[0].player)))
    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]

    print(f"[batch-transfer] eligible players: {len(filtered)}", flush=True)

    examples = build_transfer_examples(bpc, bt_rows)
    if len(examples) < 200:
        raise RuntimeError(f"Not enough historical transfer samples ({len(examples)}).")
    dest_conf = bpc._conference_key(args.destination_conference)
    same_dest = [e for e in examples if e["dst_conf"] == dest_conf]
    pool = same_dest if len(same_dest) >= 35 else examples
    print(
        f"[batch-transfer] examples={len(examples)} pool={len(pool)} same_dest={len(same_dest)} dest={args.destination_conference}",
        flush=True,
    )

    feat_keys = [
        "mpg", "ppg", "rpg", "apg", "spg", "bpg", "fg_pct", "tp_pct", "ft_pct",
        "bpm", "usg", "ts_per", "rim_pct", "ast_per", "ast_tov",
        "stl_per", "blk_per", "orb_per", "drb_per", "rapm", "net_pts", "onoff_net_rating",
    ]
    out_keys = feat_keys

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

    headers = [
        "season",
        "player",
        "team",
        "source_conference",
        "destination_conference",
        "status",
        "error",
        "transfer_grade",
        "comps_weighted",
        "proj_ppg",
        "proj_rpg",
        "proj_apg",
        "proj_spg",
        "proj_bpg",
        "proj_fg_pct",
        "proj_tp_pct",
        "proj_ft_pct",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        total = len(filtered)
        for idx, (p, row) in enumerate(filtered, start=1):
            source = bpc._row_transfer_metrics(row)
            source_conf = bpc._conference_key(bpc.bt_get(row, ["conf", "conference"]))
            parsed = project_player_transfer(
                bpc=bpc,
                source=source,
                source_conf=source_conf,
                pool=pool,
                same_dest=same_dest,
                dest_conf=dest_conf,
                feat_keys=feat_keys,
                out_keys=out_keys,
                scales=scales,
            )
            rec = {
                "season": bpc.norm_season(p.season),
                "player": p.player,
                "team": p.team,
                "source_conference": bpc.bt_get(row, ["conf", "conference"]),
                "destination_conference": args.destination_conference,
                **parsed,
            }
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

