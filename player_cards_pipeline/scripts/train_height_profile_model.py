#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Core BT features with strong height signal.
BT_FEATURE_SPECS: list[tuple[str, str]] = [
    ("usg", "usg"),
    ("orb_per", "ORB_per"),
    ("drb_per", "DRB_per"),
    ("ast_per", "AST_per"),
    ("to_per", "TO_per"),
    ("blk_per", "blk_per"),
    ("stl_per", "stl_per"),
    ("ftr", "ftr"),
    ("three_par", "3par"),
    ("rim_fg_pct", "rimmade/(rimmade+rimmiss)"),
    ("rim_att_pg", "rimmade+rimmiss"),
    ("dunk_fg_pct", "dunksmade/(dunksmade+dunksmiss)"),
    ("dunks_pg", "dunksmade"),
    ("dbpm", "dbpm"),
    ("adrtg", "adrtg"),
]

# Optional enriched features keyed by pid/season.
ENRICHED_FEATURE_KEYS: list[tuple[str, str]] = [
    ("he_off_ast_rim", "off_ast_rim"),
    ("he_off_assist", "off_assist"),
    ("he_off_usage", "off_usage"),
    ("he_off_2prim", "off_2prim"),
    ("he_off_2primr", "off_2primr"),
    ("he_off_2prim_ast", "off_2prim_ast"),
    ("he_off_team_poss_pct", "off_team_poss_pct"),
]


@dataclass
class Sample:
    season: int
    player_name: str
    team: str
    pid: str
    listed_height: str
    height_inches: float
    features: dict[str, float]


@dataclass
class RidgeModel:
    feature_names: list[str]
    mean_x: list[float]
    std_x: list[float]
    weights: list[float]
    bias: float


@dataclass
class Metrics:
    mae_inches: float
    rmse_inches: float
    r2: float


def to_float(v: Any, default: float = 0.0) -> float:
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
        pass
    return default


def parse_height_inches(height: str) -> float | None:
    s = (height or "").strip().lower()
    if not s:
        return None

    # 6-8, 6'8, 6 8
    m = re.match(r"^(\d+)\s*[-' ]\s*(\d+)$", s)
    if m:
        ft = int(m.group(1))
        inch = int(m.group(2))
        if 4 <= ft <= 8 and 0 <= inch <= 11:
            return float((ft * 12) + inch)

    # 80 (inches)
    if re.match(r"^\d{2,3}(\.\d+)?$", s):
        x = float(s)
        if 50 <= x <= 100:
            return x

    return None


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def nested_value(obj: Any, default: float = 0.0) -> float:
    if isinstance(obj, dict):
        if "value" in obj:
            return to_float(obj.get("value"), default)
        return default
    return to_float(obj, default)


def load_enriched_map(enriched_dir: Path, seasons: set[int]) -> dict[tuple[int, str], dict[str, float]]:
    out: dict[tuple[int, str], dict[str, float]] = {}
    if not enriched_dir.exists():
        return out

    for season in sorted(seasons):
        matches = sorted(enriched_dir.glob(f"players_all_Women_scriptSeason_{season}_fromJsonYear_*.json"))
        if not matches:
            continue
        path = matches[0]
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        players = payload.get("players", []) if isinstance(payload, dict) else []
        for row in players:
            pid_raw = str(row.get("_id", "")).split("_", 1)[0].strip()
            if not pid_raw:
                continue
            feats: dict[str, float] = {}
            for out_key, src_key in ENRICHED_FEATURE_KEYS:
                feats[out_key] = nested_value(row.get(src_key), 0.0)
            out[(season, pid_raw)] = feats
    return out


def safe_div(a: float, b: float) -> float:
    return 0.0 if abs(b) < 1e-9 else a / b


def pearson(xs: list[float], ys: list[float]) -> float:
    n = min(len(xs), len(ys))
    if n < 3:
        return 0.0
    mx = sum(xs[:n]) / n
    my = sum(ys[:n]) / n
    num = 0.0
    dx2 = 0.0
    dy2 = 0.0
    for i in range(n):
        dx = xs[i] - mx
        dy = ys[i] - my
        num += dx * dy
        dx2 += dx * dx
        dy2 += dy * dy
    den = math.sqrt(dx2 * dy2)
    return 0.0 if den <= 1e-12 else num / den


def mean_std(col: list[float]) -> tuple[float, float]:
    if not col:
        return 0.0, 1.0
    m = sum(col) / len(col)
    var = sum((x - m) ** 2 for x in col) / len(col)
    sd = math.sqrt(var)
    return m, (sd if sd > 1e-12 else 1.0)


def train_ridge(X: list[list[float]], y: list[float], lam: float, iters: int, lr: float) -> RidgeModel:
    n = len(X)
    d = len(X[0]) if n else 0
    mean_x = [0.0] * d
    std_x = [1.0] * d

    for j in range(d):
        col = [X[i][j] for i in range(n)]
        m, s = mean_std(col)
        mean_x[j] = m
        std_x[j] = s

    Xn = [[(X[i][j] - mean_x[j]) / std_x[j] for j in range(d)] for i in range(n)]

    bias = sum(y) / max(1, len(y))
    w = [0.0] * d
    for _ in range(iters):
        grad_w = [0.0] * d
        grad_b = 0.0
        for i in range(n):
            pred = bias + sum(w[j] * Xn[i][j] for j in range(d))
            err = pred - y[i]
            grad_b += err
            for j in range(d):
                grad_w[j] += err * Xn[i][j]

        scale = 2.0 / max(1, n)
        grad_b *= scale
        for j in range(d):
            grad_w[j] = (grad_w[j] * scale) + (2.0 * lam * w[j])
            w[j] -= lr * grad_w[j]
        bias -= lr * grad_b

    return RidgeModel(feature_names=[], mean_x=mean_x, std_x=std_x, weights=w, bias=bias)


def predict_row(model: RidgeModel, row: list[float]) -> float:
    z = [(row[j] - model.mean_x[j]) / model.std_x[j] for j in range(len(row))]
    return model.bias + sum(model.weights[j] * z[j] for j in range(len(row)))


def eval_metrics(model: RidgeModel, X: list[list[float]], y: list[float]) -> Metrics:
    if not X:
        return Metrics(mae_inches=0.0, rmse_inches=0.0, r2=0.0)
    preds = [predict_row(model, r) for r in X]
    n = len(preds)
    mae = sum(abs(preds[i] - y[i]) for i in range(n)) / n
    rmse = math.sqrt(sum((preds[i] - y[i]) ** 2 for i in range(n)) / n)
    y_mean = sum(y) / n
    ss_tot = sum((yy - y_mean) ** 2 for yy in y)
    ss_res = sum((preds[i] - y[i]) ** 2 for i in range(n))
    r2 = 0.0 if ss_tot <= 1e-12 else 1.0 - (ss_res / ss_tot)
    return Metrics(mae_inches=mae, rmse_inches=rmse, r2=r2)


def inches_to_height_str(x: float) -> str:
    if not math.isfinite(x):
        return "N/A"
    rounded = int(round(x))
    ft = rounded // 12
    inch = rounded % 12
    return f"{ft}'{inch}\""


def build_samples(
    bt_rows: list[dict[str, str]],
    enriched_map: dict[tuple[int, str], dict[str, float]],
    min_mp: float,
    min_g: float,
    seasons: set[int],
    min_height_inches: float,
    max_height_inches: float,
) -> list[Sample]:
    out: list[Sample] = []
    for r in bt_rows:
        season = int(to_float(r.get("year"), 0))
        if season not in seasons:
            continue

        mp = to_float(r.get("mp"), 0.0)
        g = to_float(r.get("g"), to_float(r.get("GP"), 0.0))
        if mp < min_mp or g < min_g:
            continue

        h = parse_height_inches(str(r.get("ht", "")))
        if h is None:
            continue
        if h < min_height_inches or h > max_height_inches:
            continue

        feats: dict[str, float] = {}
        for out_key, src_key in BT_FEATURE_SPECS:
            feats[out_key] = to_float(r.get(src_key), 0.0)

        pid = str(r.get("pid", "")).strip()
        em = enriched_map.get((season, pid), {}) if pid else {}
        for out_key, _src in ENRICHED_FEATURE_KEYS:
            feats[out_key] = to_float(em.get(out_key), 0.0)

        out.append(
            Sample(
                season=season,
                player_name=str(r.get("player_name", "")).strip(),
                team=str(r.get("team", "")).strip(),
                pid=pid,
                listed_height=str(r.get("ht", "")).strip(),
                height_inches=h,
                features=feats,
            )
        )
    return out


def dependency_rows(samples: list[Sample], model: RidgeModel, feature_names: list[str]) -> list[dict[str, Any]]:
    ys = [s.height_inches for s in samples]
    rows: list[dict[str, Any]] = []
    for j, feat in enumerate(feature_names):
        xs = [s.features.get(feat, 0.0) for s in samples]
        corr = pearson(xs, ys)
        rows.append(
            {
                "feature": feat,
                "pearson_with_height": corr,
                "abs_pearson": abs(corr),
                "std_weight": model.weights[j],
                "abs_std_weight": abs(model.weights[j]),
            }
        )
    rows.sort(key=lambda r: (r["abs_std_weight"], r["abs_pearson"]), reverse=True)
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    ap = argparse.ArgumentParser(description="Train player height profile model and score players by 'plays above/below listed height'.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--bt-csv", default="player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv")
    ap.add_argument(
        "--enriched-script-dir",
        default="player_cards_pipeline/data/manual/enriched_players/by_script_season",
    )
    ap.add_argument("--train-min-season", type=int, default=2019)
    ap.add_argument("--train-max-season", type=int, default=2025)
    ap.add_argument("--holdout-seasons", default="2025")
    ap.add_argument("--score-season", type=int, default=2026)
    ap.add_argument("--min-mp", type=float, default=8.0)
    ap.add_argument("--min-g", type=float, default=8.0)
    ap.add_argument("--min-height-inches", type=float, default=68.0)
    ap.add_argument("--max-height-inches", type=float, default=88.0)
    ap.add_argument("--ridge-lambda", type=float, default=0.08)
    ap.add_argument("--iters", type=int, default=520)
    ap.add_argument("--lr", type=float, default=0.05)
    ap.add_argument("--out-model-json", default="player_cards_pipeline/data/models/height_profile_model_wncaaw_v1.json")
    ap.add_argument("--out-score-csv", default="player_cards_pipeline/output/height_profile_scores_2026.csv")
    ap.add_argument("--out-dependency-csv", default="player_cards_pipeline/output/height_profile_feature_dependency.csv")
    ap.add_argument("--out-report-json", default="player_cards_pipeline/output/height_profile_report.json")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    bt_csv = (root / args.bt_csv).resolve() if not Path(args.bt_csv).is_absolute() else Path(args.bt_csv)
    enriched_dir = (root / args.enriched_script_dir).resolve() if not Path(args.enriched_script_dir).is_absolute() else Path(args.enriched_script_dir)
    out_model_json = (root / args.out_model_json).resolve() if not Path(args.out_model_json).is_absolute() else Path(args.out_model_json)
    out_score_csv = (root / args.out_score_csv).resolve() if not Path(args.out_score_csv).is_absolute() else Path(args.out_score_csv)
    out_dependency_csv = (root / args.out_dependency_csv).resolve() if not Path(args.out_dependency_csv).is_absolute() else Path(args.out_dependency_csv)
    out_report_json = (root / args.out_report_json).resolve() if not Path(args.out_report_json).is_absolute() else Path(args.out_report_json)

    holdout_seasons = {
        int(s.strip())
        for s in str(args.holdout_seasons).split(",")
        if s.strip()
    }

    train_seasons = set(range(args.train_min_season, args.train_max_season + 1))
    all_needed = set(train_seasons)
    all_needed.add(int(args.score_season))

    bt_rows = read_csv_rows(bt_csv)
    enriched_map = load_enriched_map(enriched_dir, all_needed)
    all_samples = build_samples(
        bt_rows,
        enriched_map,
        args.min_mp,
        args.min_g,
        all_needed,
        args.min_height_inches,
        args.max_height_inches,
    )

    if len(all_samples) < 300:
        raise SystemExit(f"Not enough valid samples ({len(all_samples)}). Lower --min-mp/--min-g or widen seasons.")

    feature_names = [k for k, _ in BT_FEATURE_SPECS] + [k for k, _ in ENRICHED_FEATURE_KEYS]

    train_samples = [s for s in all_samples if s.season in train_seasons and s.season not in holdout_seasons]
    test_samples = [s for s in all_samples if s.season in holdout_seasons]

    if len(train_samples) < 250:
        raise SystemExit(f"Not enough training samples ({len(train_samples)}).")

    X_train = [[s.features.get(f, 0.0) for f in feature_names] for s in train_samples]
    y_train = [s.height_inches for s in train_samples]
    model = train_ridge(X_train, y_train, lam=args.ridge_lambda, iters=args.iters, lr=args.lr)
    model.feature_names = feature_names

    train_m = eval_metrics(model, X_train, y_train)

    X_test = [[s.features.get(f, 0.0) for f in feature_names] for s in test_samples]
    y_test = [s.height_inches for s in test_samples]
    test_m = eval_metrics(model, X_test, y_test) if test_samples else Metrics(mae_inches=0.0, rmse_inches=0.0, r2=0.0)

    score_samples = [s for s in all_samples if s.season == int(args.score_season)]
    score_rows: list[dict[str, Any]] = []
    for s in score_samples:
        x = [s.features.get(f, 0.0) for f in feature_names]
        pred_h = predict_row(model, x)
        delta = pred_h - s.height_inches
        if delta >= 0.75:
            label = "plays_taller"
        elif delta <= -0.75:
            label = "plays_shorter"
        else:
            label = "plays_at_height"
        score_rows.append(
            {
                "season": s.season,
                "player_name": s.player_name,
                "team": s.team,
                "pid": s.pid,
                "listed_height_raw": s.listed_height,
                "listed_height": inches_to_height_str(s.height_inches),
                "listed_height_inches": round(s.height_inches, 2),
                "predicted_profile_height_inches": round(pred_h, 2),
                "predicted_profile_height": inches_to_height_str(pred_h),
                "height_delta_inches": round(delta, 2),
                "height_profile_label": label,
            }
        )

    dep_rows = dependency_rows(train_samples, model, feature_names)

    out_model_json.parent.mkdir(parents=True, exist_ok=True)
    out_model_json.write_text(
        json.dumps(
            {
                "model": "height_profile_ridge_v1",
                "feature_names": feature_names,
                "mean_x": model.mean_x,
                "std_x": model.std_x,
                "weights": model.weights,
                "bias": model.bias,
                "train_seasons": sorted(train_seasons),
                "holdout_seasons": sorted(holdout_seasons),
                "train_rows": len(train_samples),
                "test_rows": len(test_samples),
                "train_metrics": {
                    "mae_inches": train_m.mae_inches,
                    "rmse_inches": train_m.rmse_inches,
                    "r2": train_m.r2,
                },
                "test_metrics": {
                    "mae_inches": test_m.mae_inches,
                    "rmse_inches": test_m.rmse_inches,
                    "r2": test_m.r2,
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    write_csv(
        out_score_csv,
        score_rows,
        [
            "season",
            "player_name",
            "team",
            "pid",
            "listed_height_raw",
            "listed_height",
            "listed_height_inches",
            "predicted_profile_height_inches",
            "predicted_profile_height",
            "height_delta_inches",
            "height_profile_label",
        ],
    )

    write_csv(
        out_dependency_csv,
        dep_rows,
        ["feature", "pearson_with_height", "abs_pearson", "std_weight", "abs_std_weight"],
    )

    out_report_json.parent.mkdir(parents=True, exist_ok=True)
    out_report_json.write_text(
        json.dumps(
            {
                "train_rows": len(train_samples),
                "test_rows": len(test_samples),
                "score_rows": len(score_rows),
                "train_metrics": {
                    "mae_inches": round(train_m.mae_inches, 4),
                    "rmse_inches": round(train_m.rmse_inches, 4),
                    "r2": round(train_m.r2, 4),
                },
                "test_metrics": {
                    "mae_inches": round(test_m.mae_inches, 4),
                    "rmse_inches": round(test_m.rmse_inches, 4),
                    "r2": round(test_m.r2, 4),
                },
                "top_height_dependence": dep_rows[:12],
                "notes": [
                    "Positive height_delta_inches means a player statistically profiles as taller than listed height.",
                    "Negative height_delta_inches means a player statistically profiles as shorter than listed height.",
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"[height-profile] train_rows={len(train_samples)} test_rows={len(test_samples)} score_rows={len(score_rows)}")
    print(
        f"[height-profile] test MAE={test_m.mae_inches:.2f} in | RMSE={test_m.rmse_inches:.2f} in | R2={test_m.r2:.3f}"
        if test_samples
        else "[height-profile] no holdout seasons in sample"
    )
    print(f"[height-profile] model -> {out_model_json}")
    print(f"[height-profile] score csv -> {out_score_csv}")
    print(f"[height-profile] dependence csv -> {out_dependency_csv}")
    print(f"[height-profile] report -> {out_report_json}")


if __name__ == "__main__":
    main()
