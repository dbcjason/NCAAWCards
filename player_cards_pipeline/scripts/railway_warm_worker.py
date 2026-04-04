#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc

SECTION_PHASES = [
    "per_game_percentiles",
    "grade_boxes_html",
    "bt_percentiles_html",
    "self_creation_html",
    "playstyles_html",
    "team_impact_html",
    "shot_diet_html",
    "player_comparisons_html",
    "draft_projection_html",
]

DEFAULT_GENDER = "women"
SYNC_STATE_PREFIX = ".payload_sync_state_"


def load_settings(project_root: Path) -> dict[str, Any]:
    p = project_root / "player_cards_pipeline" / "config" / "settings.json"
    return json.loads(p.read_text(encoding="utf-8"))


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw.strip())


def output_dir_path() -> Path:
    rel = os.getenv("OUT_DIR", "player_cards_pipeline/public/cards").strip() or "player_cards_pipeline/public/cards"
    return PROJECT_ROOT / rel


def normalize_sync_endpoint(endpoint: str) -> str:
    normalized = endpoint.strip().rstrip("/")
    if normalized == "https://dbcjason.com":
        return "https://www.dbcjason.com"
    if normalized.startswith("https://dbcjason.com/"):
        suffix = normalized[len("https://dbcjason.com") :]
        return f"https://www.dbcjason.com{suffix}"
    return normalized


def parse_sync_state(out_dir: Path, gender: str) -> dict[str, Any]:
    path = out_dir / f"{SYNC_STATE_PREFIX}{gender}.json"
    if not path.exists():
        return {"years": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            years = data.get("years")
            if isinstance(years, dict):
                return data
    except Exception:
        pass
    return {"years": {}}


def save_sync_state(out_dir: Path, gender: str, state: dict[str, Any]) -> None:
    path = out_dir / f"{SYNC_STATE_PREFIX}{gender}.json"
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def update_sync_state(out_dir: Path, gender: str, year: str, status: str, **extra: Any) -> None:
    state = parse_sync_state(out_dir, gender)
    years = state.setdefault("years", {})
    if not isinstance(years, dict):
        years = {}
        state["years"] = years
    payload: dict[str, Any] = {
        "status": status,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    payload.update(extra)
    years[str(year)] = payload
    save_sync_state(out_dir, gender, state)


def idle_forever(reason: str) -> None:
    heartbeat_seconds = max(60, env_int("IDLE_HEARTBEAT_SECONDS", 900))
    print(f"[railway-worker] idle mode reason={reason} heartbeat_seconds={heartbeat_seconds}")
    while True:
        print(f"[railway-worker] idle heartbeat reason={reason}")
        time.sleep(heartbeat_seconds)


def cleanup_output_years(out_dir: Path, years: list[str]) -> None:
    for year in years:
        year_dir = out_dir / str(year)
        if not year_dir.exists():
            continue
        shutil.rmtree(year_dir, ignore_errors=True)
        print(f"[railway-worker] cleaned local outputs year={year}")


def existing_merged_years(out_dir: Path, years: list[str]) -> list[str]:
    present: list[str] = []
    for year in years:
        year_dir = out_dir / year
        if (year_dir / "index.json").exists() and (year_dir / "manifest.json").exists():
            present.append(year)
    return present


def pending_sync_years(out_dir: Path, years: list[str], gender: str) -> list[str]:
    state = parse_sync_state(out_dir, gender)
    years_state = state.get("years", {}) if isinstance(state, dict) else {}
    pending: list[str] = []
    for year in existing_merged_years(out_dir, years):
        year_state = years_state.get(str(year), {}) if isinstance(years_state, dict) else {}
        if not isinstance(year_state, dict) or year_state.get("status") != "synced":
            pending.append(year)
    return pending


def synced_merged_years(out_dir: Path, years: list[str], gender: str) -> list[str]:
    state = parse_sync_state(out_dir, gender)
    years_state = state.get("years", {}) if isinstance(state, dict) else {}
    synced: list[str] = []
    for year in existing_merged_years(out_dir, years):
        year_state = years_state.get(str(year), {}) if isinstance(years_state, dict) else {}
        if isinstance(year_state, dict) and year_state.get("status") == "synced":
            synced.append(year)
    return synced


def build_shard_command(chunk_index: int, chunk_count: int, phases: list[str] | None = None) -> list[str]:
    years = os.getenv("YEARS", os.getenv("WARM_SEASON", "2026")).strip() or "2026"
    out_dir = os.getenv("OUT_DIR", "player_cards_pipeline/public/cards").strip() or "player_cards_pipeline/public/cards"
    cmd = [
        sys.executable,
        "player_cards_pipeline/scripts/build_static_card_payloads.py",
        "--project-root",
        ".",
        "--out-dir",
        out_dir,
        "--years",
        years,
        "--min-games",
        str(env_int("MIN_GAMES", 5)),
        "--chunk-count",
        str(chunk_count),
        "--chunk-index",
        str(chunk_index),
        "--checkpoint-every",
        str(env_int("CHECKPOINT_EVERY", 25)),
    ]
    if env_flag("INCREMENTAL", True):
        cmd.append("--incremental")
    if env_flag("WRITE_SHARD_FILES", True):
        cmd.append("--write-shard-files")
    limit = env_int("LIMIT", 0)
    if limit > 0:
        cmd.extend(["--limit", str(limit)])
    targets_file = os.getenv("TARGETS_FILE", "").strip()
    if targets_file:
        cmd.extend(["--targets-file", targets_file])
    if phases:
        cmd.extend(["--phases", ",".join(phases)])
    return cmd


def build_merge_command(chunk_count: int) -> list[str]:
    years = os.getenv("YEARS", os.getenv("WARM_SEASON", "2026")).strip() or "2026"
    out_dir = os.getenv("OUT_DIR", "player_cards_pipeline/public/cards").strip() or "player_cards_pipeline/public/cards"
    return [
        sys.executable,
        "player_cards_pipeline/scripts/build_static_card_payloads.py",
        "--project-root",
        ".",
        "--out-dir",
        out_dir,
        "--years",
        years,
        "--chunk-count",
        str(chunk_count),
        "--merge-shards",
    ]


def parse_years(spec: str) -> list[str]:
    out: set[str] = set()
    for part in (spec or "").split(","):
        p = part.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            start = int(a)
            end = int(b)
            step = 1 if end >= start else -1
            for year in range(start, end + step, step):
                out.add(str(year))
        else:
            out.add(str(int(p)))
    return sorted(out)


def merge_outputs(chunk_count: int) -> None:
    cmd = build_merge_command(chunk_count)
    print(f"[railway-worker] merge start: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=PROJECT_ROOT, text=True, check=True)
    print("[railway-worker] merge finished successfully")


def mark_merged_outputs(default_gender: str) -> None:
    out_dir = output_dir_path()
    years = parse_years(os.getenv("YEARS", os.getenv("WARM_SEASON", "2026")).strip() or "2026")
    gender = os.getenv("PAYLOAD_SYNC_GENDER", default_gender).strip().lower() or default_gender

    for year in years:
        year_dir = out_dir / year
        index_path = year_dir / "index.json"
        manifest_path = year_dir / "manifest.json"
        if not index_path.exists() or not manifest_path.exists():
            continue
        try:
            rows = json.loads(index_path.read_text(encoding="utf-8"))
            row_count = len(rows) if isinstance(rows, list) else 0
        except Exception:
            row_count = 0
        update_sync_state(out_dir, gender, year, "merged", rows=row_count)
        print(f"[railway-worker] preserved merged outputs year={year} rows={row_count}")


def sync_outputs_to_endpoint(default_gender: str) -> None:
    endpoint = normalize_sync_endpoint(os.getenv("PAYLOAD_SYNC_ENDPOINT", ""))
    token = os.getenv("PAYLOAD_SYNC_TOKEN", "").strip()
    if not endpoint or not token:
        print("[railway-worker] sync skipped: PAYLOAD_SYNC_ENDPOINT or PAYLOAD_SYNC_TOKEN missing")
        return

    out_dir = output_dir_path()
    years = parse_years(os.getenv("YEARS", os.getenv("WARM_SEASON", "2026")).strip() or "2026")
    gender = os.getenv("PAYLOAD_SYNC_GENDER", default_gender).strip().lower() or default_gender
    batch_size = max(1, env_int("PAYLOAD_SYNC_BATCH_SIZE", 50))
    timeout_seconds = max(5, env_int("PAYLOAD_SYNC_TIMEOUT_SECONDS", 120))
    print(f"[railway-worker] sync start endpoint={endpoint} years={','.join(years)} gender={gender}")

    total_rows = 0
    for year in years:
        year_dir = out_dir / year
        index_path = year_dir / "index.json"
        manifest_path = year_dir / "manifest.json"
        if not index_path.exists() or not manifest_path.exists():
            print(f"[railway-worker] sync skipped year={year}: merged index/manifest missing")
            continue

        try:
            index_rows = json.loads(index_path.read_text(encoding="utf-8"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"sync failed reading merged outputs for year={year}: {exc}") from exc

        if not isinstance(index_rows, list):
            print(f"[railway-worker] sync skipped year={year}: index.json is not a list")
            continue

        batch: list[dict[str, Any]] = []
        for row in index_rows:
            if not isinstance(row, dict):
                continue
            rel_path = str(row.get("path", "")).strip()
            if not rel_path:
                continue
            payload_path = year_dir / rel_path
            if not payload_path.exists():
                continue
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            batch.append(
                {
                    "gender": gender,
                    "season": int(row.get("season") or year),
                    "team": str(row.get("team", "")),
                    "player": str(row.get("player", "")),
                    "cache_key": str(row.get("cache_key", "")),
                    "source_hash": str(manifest.get(str(row.get("cache_key", "")), row.get("source_hash", ""))),
                    "path": rel_path,
                    "payload_json": payload,
                }
            )
            if len(batch) >= batch_size:
                post_sync_batch(endpoint, token, batch, timeout_seconds)
                total_rows += len(batch)
                print(f"[railway-worker] sync year={year} rows={total_rows}")
                batch = []
        if batch:
            post_sync_batch(endpoint, token, batch, timeout_seconds)
            total_rows += len(batch)
            print(f"[railway-worker] sync year={year} rows={total_rows}")
        update_sync_state(out_dir, gender, year, "synced", rows=len(index_rows), endpoint=endpoint)

    print(f"[railway-worker] sync finished successfully rows={total_rows}")


def post_sync_batch(endpoint: str, token: str, rows: list[dict[str, Any]], timeout_seconds: int) -> None:
    data = json.dumps({"rows": rows}).encode("utf-8")
    current_endpoint = endpoint
    max_redirects = max(0, env_int("PAYLOAD_SYNC_MAX_REDIRECTS", 5))

    for _attempt in range(max_redirects + 1):
        req = urllib.request.Request(
            current_endpoint,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                if resp.status >= 400:
                    raise RuntimeError(f"sync failed status={resp.status} body={body[:500]}")
                return
        except urllib.error.HTTPError as exc:
            if exc.code in {301, 302, 303, 307, 308}:
                location = exc.headers.get("Location", "").strip()
                if not location:
                    body = exc.read().decode("utf-8", errors="replace")
                    raise RuntimeError(f"sync redirect missing location status={exc.code} body={body[:500]}") from exc
                redirected = normalize_sync_endpoint(urllib.parse.urljoin(current_endpoint, location))
                print(f"[railway-worker] sync redirect status={exc.code} to={redirected}")
                current_endpoint = redirected
                continue
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"sync failed status={exc.code} body={body[:500]}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"sync failed url={current_endpoint} error={exc}") from exc

    raise RuntimeError(f"sync failed: too many redirects starting from {endpoint}")


def run_phase_wave(chunk_count: int, max_parallel: int, phases: list[str]) -> None:
    pending = list(range(chunk_count))
    running: list[tuple[int, subprocess.Popen[str]]] = []
    failures: list[int] = []
    phase_label = ",".join(phases)
    print(f"[railway-worker] phase wave start phases={phase_label} chunk_count={chunk_count} max_parallel={max_parallel}")

    while pending or running:
        while pending and len(running) < max_parallel:
            chunk_index = pending.pop(0)
            cmd = build_shard_command(chunk_index, chunk_count, phases=phases)
            print(f"[railway-worker] start shard {chunk_index + 1}/{chunk_count} phases={phase_label}: {' '.join(cmd)}")
            proc = subprocess.Popen(cmd, cwd=PROJECT_ROOT, text=True)
            running.append((chunk_index, proc))

        time.sleep(5)
        next_running: list[tuple[int, subprocess.Popen[str]]] = []
        for chunk_index, proc in running:
            code = proc.poll()
            if code is None:
                next_running.append((chunk_index, proc))
                continue
            if code == 0:
                print(f"[railway-worker] shard {chunk_index + 1}/{chunk_count} phases={phase_label} finished successfully")
            else:
                print(f"[railway-worker] shard {chunk_index + 1}/{chunk_count} phases={phase_label} failed with exit code {code}")
                failures.append(chunk_index)
        running = next_running

    if failures:
        raise SystemExit(f"phase wave failed phases={phase_label}: {failures}")

    print(f"[railway-worker] phase wave finished phases={phase_label}")


def run_bootstrap_worker() -> None:
    chunk_count = max(1, env_int("CHUNK_COUNT", 10))
    max_parallel = max(1, env_int("MAX_PARALLEL_SHARDS", chunk_count))
    years = os.getenv("YEARS", os.getenv("WARM_SEASON", "2026"))
    section_major = env_flag("SECTION_MAJOR_BOOTSTRAP", True)
    out_dir = output_dir_path()
    parsed_years = parse_years(years.strip() or "2026")
    gender = os.getenv("PAYLOAD_SYNC_GENDER", DEFAULT_GENDER).strip().lower() or DEFAULT_GENDER

    print(
        f"[railway-worker] bootstrap mode chunk_count={chunk_count} "
        f"max_parallel={max_parallel} years={years} section_major={str(section_major).lower()}"
    )

    pending_years = pending_sync_years(out_dir, parsed_years, gender)
    if pending_years:
        print(f"[railway-worker] found existing merged outputs awaiting sync years={','.join(pending_years)}")
        try:
            sync_outputs_to_endpoint(DEFAULT_GENDER)
            if env_flag("CLEANUP_AFTER_SYNC", True):
                cleanup_output_years(out_dir, pending_years)
            print("[railway-worker] recovered existing merged outputs via sync")
            print("[railway-worker] bootstrap mode finished successfully")
            if env_flag("IDLE_AFTER_BOOTSTRAP", True):
                idle_forever("bootstrap finished successfully")
            return
        except Exception as exc:
            print(f"[railway-worker] sync recovery failed: {exc}")
            if env_flag("IDLE_AFTER_SYNC_FAILURE", True):
                idle_forever("sync failed; merged outputs preserved for retry")
            raise

    synced_years = synced_merged_years(out_dir, parsed_years, gender)
    if parsed_years and set(synced_years) == set(parsed_years):
        print(f"[railway-worker] synced merged outputs already present years={','.join(synced_years)}; skipping rebuild")
        if env_flag("IDLE_AFTER_BOOTSTRAP", True):
            idle_forever("bootstrap already synced")
        return

    if section_major:
        run_phase_wave(chunk_count, max_parallel, ["base_metadata"])
        for section_name in SECTION_PHASES:
            run_phase_wave(chunk_count, max_parallel, [section_name])
        run_phase_wave(chunk_count, max_parallel, ["finalize"])
    else:
        run_phase_wave(chunk_count, max_parallel, [])

    merge_outputs(chunk_count)
    mark_merged_outputs(DEFAULT_GENDER)
    try:
        sync_outputs_to_endpoint(DEFAULT_GENDER)
        if env_flag("CLEANUP_AFTER_SYNC", True):
            cleanup_output_years(out_dir, parsed_years)
    except Exception as exc:
        print(f"[railway-worker] sync failed after merge: {exc}")
        if env_flag("IDLE_AFTER_SYNC_FAILURE", True):
            idle_forever("sync failed; merged outputs preserved for retry")
        raise

    print("[railway-worker] bootstrap mode finished successfully")
    if env_flag("IDLE_AFTER_BOOTSTRAP", True):
        idle_forever("bootstrap finished successfully")


def main() -> None:
    worker_mode = os.getenv("WORKER_MODE", "warm").strip().lower()
    if worker_mode == "bootstrap":
        run_bootstrap_worker()
        return

    heartbeat_seconds = int(os.getenv("HEARTBEAT_SECONDS", "300"))
    settings = load_settings(PROJECT_ROOT)
    bt_csv = rel_to_pipeline(PROJECT_ROOT, settings["bt_advstats_csv"])
    _header, bt_rows = bpc.read_csv_rows(bt_csv)
    print(f"[railway-worker] loaded {len(bt_rows)} BT rows")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    players_all = bpc.build_player_pool_from_bt(bt_rows)
    print(f"[railway-worker] built player pool with {len(players_all)} players")

    season = os.getenv("WARM_SEASON", "2026").strip()
    warm_players = [p for p in players_all if bpc.norm_season(p.season) == bpc.norm_season(season)]
    print(f"[railway-worker] warm season {season}: {len(warm_players)} players available")

    while True:
        print(f"[railway-worker] heartbeat season={season} players={len(warm_players)}")
        time.sleep(heartbeat_seconds)


if __name__ == "__main__":
    main()
