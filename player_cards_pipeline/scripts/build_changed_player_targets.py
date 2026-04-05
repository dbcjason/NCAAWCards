#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import difflib
import hashlib
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc

BASE_URL = "https://api.collegebasketballdata.com"
DEFAULT_API_KEY = "SXMeiEWTsy0KNablQhQVQBL7LhVcVnubACJUUAoeT/xWHKo+kV0fAxjAjaHEc6Ph"


def log(message: str) -> None:
    print(message, flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def norm(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def alias_variants(name: str) -> list[str]:
    out = {name.strip(), name.strip().replace(".", "")}
    s = name.strip()
    rules = [
        (" St.", " State"),
        ("Cal St.", "Cal State"),
        ("FIU", "Florida International"),
        ("LIU", "LIU Brooklyn"),
        ("Albany", "UAlbany"),
        ("American", "American University"),
        ("Appalachian St.", "App State"),
        ("Illinois Chicago", "UIC"),
        ("IU Indy", "IU Indianapolis"),
        ("Loyola MD", "Loyola Maryland"),
        ("Miami FL", "Miami"),
        ("Mississippi", "Ole Miss"),
        ("Nebraska Omaha", "Omaha"),
        ("Penn", "Pennsylvania"),
        ("Saint Francis", "St. Francis (PA)"),
        ("Southeastern Louisiana", "SE Louisiana"),
        ("Tennessee Martin", "UT Martin"),
        ("USC Upstate", "South Carolina Upstate"),
        ("Seattle", "Seattle U"),
        ("Queens", "Queens University"),
        ("San Jose St.", "San Jose State"),
        ("Louisiana Monroe", "UL Monroe"),
        ("Sam Houston St.", "Sam Houston"),
        ("Nicholls St.", "Nicholls"),
        ("McNeese St.", "McNeese"),
        ("Grambling St.", "Grambling"),
        ("Texas A&M Corpus Chris", "Texas A&M-Corpus Christi"),
        ("Long Beach St.", "Long Beach State"),
        ("Cal Baptist", "California Baptist"),
        ("Connecticut", "UConn"),
        ("UMKC", "Kansas City"),
    ]
    for a, b in rules:
        if a in s:
            out.add(s.replace(a, b))
    if s.startswith("Saint "):
        out.add(s.replace("Saint ", "St. "))
    if s.startswith("St. "):
        out.add(s.replace("St. ", "Saint "))
    return [item for item in out if item]


class Client:
    def __init__(
        self,
        api_key: str,
        sleep_sec: float = 0.1,
        timeout_sec: int = 60,
        max_requests: int = 1000,
        cache_dir: Path | None = None,
    ) -> None:
        self.api_key = api_key
        self.sleep_sec = sleep_sec
        self.timeout_sec = timeout_sec
        self.max_requests = max_requests
        self.cache_dir = cache_dir
        self.request_count = 0
        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _normalized_params(params: dict[str, Any]) -> dict[str, Any]:
        return {key: params[key] for key in sorted(params.keys()) if params[key] not in (None, "")}

    def _cache_path(self, path: str, params: dict[str, Any]) -> Path | None:
        if self.cache_dir is None:
            return None
        payload = {"path": path, "params": self._normalized_params(params)}
        digest = hashlib.sha1(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def get(self, path: str, params: dict[str, Any]) -> tuple[int, Any]:
        cache_path = self._cache_path(path, params)
        if cache_path is not None and cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
                return int(cached.get("status", 0)), cached.get("body")
            except Exception:
                pass

        if self.request_count >= self.max_requests:
            raise RuntimeError(f"Request budget exceeded (max_requests={self.max_requests})")

        norm_params = self._normalized_params(params)
        query = urlencode(norm_params)
        url = f"{BASE_URL}{path}" + (f"?{query}" if query else "")
        req = Request(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "User-Agent": "dbcjason-changed-player-targets/1.0",
            },
            method="GET",
        )
        status = 0
        body: Any = None
        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                status = int(resp.status)
                raw = resp.read().decode("utf-8", errors="replace")
                body = json.loads(raw) if raw else None
        except HTTPError as exc:
            status = int(exc.code)
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(raw) if raw else None
            except Exception:
                body = {"error_text": raw[:5000]}
        except URLError as exc:
            body = {"error": str(exc)}
        except Exception as exc:  # noqa: BLE001
            body = {"error": str(exc)}
        finally:
            self.request_count += 1
            if cache_path is not None:
                try:
                    cache_path.write_text(
                        json.dumps(
                            {
                                "status": status,
                                "body": body,
                                "cached_utc": utc_now(),
                                "path": path,
                                "params": norm_params,
                            },
                            ensure_ascii=True,
                        ),
                        encoding="utf-8",
                    )
                except Exception:
                    pass
            time.sleep(self.sleep_sec)
        return status, body


def to_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "items", "rows", "games"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    return []


def extract_team_names(game: dict[str, Any]) -> list[str]:
    values: list[str] = []
    candidates = [
        game.get("homeTeam"),
        game.get("awayTeam"),
        game.get("home_team"),
        game.get("away_team"),
        game.get("team"),
        game.get("school"),
        game.get("home"),
        game.get("away"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            values.append(candidate.strip())
        elif isinstance(candidate, dict):
            for key in ("school", "team", "name", "displayName"):
                inner = candidate.get(key)
                if isinstance(inner, str) and inner.strip():
                    values.append(inner.strip())
    return values


def load_settings(project_root: Path) -> dict[str, Any]:
    return json.loads((project_root / "player_cards_pipeline" / "config" / "settings.json").read_text(encoding="utf-8"))


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def load_local_players(project_root: Path, season: str) -> tuple[list[bpc.PlayerGameStats], list[str]]:
    settings = load_settings(project_root)
    bt_csv = rel_to_pipeline(project_root, settings["bt_advstats_csv"])
    _header, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    players_all = bpc.build_player_pool_from_bt(bt_rows)
    season_key = bpc.norm_season(season)
    season_players = [player for player in players_all if bpc.norm_season(player.season) == season_key]
    teams = sorted({player.team.strip() for player in season_players if player.team and player.team.strip()})
    return season_players, teams


def map_played_teams(raw_team_names: set[str], local_teams: list[str]) -> tuple[list[str], list[str]]:
    local_by_norm = {norm(team): team for team in local_teams if norm(team)}
    local_norm_keys = list(local_by_norm.keys())
    matched: list[str] = []
    unmatched: list[str] = []
    for raw in sorted(raw_team_names):
        team = None
        for variant in alias_variants(raw):
            team = local_by_norm.get(norm(variant))
            if team:
                break
        if team is None:
            close = difflib.get_close_matches(norm(raw), local_norm_keys, n=1, cutoff=0.86)
            if close:
                team = local_by_norm[close[0]]
        if team is None:
            unmatched.append(raw)
        else:
            matched.append(team)
    deduped = sorted(dict.fromkeys(matched))
    return deduped, unmatched


def load_played_teams_from_local_plays(project_root: Path, season: str, start_date: str, end_date: str) -> list[str]:
    settings = load_settings(project_root)
    plays_map = settings.get("plays_csv_by_year", {}) or {}
    plays_rel = plays_map.get(str(season).strip())
    if not plays_rel:
        return []
    plays_path = rel_to_pipeline(project_root, plays_rel)
    if not plays_path.exists():
        log(f"[targets] local plays file missing: {plays_path}")
        return []

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    names: set[str] = set()
    with plays_path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_dt = (row.get("gameStartDate") or "").strip()
            if not raw_dt:
                continue
            try:
                game_date = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).date()
            except Exception:
                continue
            if game_date < start or game_date > end:
                continue
            for key in ("__team_name", "team", "opponent"):
                value = (row.get(key) or "").strip()
                if value:
                    names.add(value)
    sport_label = "women"
    log(f"[targets] local {sport_label} plays matched raw teams={len(names)} from {plays_path}")
    return sorted(names)


def fetch_games(client: Client, season: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    params = {
        "season": season,
        "startDateRange": start_date,
        "endDateRange": end_date,
    }
    status, payload = client.get("/games", params)
    if status == 200:
        records = to_records(payload)
        if records:
            log(f"[targets] fetched {len(records)} games from /games range query")
            return records
        log("[targets] /games range query returned 200 but no records; trying fallback")
    else:
        log(f"[targets] /games range query failed with status={status}; trying fallback")
    return []


def fetch_games_by_team_fallback(client: Client, season: str, start_date: str, end_date: str, local_teams: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, team in enumerate(local_teams, start=1):
        params = {
            "season": season,
            "team": team,
            "startDateRange": start_date,
            "endDateRange": end_date,
        }
        status, payload = client.get("/games", params)
        if status == 200:
            records.extend(to_records(payload))
        if index == 1 or index % 50 == 0 or index == len(local_teams):
            log(f"[targets] fallback team scan {index}/{len(local_teams)} games={len(records)}")
    return records


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    today = date.today()
    yesterday = today - timedelta(days=1)
    default_start = yesterday.isoformat()
    default_end = today.isoformat()

    ap = argparse.ArgumentParser(description="Build changed-player targets from CBBD games")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--season", default="2026")
    ap.add_argument("--start-date", default=default_start)
    ap.add_argument("--end-date", default=default_end)
    ap.add_argument("--output-file", required=True)
    ap.add_argument("--summary-file", default="")
    ap.add_argument("--sleep-sec", type=float, default=0.05)
    ap.add_argument("--max-requests", type=int, default=1000)
    ap.add_argument("--cache-dir", default="")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    season = str(args.season).strip()
    output_file = Path(args.output_file).resolve()
    summary_file = Path(args.summary_file).resolve() if args.summary_file else None
    api_key = os.getenv("CBBD_API_KEY", DEFAULT_API_KEY).strip()
    if not api_key:
        raise RuntimeError("Missing CBBD_API_KEY")

    cache_dir = Path(args.cache_dir).resolve() if args.cache_dir else (project_root / ".tmp" / "cbbd_target_cache")
    client = Client(api_key=api_key, sleep_sec=args.sleep_sec, max_requests=args.max_requests, cache_dir=cache_dir)

    season_players, local_teams = load_local_players(project_root, season)
    log(f"[targets] local season players={len(season_players)} teams={len(local_teams)} season={season}")

    games = fetch_games(client, season, args.start_date, args.end_date)
    used_fallback = False
    if not games:
        used_fallback = True
        games = fetch_games_by_team_fallback(client, season, args.start_date, args.end_date, local_teams)

    raw_team_names: set[str] = set()
    for game in games:
        raw_team_names.update(extract_team_names(game))

    api_matched_teams, unmatched_teams = map_played_teams(raw_team_names, local_teams)
    local_played_raw = load_played_teams_from_local_plays(project_root, season, args.start_date, args.end_date)
    local_matched_teams, local_unmatched_teams = map_played_teams(set(local_played_raw), local_teams)
    matched_teams = local_matched_teams or api_matched_teams
    matched_set = set(matched_teams)
    if api_matched_teams:
        log(f"[targets] api matched teams ({len(api_matched_teams)}): {', '.join(api_matched_teams)}")
    else:
        log("[targets] api matched teams (0): none")
    if local_matched_teams:
        log(f"[targets] local sport-filtered teams ({len(local_matched_teams)}): {', '.join(local_matched_teams)}")
    if matched_teams:
        log(f"[targets] final matched teams ({len(matched_teams)}): {', '.join(matched_teams)}")
    else:
        log("[targets] final matched teams (0): none")
    if unmatched_teams:
        log(f"[targets] api unmatched teams ({len(unmatched_teams)}): {', '.join(unmatched_teams)}")
    if local_unmatched_teams:
        log(f"[targets] local unmatched teams ({len(local_unmatched_teams)}): {', '.join(local_unmatched_teams)}")

    targets: list[dict[str, Any]] = []
    seen_cache_keys: set[str] = set()
    for player in sorted(season_players, key=lambda item: (bpc.norm_team(item.team), bpc.norm_player_name(item.player))):
        if player.team not in matched_set:
            continue
        cache_key = bpc.card_cache_key(player.player, player.team, player.season)
        if cache_key in seen_cache_keys:
            continue
        seen_cache_keys.add(cache_key)
        targets.append(
            {
                "player": player.player,
                "team": player.team,
                "season": int(bpc.norm_season(player.season)),
                "cache_key": cache_key,
            }
        )

    write_json(output_file, targets)
    log(f"[targets] wrote {len(targets)} players across {len(matched_teams)} matched teams to {output_file}")

    if summary_file is not None:
        summary_payload = {
            "season": season,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "used_fallback": used_fallback,
            "requests": client.request_count,
            "games": len(games),
            "raw_team_names": sorted(raw_team_names),
            "api_matched_teams": api_matched_teams,
            "matched_teams": matched_teams,
            "unmatched_teams": unmatched_teams,
            "local_played_raw": local_played_raw,
            "local_unmatched_teams": local_unmatched_teams,
            "target_count": len(targets),
            "generated_at_utc": utc_now(),
        }
        write_json(summary_file, summary_payload)
        csv_rows = [{"team": team} for team in matched_teams]
        write_csv(summary_file.with_suffix(".csv"), csv_rows, ["team"])


if __name__ == "__main__":
    main()
