#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import io
import zipfile
import base64
import re
import http.client
import urllib.error
import urllib.parse
import urllib.request
import urllib.response
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import csv

import streamlit as st
import pandas as pd
from scripts import roster_simulator as roster_sim


DEFAULT_WORKFLOW_FILE = "build_player_card.yml"
DEFAULT_REF = "main"
ROOT = Path(__file__).resolve().parent


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(ts: str) -> datetime:
    # GitHub timestamps are like 2026-03-14T19:12:52Z
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _norm_year(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if "/" in s:
        tail = s.split("/")[-1]
        if len(tail) == 2 and tail.isdigit():
            return f"20{tail}"
    if "-" in s:
        tail = s.split("-")[-1]
        if len(tail) == 2 and tail.isdigit():
            return f"20{tail}"
    return s


def load_team_player_index() -> dict[str, dict[str, list[str]]]:
    files = [
        ROOT / "data" / "bt" / "bt_advstats_2010_2025.csv",
        ROOT / "data" / "bt" / "bt_advstats_2019_2025.csv",
        ROOT / "data" / "bt" / "bt_advstats_2026.csv",
        ROOT / "data" / "bt" / "bt_advstats_2010_2026.csv",
    ]
    idx: dict[str, dict[str, set[str]]] = {}
    for p in files:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                y = _norm_year(row.get("year", ""))
                if y not in {"2021", "2022", "2023", "2024", "2025", "2026"}:
                    continue
                team = (row.get("team") or "").strip()
                player = (row.get("player_name") or "").strip()
                if not team or not player:
                    continue
                idx.setdefault(y, {}).setdefault(team, set()).add(player)

    out: dict[str, dict[str, list[str]]] = {}
    for y, team_map in idx.items():
        out[y] = {t: sorted(list(players)) for t, players in team_map.items()}
    return out


def load_player_conference_index() -> tuple[dict[str, list[dict[str, str]]], list[str]]:
    files = [
        ROOT / "data" / "bt" / "bt_advstats_2010_2025.csv",
        ROOT / "data" / "bt" / "bt_advstats_2019_2025.csv",
        ROOT / "data" / "bt" / "bt_advstats_2026.csv",
        ROOT / "data" / "bt" / "bt_advstats_2010_2026.csv",
    ]
    by_year: dict[str, dict[tuple[str, str], dict[str, str]]] = {}
    confs: set[str] = set()
    allowed_years = {"2021", "2022", "2023", "2024", "2025", "2026"}

    for p in files:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                y = _norm_year(row.get("year", ""))
                if y not in allowed_years:
                    continue
                player = (row.get("player_name") or "").strip()
                team = (row.get("team") or "").strip()
                conf = (row.get("conf") or "").strip()
                if not player or not team:
                    continue
                if conf:
                    confs.add(conf)
                ymap = by_year.setdefault(y, {})
                key = (player, team)
                if key not in ymap:
                    ymap[key] = {
                        "player": player,
                        "team": team,
                        "conf": conf,
                    }

    out_year: dict[str, list[dict[str, str]]] = {}
    for y, items in by_year.items():
        out_year[y] = sorted(
            items.values(),
            key=lambda r: ((r.get("team") or "").lower(), (r.get("player") or "").lower()),
        )
    return out_year, sorted(confs)


def github_api(
    *,
    method: str,
    owner: str,
    repo: str,
    token: str,
    path: str,
    query: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    base = f"https://api.github.com/repos/{owner}/{repo}{path}"
    if query:
        base += "?" + urllib.parse.urlencode(query)

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    def _do_req(auth_value: str) -> tuple[int, Any]:
        req = urllib.request.Request(
            base,
            data=data,
            method=method,
            headers={
                "Authorization": auth_value,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "NCAAWCards-ActionRunner",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                status = resp.status
                try:
                    raw_bytes = resp.read()
                except http.client.IncompleteRead as e:
                    raw_bytes = e.partial or b""
                raw = raw_bytes.decode("utf-8", errors="replace")
                if not raw:
                    return status, None
                try:
                    return status, json.loads(raw)
                except Exception:
                    return status, raw
        except urllib.error.HTTPError as e:
            raw = ""
            if e.fp:
                try:
                    raw = e.read().decode("utf-8", errors="replace")
                except http.client.IncompleteRead as ie:
                    raw = (ie.partial or b"").decode("utf-8", errors="replace")
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"raw": raw}
            return int(e.code or 0), body

    code, body = _do_req(f"Bearer {token}")
    if code == 401:
        code, body = _do_req(f"token {token}")
    return code, body


def github_viewer_login(owner: str, repo: str, token: str) -> str:
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path="",
    )
    # Repo endpoint returns owner info; use that as a stable actor fallback.
    if code == 200 and isinstance(body, dict):
        o = body.get("owner")
        if isinstance(o, dict):
            return str(o.get("login") or "").strip()
    return ""


def dispatch_build(
    owner: str,
    repo: str,
    token: str,
    workflow_file: str,
    ref: str,
    *,
    year: str,
    player: str,
    team: str,
    output_filename: str,
    commit_to_repo: bool,
    transfer_up: bool = False,
    destination_conference: str = "",
) -> tuple[bool, str]:
    payload = {
        "ref": ref,
        "inputs": {
            "year": str(year),
            "player": player,
            "team": team,
            "output_filename": output_filename,
            "commit_to_repo": bool(commit_to_repo),
            "transfer_up": bool(transfer_up),
            "destination_conference": destination_conference,
        },
    }
    code, body = github_api(
        method="POST",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/actions/workflows/{workflow_file}/dispatches",
        payload=payload,
    )
    if code == 204:
        return True, "Workflow dispatched"
    return False, f"Dispatch failed ({code}): {body}"


def slugify(s: str) -> str:
    s2 = re.sub(r"[^A-Za-z0-9]+", "_", (s or "").strip()).strip("_").lower()
    return s2 or "player"


def get_repo_file_bytes(owner: str, repo: str, token: str, path: str, ref: str) -> tuple[bytes | None, str]:
    enc_path = urllib.parse.quote(path, safe="/")
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/contents/{enc_path}",
        query={"ref": ref},
    )
    if code != 200 or not isinstance(body, dict):
        return None, f"http {code}"
    if str(body.get("type") or "") != "file":
        return None, "not a file"
    content = str(body.get("content") or "")
    encoding = str(body.get("encoding") or "")
    if not content:
        return None, "empty content"
    try:
        if encoding == "base64":
            return base64.b64decode(content.encode("ascii"), validate=False), ""
        return content.encode("utf-8"), ""
    except Exception:
        return None, "decode failed"


def list_dispatch_runs(
    owner: str,
    repo: str,
    token: str,
    workflow_file: str,
    *,
    per_page: int = 20,
) -> list[dict[str, Any]]:
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/actions/workflows/{workflow_file}/runs",
        query={"event": "workflow_dispatch", "per_page": per_page},
    )
    if code != 200 or not isinstance(body, dict):
        return []
    runs = body.get("workflow_runs", [])
    if not isinstance(runs, list):
        return []
    return [r for r in runs if isinstance(r, dict)]


def find_run_id_for_dispatch(
    owner: str,
    repo: str,
    token: str,
    workflow_file: str,
    *,
    after_ts: str,
    actor_login: str = "",
    tries: int = 8,
    sleep_sec: float = 1.25,
) -> int | None:
    for _ in range(max(1, tries)):
        runs = list_dispatch_runs(owner, repo, token, workflow_file, per_page=30)
        for r in runs:
            created_at = str(r.get("created_at") or "")
            if created_at:
                try:
                    if _parse_iso(created_at) < _parse_iso(after_ts):
                        continue
                except Exception:
                    pass
            if actor_login:
                ta = r.get("triggering_actor")
                tal = str(ta.get("login") if isinstance(ta, dict) else "").strip().lower()
                if tal and tal != actor_login.strip().lower():
                    continue
            rid = r.get("id")
            if isinstance(rid, int):
                return rid
            try:
                return int(str(rid))
            except Exception:
                continue
        time.sleep(sleep_sec)
    return None


def get_artifacts(owner: str, repo: str, token: str, run_id: int) -> list[dict[str, Any]]:
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/actions/runs/{run_id}/artifacts",
    )
    if code != 200 or not isinstance(body, dict):
        return []
    arts = body.get("artifacts", [])
    if not isinstance(arts, list):
        return []
    return [a for a in arts if isinstance(a, dict)]


def _http_fetch_bytes(url: str, token: str, *, accept: str, with_auth: bool) -> tuple[int, bytes, str]:
    def _single_fetch(auth_value: str | None) -> tuple[int, bytes, str]:
        headers = {
            "Accept": accept,
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "NCAAWCards-ActionRunner",
        }
        if auth_value:
            headers["Authorization"] = auth_value
        req = urllib.request.Request(url, method="GET", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                try:
                    data = resp.read()
                except http.client.IncompleteRead as e:
                    data = e.partial or b""
                status = int(getattr(resp, "status", 200) or 200)
                location = str(getattr(resp, "url", "") or "")
                return status, data, location
        except urllib.error.HTTPError as e:
            body = b""
            try:
                body = e.read()
            except http.client.IncompleteRead as ie:
                body = ie.partial or b""
            except Exception:
                pass
            location = str(e.headers.get("Location", "") if e.headers else "")
            return int(e.code or 0), body, location
        except Exception:
            return 0, b"", ""

    if with_auth:
        code, data, location = _single_fetch(f"Bearer {token}")
        if code == 401:
            code, data, location = _single_fetch(f"token {token}")
        return code, data, location

    return _single_fetch(None)


def download_artifact_zip(owner: str, repo: str, token: str, artifact_id: int, archive_url: str = "") -> tuple[bytes | None, str]:
    api_url = f"https://api.github.com/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip"
    urls = [u for u in [archive_url, api_url] if u]
    accepts = ["application/octet-stream", "application/zip", "*/*", "application/vnd.github+json"]
    last_err = "unknown download error"

    for u in urls:
        for accept in accepts:
            code, data, loc = _http_fetch_bytes(u, token, accept=accept, with_auth=True)
            if data[:2] == b"PK":
                return data, ""

            # Sometimes GitHub returns a redirect URL that must be fetched without auth header.
            if loc and loc != u:
                _c2, d2, _loc2 = _http_fetch_bytes(loc, token, accept="*/*", with_auth=False)
                if d2[:2] == b"PK":
                    return d2, ""
                last_err = f"http {code} -> redirected non-zip"
            else:
                last_err = f"http {code} ({len(data)} bytes)"

    return None, last_err


def extract_first_html(zip_bytes: bytes) -> tuple[str, bytes] | None:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            html_names = [n for n in zf.namelist() if n.lower().endswith((".html", ".htm"))]
            if not html_names:
                return None
            target = html_names[0]
            return target, zf.read(target)
    except Exception:
        return None


def run_progress(status: str, conclusion: str) -> tuple[int, str]:
    s = (status or "").strip().lower()
    c = (conclusion or "").strip().lower()
    if s == "queued":
        return 20, "Queued"
    if s == "in_progress":
        return 65, "Running"
    if s == "completed":
        if c == "success":
            return 100, "Completed"
        return 100, f"Completed ({conclusion or 'unknown'})"
    if s:
        return 35, s.replace("_", " ").title()
    return 0, "Not started"


def run_matches_request(run: dict[str, Any], year: str, player: str, after_ts: str | None) -> bool:
    title = str(run.get("display_title") or "")
    if year not in title and player.lower() not in title.lower():
        # display_title often contains actor; fallback to only timestamp gate.
        pass
    if after_ts:
        ra = str(run.get("created_at") or "")
        if ra:
            try:
                if _parse_iso(ra) < _parse_iso(after_ts):
                    return False
            except Exception:
                pass
    return True


ROSTER_BT_CSV_CANDIDATES = [
    ROOT / "data" / "bt" / "bt_advstats_2010_2026.csv",
    ROOT / "data" / "bt" / "bt_advstats_2010_2025.csv",
    ROOT / "data" / "bt" / "bt_advstats_2019_2025.csv",
]


@st.cache_resource(show_spinner=False)
def load_roster_core():
    mod = roster_sim.load_module(ROOT.parent)
    loaded_files = []
    bt_rows = []
    seen = set()
    for p in ROSTER_BT_CSV_CANDIDATES:
        if not p.exists():
            continue
        rows = roster_sim.read_bt_rows(p)
        if not rows:
            continue
        loaded_files.append(p)
        for r in rows:
            k = (
                mod.norm_player_name(mod.bt_get(r, ["player_name"])),
                mod.norm_team(mod.bt_get(r, ["team"])),
                mod.norm_season(mod.bt_get(r, ["year"])),
            )
            if not k[0] or not k[1] or not k[2]:
                continue
            if int(k[2]) < 2021:
                continue
            if k in seen:
                continue
            seen.add(k)
            bt_rows.append(r)
    return mod, bt_rows, loaded_files


def roster_seasons(mod, bt_rows):
    years = sorted(
        {
            int(mod.norm_season(mod.bt_get(r, ["year"])))
            for r in bt_rows
            if mod.norm_season(mod.bt_get(r, ["year"])).isdigit()
            and int(mod.norm_season(mod.bt_get(r, ["year"]))) >= 2021
        },
        reverse=True,
    )
    return years


def roster_teams(mod, bt_rows, season):
    teams = sorted(
        {
            (mod.bt_get(r, ["team"]) or "").strip()
            for r in bt_rows
            if mod.norm_season(mod.bt_get(r, ["year"])) == str(season)
        }
    )
    return [t for t in teams if t]


def roster_rows_for_team(mod, bt_rows, season, team):
    out = []
    for r in bt_rows:
        if mod.norm_season(mod.bt_get(r, ["year"])) != str(season):
            continue
        t = (mod.bt_get(r, ["team"]) or "").strip()
        if mod.norm_team(t) != mod.norm_team(team):
            continue
        name = (mod.bt_get(r, ["player_name"]) or "").strip()
        if not name:
            continue
        m = mod._row_transfer_metrics(r)
        out.append(
            {
                "player": name,
                "team": t,
                "season": season,
                "conf": (mod.bt_get(r, ["conf", "conference"]) or "").strip(),
                "mpg": float(m.get("mpg", 20.0) or 20.0),
            }
        )
    out.sort(key=lambda x: x["player"])
    return out


def roster_candidate_pool(mod, bt_rows, season, exclude_team):
    rows = []
    for r in bt_rows:
        if mod.norm_season(mod.bt_get(r, ["year"])) != str(season):
            continue
        team = (mod.bt_get(r, ["team"]) or "").strip()
        if mod.norm_team(team) == mod.norm_team(exclude_team):
            continue
        name = (mod.bt_get(r, ["player_name"]) or "").strip()
        if not name:
            continue
        m = mod._row_transfer_metrics(r)
        rows.append(
            {
                "player": name,
                "team": team,
                "season": season,
                "conf": (mod.bt_get(r, ["conf", "conference"]) or "").strip(),
                "mpg": float(m.get("mpg", 15.0) or 15.0),
                "label": f"{name} ({team})",
            }
        )
    rows.sort(key=lambda x: x["label"])
    return rows


def render_card_tab(owner: str, repo: str, token: str, workflow_file: str, ref: str):
    index = load_team_player_index()
    _player_conf_index, all_confs = load_player_conference_index()
    years = [y for y in ["2021", "2022", "2023", "2024", "2025", "2026"] if y in index]
    if not years:
        years = ["2021", "2022", "2023", "2024", "2025", "2026"]

    year = st.selectbox("Season", years, index=len(years) - 1, key="card_year")
    teams = sorted(index.get(year, {}).keys())
    team = st.selectbox("Team", teams, key="card_team") if teams else st.selectbox("Team", [""], key="card_team_empty")
    players = index.get(year, {}).get(team, []) if team else []
    player = st.selectbox("Player", players, key="card_player") if players else st.selectbox("Player", [""], key="card_player_empty")
    output_filename = st.text_input("Output filename (optional)", value="")
    transfer_up = st.checkbox("Transfer Up", value=False, key="transfer_up")
    destination_conf = ""
    if transfer_up:
        conf_choices = [""] + (all_confs if all_confs else [])
        destination_conf = st.selectbox("Destination Conference", conf_choices, key="transfer_dest_conf")

    col1, col2 = st.columns(2)
    with col1:
        run_btn = st.button("Run Card Build", type="primary")
    with col2:
        refresh_btn = st.button("Refresh Status")
    auto_refresh = st.checkbox("Auto-refresh run status", value=True)

    if "last_trigger_ts" not in st.session_state:
        st.session_state.last_trigger_ts = None
        st.session_state.last_year = ""
        st.session_state.last_player = ""
        st.session_state.last_run_id = None
        st.session_state.last_actor = ""
        st.session_state.last_output_repo_path = ""

    if run_btn:
        if not year.strip() or not player.strip():
            st.error("Please enter at least Season and Player before running.")
            st.stop()
        if transfer_up and not destination_conf.strip():
            st.error("Please choose a destination conference for Transfer Up projection.")
            st.stop()
        out_name = output_filename.strip()
        if not out_name:
            out_name = f"streamlit_cards/{year.strip()}/{slugify(player)}_{int(time.time())}.html"
        out_repo_path = f"player_cards_pipeline/output/{out_name}"
        ts = _iso_now()
        actor = github_viewer_login(owner, repo, token)
        ok, msg = dispatch_build(
            owner,
            repo,
            token,
            workflow_file,
            ref,
            year=year.strip(),
            player=player.strip(),
            team=team.strip(),
            output_filename=out_name,
            commit_to_repo=True,
            transfer_up=transfer_up,
            destination_conference=destination_conf.strip(),
        )
        if ok:
            st.success(msg)
            st.session_state.last_trigger_ts = ts
            st.session_state.last_year = year.strip()
            st.session_state.last_player = player.strip()
            st.session_state.last_actor = actor
            st.session_state.last_output_repo_path = out_repo_path
            st.session_state.last_run_id = find_run_id_for_dispatch(
                owner,
                repo,
                token,
                workflow_file,
                after_ts=ts,
                actor_login=actor,
            )
        else:
            st.error(msg)

    if refresh_btn or st.session_state.last_trigger_ts:
        target_run = None
        run_id = st.session_state.get("last_run_id")
        if not run_id and st.session_state.get("last_trigger_ts"):
            rid = find_run_id_for_dispatch(
                owner,
                repo,
                token,
                workflow_file,
                after_ts=str(st.session_state.get("last_trigger_ts")),
                actor_login=str(st.session_state.get("last_actor") or ""),
                tries=1,
                sleep_sec=0.25,
            )
            if rid:
                st.session_state.last_run_id = rid
                run_id = rid
            else:
                st.info("Waiting for your new run to appear...")
                st.stop()
        if run_id:
            code, body = github_api(
                method="GET",
                owner=owner,
                repo=repo,
                token=token,
                path=f"/actions/runs/{int(run_id)}",
            )
            if code == 200 and isinstance(body, dict):
                target_run = body
        if target_run is None:
            runs = list_dispatch_runs(owner, repo, token, workflow_file, per_page=25)
            if not runs:
                st.warning("No workflow_dispatch runs found yet.")
                st.stop()
            target_run = runs[0]
            try:
                st.session_state.last_run_id = int(target_run.get("id"))
            except Exception:
                pass

        run_id = int(target_run.get("id"))
        status = str(target_run.get("status") or "")
        conclusion = str(target_run.get("conclusion") or "")

        st.subheader("Latest Run")
        if st.session_state.last_player and st.session_state.last_year:
            st.caption(f"Requested: {st.session_state.last_player} ({st.session_state.last_year})")
        st.write(f"Run ID: `{run_id}`")
        st.write(f"Status: `{status}` | Conclusion: `{conclusion or 'N/A'}`")
        pct, label = run_progress(status, conclusion)
        st.progress(pct, text=f"Run Progress: {label} ({pct}%)")
        if auto_refresh and status in {"queued", "in_progress"}:
            st.caption("Auto-refreshing every 4 seconds while run is active...")
            time.sleep(4)
            st.rerun()

        if status == "completed":
            if conclusion == "success":
                out_repo_path = str(st.session_state.get("last_output_repo_path") or "").strip()
                if out_repo_path:
                    html_bytes = None
                    err = "not found"
                    for _ in range(8):
                        html_bytes, err = get_repo_file_bytes(owner, repo, token, out_repo_path, ref)
                        if html_bytes:
                            break
                        time.sleep(1.5)
                    if html_bytes:
                        st.download_button(
                            label="Download HTML",
                            data=html_bytes,
                            file_name=Path(out_repo_path).name,
                            mime="text/html",
                            key=f"html_{run_id}",
                        )
                    else:
                        st.caption(f"Built, but committed HTML is not readable yet ({err}).")
                else:
                    st.caption("Built, but output path was not captured for this run.")
            else:
                st.caption("Run failed; no HTML output available.")


def render_roster_tab():
    st.caption("Interactive roster simulator (Women). Remove current players, add incoming players, set minutes, and see in-app projection deltas.")
    mod, bt_rows, bt_paths = load_roster_core()
    if bt_paths:
        shown = ", ".join(str(p.name) for p in bt_paths)
        st.caption(f"BT sources: {shown} | merged rows: {len(bt_rows)}")
    else:
        st.error("No BT CSV found for roster simulator.")
        return

    years = roster_seasons(mod, bt_rows)
    if not years:
        st.error("No seasons found in BT CSV (2021+).")
        return

    c1, c2, c3 = st.columns([1, 1.2, 1.2])
    with c1:
        season = st.selectbox("Season", years, index=0, key="r_season")
    teams = roster_teams(mod, bt_rows, season)
    with c2:
        team = st.selectbox("Base Team", teams, key="r_team") if teams else st.selectbox("Base Team", [""], key="r_team_empty")
    base_rows = roster_rows_for_team(mod, bt_rows, season, team)
    if not base_rows:
        st.error("No roster rows for selected team/season.")
        return
    base_conf = base_rows[0]["conf"] if base_rows else ""
    with c3:
        dest_conf = st.text_input("Destination Conference (for added players)", value=base_conf, key="r_dest_conf")

    st.subheader("1) Exclude Current Players")
    base_names = [r["player"] for r in base_rows]
    exclude_names = st.multiselect("Select players to exclude", options=base_names, default=[], key="r_exclude")
    keep_rows = [r for r in base_rows if r["player"] not in set(exclude_names)]

    st.subheader("2) Add Incoming Players")
    pool = roster_candidate_pool(mod, bt_rows, season, team)
    add_labels = st.multiselect(
        "Select players to add",
        options=[r["label"] for r in pool],
        default=[],
        key="r_add",
        help="All available players for the selected season (excluding current base team).",
    )
    label_to_row = {r["label"]: r for r in pool}
    add_rows = [label_to_row[l] for l in add_labels if l in label_to_row]

    st.subheader("3) Minutes / Final Roster")
    merged = []
    for r in keep_rows:
        merged.append(
            {
                "player": r["player"],
                "team": r["team"],
                "season": r["season"],
                "added": False,
                "minutes": round(max(0.0, r["mpg"]), 1),
                "destination_conference": "",
            }
        )
    for r in add_rows:
        merged.append(
            {
                "player": r["player"],
                "team": r["team"],
                "season": r["season"],
                "added": True,
                "minutes": round(max(0.0, r["mpg"]), 1),
                "destination_conference": dest_conf,
            }
        )
    if not merged:
        st.warning("No players selected after exclusions/additions.")
        return

    edited = st.data_editor(
        pd.DataFrame(merged),
        hide_index=True,
        use_container_width=True,
        disabled=["player", "team", "season", "added"],
        key="r_editor",
        column_config={
            "minutes": st.column_config.NumberColumn(min_value=0.0, max_value=40.0, step=0.5),
            "destination_conference": st.column_config.TextColumn(help="Only used when added=True"),
        },
    )

    interaction_model = st.checkbox(
        "Enable Interaction Model",
        value=True,
        key="r_interaction",
        help="Rebalances projected usage/efficiency across selected players based on creation and spacing context.",
    )
    export_html = st.checkbox(
        "Also generate HTML report",
        value=False,
        key="r_export_html",
    )
    out_name = st.text_input(
        "Output HTML filename (used only if export is enabled)",
        value=f"{team.replace(' ', '_').lower()}_{season}_roster_sim.html",
        disabled=not export_html,
        key="r_out_name",
    )

    if st.button("Generate Team Fit Report", type="primary", key="r_generate"):
        progress = st.progress(0, text="Starting team-fit simulation...")
        inputs: list[roster_sim.InputPlayer] = []
        for _, row in edited.iterrows():
            inputs.append(
                roster_sim.InputPlayer(
                    player=str(row["player"]),
                    team=str(row["team"]),
                    season=int(row["season"]),
                    minutes=float(row["minutes"]),
                    destination_conference=str(row["destination_conference"] or ""),
                )
            )
        progress.progress(10, text="Prepared roster inputs")
        history_examples = roster_sim.build_transfer_examples(mod, bt_rows)
        progress.progress(25, text="Built transfer history examples")

        resolved: list[roster_sim.ResolvedPlayer] = []
        missing: list[roster_sim.InputPlayer] = []
        total_inputs = max(1, len(inputs))
        for idx, p in enumerate(inputs, start=1):
            bt_row = roster_sim.find_bt_row(mod, bt_rows, p)
            if bt_row is None:
                missing.append(p)
                continue
            projected, transfer_applied = roster_sim.project_transfer_metrics(mod, bt_row, p.destination_conference, history_examples)
            src_conf = mod._conference_key(mod.bt_get(bt_row, ["conf", "conference"]))
            resolved.append(
                roster_sim.ResolvedPlayer(
                    inp=p,
                    bt_row=bt_row,
                    projected=projected,
                    source_conf=src_conf,
                    transfer_applied=transfer_applied,
                )
            )
            if idx % 5 == 0 or idx == total_inputs:
                pct = 25 + int(35 * (idx / total_inputs))
                progress.progress(min(60, pct), text=f"Matching/projecting players ({idx}/{total_inputs})")

        if not resolved:
            progress.progress(100, text="No matched players")
            st.error("No players matched in BT data. Check names/teams.")
            return

        progress.progress(70, text="Aggregating team summaries")
        pace_scale = roster_sim.estimate_pace_scale(mod, bt_rows, season)
        team_summary, total_minutes = roster_sim.aggregate_team(resolved, interaction_model=interaction_model, pace_scale=pace_scale)
        current_players = roster_sim.build_current_team_players(mod, bt_rows, season, team)
        current_summary, _ = roster_sim.aggregate_team(current_players, interaction_model=False, pace_scale=pace_scale)
        league_team_summaries = roster_sim.build_season_team_summaries(mod, bt_rows, season)
        edited_player_metrics = roster_sim.projected_player_metrics(resolved, interaction_model=interaction_model)
        current_player_metrics = roster_sim.projected_player_metrics(current_players, interaction_model=False)
        in_rows, out_rows = roster_sim.build_in_out_rows(
            mod=mod,
            base_team=team,
            edited_players=resolved,
            edited_metrics=edited_player_metrics,
            current_players=current_players,
            current_metrics=current_player_metrics,
        )
        progress.progress(88, text="Preparing in-app tables")

        # Build In/Out.
        base_norm = mod.norm_team(team)
        selected_base_keys = {
            (str(p.inp.player).strip().lower(), mod.norm_team(p.inp.team))
            for p in resolved
            if mod.norm_team(p.inp.team) == base_norm
        }
        in_data = []
        for p, m in zip(resolved, edited_player_metrics):
            is_in = p.transfer_applied or mod.norm_team(p.inp.team) != base_norm
            if not is_in:
                continue
            in_data.append(
                {
                    "Player": p.inp.player,
                    "From Team": p.inp.team,
                    "Season": p.inp.season,
                    "MPG": round(float(m.get("mpg", 0.0)), 1),
                    "PPG": round(float(m.get("ppg", 0.0)), 1),
                    "RPG": round(float(m.get("rpg", 0.0)), 1),
                    "APG": round(float(m.get("apg", 0.0)), 1),
                    "SPG": round(float(m.get("spg", 0.0)), 1),
                    "BPG": round(float(m.get("bpg", 0.0)), 1),
                    "FG%": round(float(m.get("fg_pct", 0.0)), 1),
                    "3P%": round(float(m.get("tp_pct", 0.0)), 1),
                    "FT%": round(float(m.get("ft_pct", 0.0)), 1),
                }
            )
        out_data = []
        for p, m in zip(current_players, current_player_metrics):
            key = (str(p.inp.player).strip().lower(), mod.norm_team(p.inp.team))
            if key in selected_base_keys:
                continue
            out_data.append(
                {
                    "Player": p.inp.player,
                    "Team": p.inp.team,
                    "Season": p.inp.season,
                    "MPG": round(float(m.get("mpg", 0.0)), 1),
                    "PPG": round(float(m.get("ppg", 0.0)), 1),
                    "RPG": round(float(m.get("rpg", 0.0)), 1),
                    "APG": round(float(m.get("apg", 0.0)), 1),
                    "SPG": round(float(m.get("spg", 0.0)), 1),
                    "BPG": round(float(m.get("bpg", 0.0)), 1),
                    "FG%": round(float(m.get("fg_pct", 0.0)), 1),
                    "3P%": round(float(m.get("tp_pct", 0.0)), 1),
                    "FT%": round(float(m.get("ft_pct", 0.0)), 1),
                }
            )

        proj_rows = []
        for key, label in roster_sim.TEAM_DISPLAY_METRICS:
            cur = current_summary.get(key)
            new = team_summary.get(key)
            delta = (new - cur) if (cur is not None and new is not None) else None
            pool_vals = [s[key] for s in league_team_summaries.values() if key in s]
            low_is_better = key in {"def_rating"}
            cur_rank = roster_sim.metric_rank(cur, pool_vals, lower_is_better=low_is_better)
            new_rank = roster_sim.metric_rank(new, pool_vals, lower_is_better=low_is_better)
            proj_rows.append(
                {
                    "Metric": label,
                    "Current Team": None if cur is None else round(float(cur), 2),
                    "Current Rank": cur_rank,
                    "Edited Roster": None if new is None else round(float(new), 2),
                    "Edited Rank": new_rank,
                    "Delta": None if delta is None else round(float(delta), 2),
                }
            )
        progress.progress(100, text="Completed")

        st.success("Team fit simulation completed.")
        st.subheader("In")
        if in_data:
            st.table(pd.DataFrame(in_data))
        else:
            st.info("No added players selected.")

        st.subheader("Out")
        if out_data:
            st.table(pd.DataFrame(out_data))
        else:
            st.info("No removed players.")

        st.subheader("Team Projection: Current vs Edited")
        st.table(pd.DataFrame(proj_rows))

        if export_html:
            out_dir = ROOT / "output" / "roster_simulator"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / out_name
            roster_sim.render_html(
                out_path=out_path,
                season=season,
                players=resolved,
                edited_summary=team_summary,
                total_minutes=total_minutes,
                current_summary=current_summary,
                base_team=team,
                league_team_summaries=league_team_summaries,
                in_rows=in_rows,
                out_rows=out_rows,
                interaction_model=interaction_model,
            )
            st.caption(f"HTML export: {out_path}")
            html_bytes = out_path.read_bytes()
            st.download_button(
                label="Download HTML Report",
                data=html_bytes,
                file_name=out_path.name,
                mime="text/html",
                type="secondary",
                key="r_download_html",
            )

        if missing:
            st.warning(f"Missing matches: {len(missing)}")
            for m in missing[:20]:
                st.write(f"- {m.player} ({m.team}, {m.season})")


def main() -> None:
    st.set_page_config(page_title="NCAAW Tools", layout="centered")
    st.title("NCAAW Player Card Creator")
    st.caption("Created by @DBCJason")

    secrets = st.secrets if hasattr(st, "secrets") else {}
    owner = str(secrets.get("GITHUB_OWNER", "")).strip()
    repo = str(secrets.get("GITHUB_REPO", "")).strip()
    token = str(secrets.get("GITHUB_TOKEN", "")).strip()
    workflow_file = str(secrets.get("GITHUB_WORKFLOW_FILE", DEFAULT_WORKFLOW_FILE)).strip() or DEFAULT_WORKFLOW_FILE
    ref = str(secrets.get("GITHUB_REF", DEFAULT_REF)).strip() or DEFAULT_REF

    if not owner or not repo or not token:
        st.error("Missing Streamlit secrets: GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN")
        st.code(
            """
GITHUB_OWNER = "dbcjason"
GITHUB_REPO = "NCAAWCards"
GITHUB_TOKEN = "ghp_..."
GITHUB_WORKFLOW_FILE = "build_player_card.yml"
GITHUB_REF = "main"
            """.strip()
        )
        st.stop()

    tab_cards, tab_roster = st.tabs(["Card Creator", "Roster Simulator"])
    with tab_cards:
        render_card_tab(owner, repo, token, workflow_file, ref)
    with tab_roster:
        render_roster_tab()


if __name__ == "__main__":
    main()
