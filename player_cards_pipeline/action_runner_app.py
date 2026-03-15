#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import io
import zipfile
import urllib.error
import urllib.parse
import urllib.request
import urllib.response
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import csv

import streamlit as st


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

    req = urllib.request.Request(
        base,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "NCAAWCards-ActionRunner",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            status = resp.status
            raw = resp.read().decode("utf-8", errors="replace")
            if not raw:
                return status, None
            try:
                return status, json.loads(raw)
            except Exception:
                return status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if e.fp else ""
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}
        return e.code, body


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
) -> tuple[bool, str]:
    payload = {
        "ref": ref,
        "inputs": {
            "year": str(year),
            "player": player,
            "team": team,
            "output_filename": output_filename,
            "commit_to_repo": bool(commit_to_repo),
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


def download_artifact_zip(owner: str, repo: str, token: str, artifact_id: int) -> tuple[bytes | None, str]:
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/octet-stream",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "NCAAWCards-ActionRunner",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            if raw[:2] != b"PK":
                return None, f"unexpected payload ({len(raw)} bytes)"
            return raw, ""
    except urllib.error.HTTPError as e:
        body = b""
        try:
            body = e.read()
        except Exception:
            pass
        return None, f"http {e.code} ({len(body)} bytes)"
    except Exception as e:
        return None, str(e)


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


def main() -> None:
    st.set_page_config(page_title="NCAAW Player Card Creator", layout="centered")
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

    index = load_team_player_index()
    years = [y for y in ["2021", "2022", "2023", "2024", "2025", "2026"] if y in index]
    if not years:
        years = ["2021", "2022", "2023", "2024", "2025", "2026"]

    year = st.selectbox("Season", years, index=len(years) - 1)
    teams = sorted(index.get(year, {}).keys())
    team = st.selectbox("Team", teams) if teams else st.selectbox("Team", [""])
    players = index.get(year, {}).get(team, []) if team else []
    player = st.selectbox("Player", players) if players else st.selectbox("Player", [""])
    output_filename = st.text_input("Output filename (optional)", value="")

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

    if run_btn:
        if not year.strip() or not player.strip():
            st.error("Please enter at least Season and Player before running.")
            st.stop()
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
            output_filename=output_filename.strip(),
            commit_to_repo=False,
        )
        if ok:
            st.success(msg)
            st.session_state.last_trigger_ts = ts
            st.session_state.last_year = year.strip()
            st.session_state.last_player = player.strip()
            st.session_state.last_actor = actor
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
            # Try to resolve the just-dispatched run id; don't show stale older runs.
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
            arts = get_artifacts(owner, repo, token, run_id)
            if arts:
                st.subheader("Artifacts")
                for i, a in enumerate(arts):
                    name = str(a.get("name") or "artifact")
                    aid = a.get("id")
                    if aid is None:
                        st.markdown(f"- {name}")
                        continue
                    try:
                        aid_int = int(aid)
                    except Exception:
                        st.markdown(f"- {name}")
                        continue

                    zip_bytes, err = download_artifact_zip(owner, repo, token, aid_int)
                    if zip_bytes:
                        html_payload = extract_first_html(zip_bytes)
                        if html_payload:
                            html_name, html_bytes = html_payload
                            st.download_button(
                                label=f"Download {html_name}",
                                data=html_bytes,
                                file_name=Path(html_name).name,
                                mime="text/html",
                                key=f"dl_{run_id}_{aid_int}_{i}",
                            )
                        else:
                            st.caption(f"{name}: No HTML file found inside artifact.")
                    else:
                        st.caption(f"{name}: Download unavailable ({err}).")
            else:
                st.info("No artifacts found on this run yet.")


if __name__ == "__main__":
    main()
