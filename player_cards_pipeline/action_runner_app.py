#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

import streamlit as st


DEFAULT_WORKFLOW_FILE = "build_player_card.yml"
DEFAULT_REF = "main"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(ts: str) -> datetime:
    # GitHub timestamps are like 2026-03-14T19:12:52Z
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


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
    st.set_page_config(page_title="NCAAW Card Action Runner", layout="centered")
    st.title("NCAAW Card Action Runner")

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

    st.caption(f"Repo: {owner}/{repo} | Workflow: {workflow_file} | Ref: {ref}")

    year = st.text_input("Season", value="2026")
    player = st.text_input("Player", value="")
    team = st.text_input("Team (optional)", value="")
    output_filename = st.text_input("Output filename (optional)", value="")
    commit_to_repo = st.checkbox("Commit output HTML back to repo", value=False)

    col1, col2 = st.columns(2)
    with col1:
        run_btn = st.button("Run Card Build", type="primary")
    with col2:
        refresh_btn = st.button("Refresh Status")

    if "last_trigger_ts" not in st.session_state:
        st.session_state.last_trigger_ts = None
        st.session_state.last_year = ""
        st.session_state.last_player = ""

    if run_btn:
        if not year.strip() or not player.strip():
            st.error("Please enter at least Season and Player before running.")
            st.stop()
        ts = _iso_now()
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
            commit_to_repo=commit_to_repo,
        )
        if ok:
            st.success(msg)
            st.session_state.last_trigger_ts = ts
            st.session_state.last_year = year.strip()
            st.session_state.last_player = player.strip()
        else:
            st.error(msg)

    if refresh_btn or st.session_state.last_trigger_ts:
        runs = list_dispatch_runs(owner, repo, token, workflow_file, per_page=25)
        if not runs:
            st.warning("No workflow_dispatch runs found yet.")
            st.stop()

        target_run = None
        for r in runs:
            if run_matches_request(
                r,
                st.session_state.last_year or year.strip(),
                st.session_state.last_player or player.strip(),
                st.session_state.last_trigger_ts,
            ):
                target_run = r
                break
        if target_run is None:
            target_run = runs[0]

        run_id = int(target_run.get("id"))
        html_url = str(target_run.get("html_url") or "")
        status = str(target_run.get("status") or "")
        conclusion = str(target_run.get("conclusion") or "")

        st.subheader("Latest Run")
        if st.session_state.last_player and st.session_state.last_year:
            st.caption(f"Requested: {st.session_state.last_player} ({st.session_state.last_year})")
        st.write(f"Run ID: `{run_id}`")
        st.write(f"Status: `{status}` | Conclusion: `{conclusion or 'N/A'}`")
        if html_url:
            st.markdown(f"[Open Run in GitHub]({html_url})")

        if status == "completed":
            arts = get_artifacts(owner, repo, token, run_id)
            if arts:
                st.subheader("Artifacts")
                for a in arts:
                    name = str(a.get("name") or "artifact")
                    aid = a.get("id")
                    aurl = f"https://github.com/{owner}/{repo}/actions/runs/{run_id}/artifacts/{aid}"
                    st.markdown(f"- [{name}]({aurl})")
            else:
                st.info("No artifacts found on this run yet.")


if __name__ == "__main__":
    main()
