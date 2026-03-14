#!/usr/bin/env python3
"""Refresh enriched player JSON for script season 2026 from a Dropbox zip link.

This downloads a zip that contains `enrichedPlayers/players_all_Women_2025_*.json`,
copies those source files into the repo, and combines them into the by_script_season
output used by `build_player_card.py`.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path


DEFAULT_DROPBOX_URL = (
    "https://www.dropbox.com/scl/fi/REPLACE_WITH_WOMEN_ZIP/"
    "hoop_explorer_players_all_women_2025_26.zip?dl=0"
)


def to_direct_download_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    q = urllib.parse.parse_qs(parsed.query)
    q["dl"] = ["1"]
    new_query = urllib.parse.urlencode(q, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=new_query))


def download_zip(url: str, out_zip: Path) -> None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/zip,*/*",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    if not data:
        raise RuntimeError("Downloaded empty payload from Dropbox link.")
    out_zip.write_bytes(data)


def find_enriched_jsons(extract_root: Path, gender: str, json_year: str) -> list[Path]:
    # Keep only json year 2025 files, since script season 2026 expects that mapping.
    found = sorted(extract_root.rglob(f"players_all_{gender}_{json_year}_*.json"))
    return [p for p in found if p.name.endswith(".json")]


def copy_sources(src_files: list[Path], dst_dir: Path, gender: str, json_year: str) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    for p in dst_dir.glob(f"players_all_{gender}_{json_year}_*.json"):
        p.unlink()
    for src in src_files:
        shutil.copy2(src, dst_dir / src.name)


def verify_net_pts_present(src_files: list[Path]) -> None:
    checked = 0
    with_net = 0
    for p in src_files:
        obj = json.loads(p.read_text(encoding="utf-8"))
        players = obj.get("players", []) if isinstance(obj, dict) else []
        has = False
        for r in players:
            if isinstance(r, dict) and isinstance(r.get("net_pts"), dict):
                has = True
                break
        checked += 1
        if has:
            with_net += 1
    if checked == 0 or with_net == 0:
        raise RuntimeError("Refreshed enriched files do not contain net_pts; aborting refresh.")
    print(f"net_pts check: {with_net}/{checked} source files contain net_pts")


def run_combiner(project_root: Path, input_dir: Path, gender: str) -> None:
    combine_script = project_root / "player_cards_pipeline" / "scripts" / "combine_enriched_players_json.py"
    out_dir = project_root / "player_cards_pipeline" / "data" / "manual" / "enriched_players"
    cmd = [
        "python3",
        str(combine_script),
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(out_dir),
        "--gender",
        gender,
    ]
    subprocess.run(cmd, cwd=str(project_root), check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Refresh enriched 2026 source from Dropbox zip.")
    ap.add_argument("--project-root", default=".", help="Repo root.")
    ap.add_argument("--dropbox-url", default=DEFAULT_DROPBOX_URL, help="Dropbox zip share URL.")
    ap.add_argument("--gender", default="Women", help="Gender token (Women or Men).")
    ap.add_argument("--json-year", default="2025", help="Source JSON year (script season = json year + 1).")
    ap.add_argument("--script-season", default="2026", help="Target script season.")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    manual_root = root / "player_cards_pipeline" / "data" / "manual" / "enriched_players"
    source_dir = manual_root / "dropbox_2025_source"
    by_script = manual_root / "by_script_season"

    direct_url = to_direct_download_url(args.dropbox_url)

    with tempfile.TemporaryDirectory(prefix="enriched_zip_") as td:
        tmp = Path(td)
        zip_path = tmp / "enriched.zip"
        extract_dir = tmp / "unzipped"
        extract_dir.mkdir(parents=True, exist_ok=True)

        download_zip(direct_url, zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        src_files = find_enriched_jsons(extract_dir, gender, json_year)
        if not src_files:
            raise RuntimeError(f"No players_all_{gender}_{json_year}_*.json files found in downloaded zip.")

        verify_net_pts_present(src_files)
        copy_sources(src_files, source_dir, gender, json_year)
        run_combiner(root, source_dir, gender)

    expected = by_script / f"players_all_{gender}_scriptSeason_{script_season}_fromJsonYear_{json_year}.json"
    if not expected.exists():
        raise RuntimeError(f"Expected combined output not found: {expected}")

    print(f"Enriched 2026 refresh complete: {expected}")


if __name__ == "__main__":
    main()
    gender = args.gender.strip().capitalize()
    json_year = str(args.json_year).strip()
    script_season = str(args.script_season).strip()
