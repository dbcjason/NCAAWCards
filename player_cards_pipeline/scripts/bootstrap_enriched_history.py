#!/usr/bin/env python3
"""Bootstrap historical enriched player JSON from a Dropbox zip.

This is intended for one-time historical setup (e.g. 2018+), and writes
women/men by_script_season files consumed by build_player_card.py.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

DEFAULT_DROPBOX_URL = (
    "https://www.dropbox.com/scl/fi/5uzu9wa7a7dr5oap52s8s/"
    "hoop_explorer_players_all_2018plus.zip?rlkey=avlog6no6nkpz7bbeob20a24j&e=1&dl=0"
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
    with urllib.request.urlopen(req, timeout=180) as resp:
        data = resp.read()
    if not data:
        raise RuntimeError("Downloaded empty payload from Dropbox link.")
    out_zip.write_bytes(data)


def find_enriched_jsons(extract_root: Path, gender: str) -> list[Path]:
    files = sorted(extract_root.rglob(f"players_all_{gender}_*_*.json"))
    out: list[Path] = []
    for p in files:
        parts = p.name.replace(".json", "").split("_")
        if len(parts) != 5:
            continue
        _, _, _, year, tier = parts
        if not year.isdigit():
            continue
        if tier not in {"Low", "Medium", "High"}:
            continue
        out.append(p)
    return out


def copy_sources(src_files: list[Path], dst_dir: Path, gender: str) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    for p in dst_dir.glob(f"players_all_{gender}_*_*.json"):
        p.unlink()
    for src in src_files:
        shutil.copy2(src, dst_dir / src.name)


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
    ap = argparse.ArgumentParser(description="Bootstrap historical enriched players JSON from Dropbox zip.")
    ap.add_argument("--project-root", default=".", help="Repo root")
    ap.add_argument("--dropbox-url", default=DEFAULT_DROPBOX_URL, help="Dropbox share link to historical zip")
    ap.add_argument("--gender", default="Women", help="Gender token in filenames: Women or Men")
    args = ap.parse_args()

    gender = args.gender.strip().capitalize()
    root = Path(args.project_root).resolve()
    manual_root = root / "player_cards_pipeline" / "data" / "manual" / "enriched_players"
    source_dir = manual_root / "dropbox_history_source"

    direct_url = to_direct_download_url(args.dropbox_url)
    with tempfile.TemporaryDirectory(prefix="enriched_history_zip_") as td:
        tmp = Path(td)
        zip_path = tmp / "history.zip"
        extract_dir = tmp / "unzipped"
        extract_dir.mkdir(parents=True, exist_ok=True)

        download_zip(direct_url, zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir)

        src_files = find_enriched_jsons(extract_dir, gender)
        if not src_files:
            raise RuntimeError(f"No players_all_{gender}_*_*.json files found in downloaded historical zip.")

        copy_sources(src_files, source_dir, gender)
        run_combiner(root, source_dir, gender)

    by_script_dir = manual_root / "by_script_season"
    produced = sorted(by_script_dir.glob(f"players_all_{gender}_scriptSeason_*_fromJsonYear_*.json"))
    if not produced:
        raise RuntimeError("Historical combine completed but no by_script_season outputs were found.")

    print(f"Historical enriched bootstrap complete for {gender}. files={len(produced)}")


if __name__ == "__main__":
    main()
