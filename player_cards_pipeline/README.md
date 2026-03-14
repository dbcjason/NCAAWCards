# Player Cards Pipeline (GitHub-Ready)

This package gives you:
- One-time historical bootstrap for Bart data (2010-2025)
- Automated 2026 CBBD **plays-only** pulls (regular + postseason combined), chunked and file-size-safe
- A dropdown UI (year + player) to generate cards

## 1) Repository Structure

- `player_cards_pipeline/app.py`
  - Streamlit dropdown app (`Year`, `Player`, optional `Team`) and `Run` button.
- `player_cards_pipeline/config/settings.json`
  - Paths and URL templates.
- `player_cards_pipeline/scripts/fetch_bt_history_2010_2025.py`
  - One-time pull of Bart historical datasets (2010..2025):
    - `getadvstats.php?year=...&csv=1`
    - `{year}_pbp_playerstat_array.json`
- `player_cards_pipeline/scripts/pull_2026_plays_every_other_day.py`
  - Chunked CBBD plays pull wrapper for 2026, safe merge, 95MB split cap.
- `player_cards_pipeline/scripts/is_4am_et_every_other_day.py`
  - Time gate used by GitHub Actions.
- `.github/workflows/cbbd_2026_plays_every_other_day.yml`
  - Scheduled automation at 4AM ET every other day.

## 2) One-Time Historical Run (2010-2025)

Run this once after cloning:

```bash
cd "/Users/henryhalverson/Documents/New project"
python3 player_cards_pipeline/scripts/fetch_bt_history_2010_2025.py \
  --year-start 2010 \
  --year-end 2025 \
  --out-dir player_cards_pipeline
```

Outputs:
- `player_cards_pipeline/data/bt/bt_advstats_2010_2025.csv`
- `player_cards_pipeline/data/bt/bt_playerstat_2010_2025.csv`
- `player_cards_pipeline/data/bt/raw_playerstat_json/*.json`

## 3) Where to Upload Manual PBP Metrics

For PBP-derived possession denominator files, upload to:
- `player_cards_pipeline/data/manual/pbp_metrics/<YEAR>/pbp_player_metrics_<YEAR>.csv`

Examples:
- `player_cards_pipeline/data/manual/pbp_metrics/2025/pbp_player_metrics_2025.csv`
- `player_cards_pipeline/data/manual/pbp_metrics/2026/pbp_player_metrics_2026.csv`

The card builder uses `off_possessions` from this file to normalize Bart unassisted makes into per-100 values.

## 4) GitHub Automation for 2026 Plays

### Required secret
Add this repo secret:
- `CBBD_API_KEY`

### What it does
Workflow: `.github/workflows/cbbd_2026_plays_every_other_day.yml`
- Runs on schedule and manual trigger
- Enforces local-time gate: **4 AM America/New_York, every other day**
- Pulls **plays only**, `season-type=both` (regular+postseason)
- Chunks team ranges to avoid API/request/file-size failures
- Merges chunk outputs with `--max-csv-mb 95` splitting
- Commits and pushes only changed files under `cbbd_seasons/2025-2026/`

## 5) Run the Dropdown App

```bash
cd "/Users/henryhalverson/Documents/New project"
python3 -m pip install -r player_cards_pipeline/requirements.txt
streamlit run player_cards_pipeline/app.py
```

In the app:
1. Select `Year`
2. Select `Player`
3. (If needed) select `Team`
4. Click `Run`

Generated card path defaults to:
- `player_cards_pipeline/output/<player>_<year>.html`

## 6) Card Metric Source Rule (Current)

Self Creation now uses:
- Numerators from Bart `/{year}_pbp_playerstat_array.json`
  - unassisted makes = `made - assisted` by zone
  - unassisted dunks = `dunks_made - dunks_assisted`
- Denominator from manual PBP metrics file:
  - `off_possessions`

`Unassisted Pts/100` formula:
- `(2 * UAsst'd Rim FGM + 2 * UAsst'd Mid FGM + 3 * UAsst'd 3PM) / off_possessions * 100`
- `UAsst'd Dunks/100` is separate and does **not** count toward `Unassisted Pts/100`.

## 7) Going Forward

For a new year (example `2027`):
1. Add its plays path + advgames path + pbp metrics path in `player_cards_pipeline/config/settings.json`
2. Add the manual PBP file to `player_cards_pipeline/data/manual/pbp_metrics/2027/`
3. Cards will pull Bart playerstat JSON automatically using:
   - `https://barttorvik.com/{year}_pbp_playerstat_array.json`
