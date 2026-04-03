Static Card Payloads (v1)
=========================

Purpose
-------
This folder is for the additive static-site pipeline (it does not replace your current HTML card builder).

The script `player_cards_pipeline/scripts/build_static_card_payloads.py` writes:

- One JSON payload per player per season:
  - `player_cards_pipeline/public/cards/<year>/<team_slug>__<player_slug>.json`
- Per-year index:
  - `player_cards_pipeline/public/cards/<year>/index.json`
- Per-year incremental hash manifest:
  - `player_cards_pipeline/public/cards/<year>/manifest.json`

Why this exists
---------------
This supports an instant web experience:

1. Precompute payloads once for historical years.
2. Incrementally update only changed current-season players nightly.
3. Frontend loads a small JSON and renders immediately.

Run Examples
------------

Historical bootstrap:

`python3 player_cards_pipeline/scripts/build_static_card_payloads.py --project-root . --years "2019-2025" --min-games 5`

Nightly incremental current season:

`python3 player_cards_pipeline/scripts/build_static_card_payloads.py --project-root . --years "2026" --min-games 5 --incremental`

Testing small sample:

`python3 player_cards_pipeline/scripts/build_static_card_payloads.py --project-root . --years "2026" --limit 50`
