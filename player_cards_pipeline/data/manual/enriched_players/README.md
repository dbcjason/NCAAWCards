# Enriched Players JSON (Combined)

This folder stores combined `enrichedPlayers` JSON files for player cards.

## Year mapping rule

- Source JSON naming uses **json year**.
- Card pipeline uses **script season**.
- Mapping: `script_season = json_year + 1`

Example:
- `players_all_Men_2022_*.json` (source) -> script season `2023`

## Outputs

- `by_json_year/players_all_Men_<json_year>_combined.json`
- `by_script_season/players_all_Men_scriptSeason_<script_season>_fromJsonYear_<json_year>.json`
- `manifest.json` summary of included tiers/files and counts.

## Combine script

Run:

```bash
python3 player_cards_pipeline/scripts/combine_enriched_players_json.py \
  --input-dir "/Users/henryhalverson/Downloads/enrichedPlayers" \
  --output-dir "player_cards_pipeline/data/manual/enriched_players"
```

