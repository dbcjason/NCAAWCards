# Streamlit Action Runner Setup (NCAAW)

This app triggers your GitHub `build_player_card.yml` workflow by API and shows run/artifact links.

## Files
- `player_cards_pipeline/action_runner_app.py`

## Local run
```bash
cd "/Users/henryhalverson/Documents/New project/NCAAWCards_clean"
streamlit run player_cards_pipeline/action_runner_app.py
```

## Streamlit Cloud setup
1. Deploy this repo in Streamlit Cloud.
2. Set app entrypoint to:
   - `player_cards_pipeline/action_runner_app.py`
3. In Streamlit app **Secrets**, add:

```toml
GITHUB_OWNER = "dbcjason"
GITHUB_REPO = "NCAAWCards"
GITHUB_TOKEN = "ghp_or_fine_grained_token"
GITHUB_WORKFLOW_FILE = "build_player_card.yml"
GITHUB_REF = "main"
```

## Required token scopes
For classic PAT: `repo`, `workflow`

For fine-grained token:
- Repository access: `NCAAWCards`
- Permissions:
  - Actions: Read and write
  - Contents: Read

## Notes
- Users only need your Streamlit link; they do not need repo access.
- This app dispatches workflow runs and links to artifacts from completed runs.
