#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc

API_VERSION = "2026-04-05b"


def load_settings(project_root: Path) -> dict[str, Any]:
    import json as _json

    p = project_root / "player_cards_pipeline" / "config" / "settings.json"
    return _json.loads(p.read_text(encoding="utf-8"))


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def auth_token() -> str:
    return StringOrEmpty(os.getenv("TRANSFER_MODEL_TOKEN")) or StringOrEmpty(os.getenv("PAYLOAD_SYNC_TOKEN"))


def StringOrEmpty(value: str | None) -> str:
    return str(value or "").strip()


def make_target(player: str, team: str, season: str) -> bpc.PlayerGameStats:
    return bpc.PlayerGameStats(
        player=player,
        team=team,
        season=season,
        games=0,
        points=0,
        rebounds=0,
        assists=0,
        steals=0,
        blocks=0,
        fgm=0,
        fga=0,
        tpm=0,
        tpa=0,
        ftm=0,
        fta=0,
    )


class TransferProjectionHandler(BaseHTTPRequestHandler):
    bt_rows: list[dict[str, str]] = []
    token: str = ""

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        import json as _json

        raw = _json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self) -> bool:
        if not self.token:
            return True
        return self.headers.get("Authorization", "").strip() == f"Bearer {self.token}"

    def do_GET(self) -> None:
        if self.path.rstrip("/") == "/health":
            self._send_json(200, {"ok": True, "rows": len(self.bt_rows), "version": API_VERSION})
            return
        self._send_json(404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/transfer":
            self._send_json(404, {"ok": False, "error": "Not found"})
            return
        if not self._authorized():
            self._send_json(401, {"ok": False, "error": "Unauthorized"})
            return

        try:
            import json as _json

            length = int(self.headers.get("Content-Length", "0") or "0")
            body = _json.loads(self.rfile.read(length).decode("utf-8") if length > 0 else "{}")
            player = StringOrEmpty(body.get("player"))
            team = StringOrEmpty(body.get("team"))
            season = StringOrEmpty(body.get("season"))
            destination_conference = StringOrEmpty(body.get("destinationConference"))
            if not player or not team or not season or not destination_conference:
                self._send_json(400, {"ok": False, "error": "Missing player, team, season, or destinationConference"})
                return

            target = make_target(player, team, season)
            html = bpc.build_transfer_projection_html(target, destination_conference, self.bt_rows)
            self._send_json(
                200,
                {
                    "ok": True,
                    "player": player,
                    "team": team,
                    "season": season,
                    "destinationConference": destination_conference,
                    "html": html,
                },
            )
        except Exception as exc:
            self._send_json(500, {"ok": False, "error": str(exc)})

    def log_message(self, format: str, *args: Any) -> None:
        print(f"[transfer-api] {self.address_string()} - {format % args}", flush=True)


def main() -> None:
    settings = load_settings(PROJECT_ROOT)
    bt_csv = rel_to_pipeline(PROJECT_ROOT, settings["bt_advstats_csv"])
    _header, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
      raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    TransferProjectionHandler.bt_rows = bt_rows
    TransferProjectionHandler.token = auth_token()

    host = os.getenv("HOST", "0.0.0.0").strip() or "0.0.0.0"
    port = int(os.getenv("PORT", "8080"))
    server = ThreadingHTTPServer((host, port), TransferProjectionHandler)
    print(f"[transfer-api] listening on http://{host}:{port} rows={len(bt_rows)} version={API_VERSION}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
