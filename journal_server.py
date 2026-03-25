"""
Trade Journal Server
=====================
Tiny local HTTP server that saves/loads trade journal data to a JSON file.
Run this once and leave it running in the background.

Usage:
    python journal_server.py

Then open journal.html in your browser.
Server runs on http://localhost:7777
Data saved to: data/trade_journal.json
"""

import json
import os
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "trade_journal.json")
PORT      = 7777

os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)


def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"trades": [], "version": 1}


def save_data(data: dict):
    # Write to temp file first, then rename — prevents data corruption
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, DATA_FILE)


class JournalHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        # Suppress default access log noise, only show errors
        if args and str(args[1]) not in ('200', '204'):
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]}")

    def _send(self, status: int, body: str, content_type="application/json"):
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", len(data))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        self._send(204, "")

    def do_GET(self):
        if self.path == "/trades":
            data = load_data()
            self._send(200, json.dumps(data))
        elif self.path == "/health":
            self._send(200, json.dumps({"status": "ok", "file": DATA_FILE}))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if self.path == "/trades":
            length  = int(self.headers.get("Content-Length", 0))
            body    = self.rfile.read(length)
            trade   = json.loads(body)
            data    = load_data()
            # Assign an ID if not present
            if "id" not in trade:
                existing_ids = [t.get("id", 0) for t in data["trades"]]
                trade["id"] = max(existing_ids, default=0) + 1
            trade["created_at"] = datetime.now().isoformat()
            data["trades"].append(trade)
            save_data(data)
            print(f"  [{datetime.now().strftime('%H:%M:%S')}] Saved trade #{trade['id']} — {trade.get('symbol','')} {trade.get('strategy','')}")
            self._send(200, json.dumps(trade))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_PUT(self):
        # Update existing trade by ID
        parts = self.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "trades":
            trade_id = int(parts[1])
            length   = int(self.headers.get("Content-Length", 0))
            body     = self.rfile.read(length)
            updated  = json.loads(body)
            data     = load_data()
            for i, t in enumerate(data["trades"]):
                if t.get("id") == trade_id:
                    updated["id"]         = trade_id
                    updated["created_at"] = t.get("created_at", "")
                    updated["updated_at"] = datetime.now().isoformat()
                    data["trades"][i]     = updated
                    save_data(data)
                    print(f"  [{datetime.now().strftime('%H:%M:%S')}] Updated trade #{trade_id}")
                    self._send(200, json.dumps(updated))
                    return
            self._send(404, json.dumps({"error": "trade not found"}))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_DELETE(self):
        parts = self.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "trades":
            trade_id = int(parts[1])
            data     = load_data()
            before   = len(data["trades"])
            data["trades"] = [t for t in data["trades"] if t.get("id") != trade_id]
            if len(data["trades"]) < before:
                save_data(data)
                print(f"  [{datetime.now().strftime('%H:%M:%S')}] Deleted trade #{trade_id}")
                self._send(200, json.dumps({"deleted": trade_id}))
            else:
                self._send(404, json.dumps({"error": "trade not found"}))
        else:
            self._send(404, json.dumps({"error": "not found"}))


if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), JournalHandler)
    print(f"\n{'='*50}")
    print(f"  Trade Journal Server")
    print(f"  Running on http://localhost:{PORT}")
    print(f"  Data file: {DATA_FILE}")
    print(f"  Open journal.html in your browser")
    print(f"  Press Ctrl+C to stop")
    print(f"{'='*50}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
