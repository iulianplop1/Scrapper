"""
999.md Scraper Dashboard — Web Server v2
Handles filters, parallel scraping, and previous results loading.
"""

import json
import os
import sys
import threading
import glob
from datetime import datetime
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper import VehicleScraper, build_filtered_url

OUTPUT_DIR = Path("./output")
OUTPUT_DIR.mkdir(exist_ok=True)

active_scrape = {
    "running": False,
    "progress": [],
    "vehicles": [],
    "total_target": None,
    "started_at": None,
    "error": None,
    "complete": False,
    "files": [],
    "elapsed": None,
}
scrape_lock = threading.Lock()
active_scraper_instance = None


def progress_callback(event_type: str, data: dict):
    with scrape_lock:
        if event_type == "vehicle":
            active_scrape["vehicles"].append(data["data"])
            active_scrape["progress"].append({
                "type": "vehicle",
                "index": data["index"],
                "title": data["data"].get("title", "?"),
                "price": data["data"].get("price", "?"),
            })
        elif event_type == "skip":
            active_scrape["progress"].append({
                "type": "skip",
                "url": data["url"]
            })
        elif event_type == "complete":
            active_scrape["complete"] = True
            active_scrape["running"] = False
            active_scrape["files"] = data.get("files", [])
            active_scrape["elapsed"] = data.get("elapsed")
        elif event_type == "started":
            active_scrape["started_at"] = datetime.now().isoformat()


def run_scrape_thread(url: str, limit: int):
    global active_scraper_instance
    try:
        scraper = VehicleScraper(
            search_url=url,
            limit=limit,
            output_format="both",
            output_dir=str(OUTPUT_DIR),
            headless=True,
            progress_callback=progress_callback,
        )
        with scrape_lock:
            active_scraper_instance = scraper
        scraper.run()
    except Exception as e:
        with scrape_lock:
            active_scrape["error"] = str(e)
            active_scrape["running"] = False
            active_scrape["complete"] = True
    finally:
        with scrape_lock:
            active_scraper_instance = None


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self._serve_file("index.html", "text/html")
        elif path == "/api/status":
            self._handle_status()
        elif path == "/api/results":
            self._handle_results()
        elif path == "/api/files":
            self._handle_files()
        elif path == "/api/latest":
            self._handle_latest()
        elif path == "/api/database":
            self._handle_database()
        elif path.startswith("/api/file/"):
            self._handle_file_download(path)
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/scrape":
            self._handle_start_scrape()
        elif parsed.path == "/api/cancel":
            self._handle_cancel_scrape()
        else:
            self.send_error(404)

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, filename, content_type):
        fp = Path(__file__).parent / filename
        if not fp.exists():
            self.send_error(404)
            return
        content = fp.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.wfile.write(content)

    def _handle_status(self):
        with scrape_lock:
            self._send_json({
                "running": active_scrape["running"],
                "complete": active_scrape["complete"],
                "collected": len(active_scrape["vehicles"]),
                "total_target": active_scrape["total_target"],
                "started_at": active_scrape["started_at"],
                "error": active_scrape["error"],
                "elapsed": active_scrape["elapsed"],
                "progress": active_scrape["progress"][-80:],
                "progress_total": len(active_scrape["progress"])
            })

    def _handle_results(self):
        with scrape_lock:
            self._send_json({
                "vehicles": active_scrape["vehicles"],
                "files": active_scrape["files"],
                "complete": active_scrape["complete"],
            })

    def _handle_cancel_scrape(self):
        global active_scraper_instance
        with scrape_lock:
            if active_scraper_instance and active_scrape["running"]:
                active_scraper_instance.cancel()
                self._send_json({"status": "cancelled"})
            else:
                self._send_json({"error": "No active scrape to cancel"}, 400)

    def _handle_start_scrape(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        try:
            params = json.loads(body)
        except json.JSONDecodeError:
            self._send_json({"error": "Invalid JSON"}, 400)
            return

        base_url = params.get("url", "https://999.md/ro/list/transport/cars?o_16_1=776")
        limit = params.get("limit", 10)

        # Build filtered URL
        url = build_filtered_url(
            base_url,
            price_min=params.get("price_min"),
            price_max=params.get("price_max"),
            year_min=params.get("year_min"),
            year_max=params.get("year_max"),
            fuel_type=params.get("fuel_type"),
            transmission=params.get("transmission"),
            body_type=params.get("body_type"),
            mileage_max=params.get("mileage_max"),
            currency=params.get("currency", "eur"),
        )

        with scrape_lock:
            if active_scrape["running"]:
                self._send_json({"error": "A scrape is already running"}, 409)
                return
            active_scrape["running"] = True
            active_scrape["complete"] = False
            active_scrape["progress"] = []
            active_scrape["vehicles"] = []
            active_scrape["total_target"] = limit
            active_scrape["started_at"] = None
            active_scrape["error"] = None
            active_scrape["files"] = []
            active_scrape["elapsed"] = None

        thread = threading.Thread(target=run_scrape_thread, args=(url, limit), daemon=True)
        thread.start()
        self._send_json({"status": "started", "url": url, "limit": limit})

    def _handle_files(self):
        files = []
        for pattern in ["*.json", "*.csv"]:
            for f in sorted(OUTPUT_DIR.glob(pattern), reverse=True):
                files.append({
                    "name": f.name,
                    "size": f.stat().st_size,
                    "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                    "type": f.suffix[1:],
                })
        self._send_json({"files": files})

    def _handle_latest(self):
        """Load latest JSON results for dashboard startup."""
        json_files = sorted(OUTPUT_DIR.glob("vehicles_*.json"), reverse=True)
        if not json_files:
            self._send_json({"vehicles": [], "source": None})
            return
        latest = json_files[0]
        try:
            data = json.loads(latest.read_text(encoding="utf-8"))
            self._send_json({
                "vehicles": data.get("listings", []),
                "source": data.get("metadata", {}).get("source", ""),
                "scraped_at": data.get("metadata", {}).get("scraped_at", ""),
                "filename": latest.name,
            })
        except Exception:
            self._send_json({"vehicles": [], "source": None})

    def _handle_database(self):
        db_path = OUTPUT_DIR / "vehicles.db"
        if not db_path.exists():
            self._send_json({"vehicles": []})
            return
            
        import sqlite3
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute("SELECT data FROM scraped_urls WHERE data IS NOT NULL")
                rows = cursor.fetchall()
                vehicles = [json.loads(row[0]) for row in rows if row[0]]
            self._send_json({"vehicles": vehicles})
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def _handle_file_download(self, path):
        filename = path.replace("/api/file/", "")
        filepath = OUTPUT_DIR / filename
        if not filepath.exists() or ".." in filename:
            self.send_error(404)
            return
        content = filepath.read_bytes()
        ct = "application/json" if filename.endswith(".json") else "text/csv"
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Disposition", f"attachment; filename={filename}")
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        pass


def main():
    port = 8899
    server = HTTPServer(("127.0.0.1", port), DashboardHandler)
    print(f"\n{'═'*60}")
    print(f"  999.md Scraper Dashboard v2")
    print(f"  Open: http://localhost:{port}")
    print(f"{'═'*60}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

if __name__ == "__main__":
    main()
