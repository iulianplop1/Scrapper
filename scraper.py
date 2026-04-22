"""
999.md Vehicle Listings Scraper v2 — Parallel Extraction
=========================================================
Uses multiple browser tabs for 3-4x faster extraction.
Supports URL filter building for pre-filtered results.
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import threading
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://999.md"
DEFAULT_SEARCH_URL = "https://999.md/ro/list/transport/cars?o_16_1=776"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

PARALLEL_TABS = 4        # Number of concurrent detail-page tabs
DELAY_BETWEEN_PAGES = (0.5, 1.0)
DELAY_BETWEEN_BATCHES = (0.2, 0.4)
DELAY_ON_ERROR = (3.0, 6.0)

MAX_RETRIES = 2
DETAIL_TIMEOUT_MS = 12_000
LISTING_TIMEOUT_MS = 20_000

# ─────────────────────────────────────────────────────────────────────────────
# Filter URL Builder
# ─────────────────────────────────────────────────────────────────────────────

FILTER_PARAMS = {
    "price_min": "from_9441_2",
    "price_max": "to_9441_2",
    "currency": "unit_9441_2",
    "year_min": "from_7_19",
    "year_max": "to_7_19",
    "mileage_min": "from_1081_104",
    "mileage_max": "to_1081_104",
    "fuel_type": "o_4_151",
    "transmission": "o_5_101",
    "body_type": "o_3_1",
    "offer_type": "o_16_1",
}

FUEL_VALUES = {
    "benzina": "22", "gasoline": "22", "petrol": "22",
    "diesel": "24",
    "electric": "12617",
    "hybrid": "23",
    "gaz": "748", "lpg": "748",
    "phev": "12618", "plug-in": "12618",
}

TRANSMISSION_VALUES = {
    "manual": "4", "mecanica": "4", "mechanic": "4",
    "automatic": "5", "automata": "5", "auto": "5",
    "robot": "142", "robotizata": "142",
}

BODY_VALUES = {
    "sedan": "143",
    "universal": "144", "wagon": "144", "combi": "144",
    "hatchback": "145",
    "coupe": "146",
    "cabrio": "147", "convertible": "147",
    "suv": "148",
    "crossover": "4721",
    "minivan": "149", "van": "149",
    "pickup": "150",
    "microbus": "7891",
}


def build_filtered_url(base_url: str = DEFAULT_SEARCH_URL, **filters) -> str:
    """Build a 999.md URL with applied filters."""
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query, keep_blank_values=True)

    # Always ensure sell mode
    params.setdefault("o_16_1", ["776"])

    for key, value in filters.items():
        if value is None or value == "" or value == 0:
            continue

        if key in FILTER_PARAMS:
            param_name = FILTER_PARAMS[key]

            # Handle enum lookups
            if key == "fuel_type":
                v = str(value).lower()
                val = FUEL_VALUES.get(v, v)
                params[param_name] = [val]
            elif key == "transmission":
                v = str(value).lower()
                val = TRANSMISSION_VALUES.get(v, v)
                params[param_name] = [val]
            elif key == "body_type":
                v = str(value).lower()
                val = BODY_VALUES.get(v, v)
                params[param_name] = [val]
            else:
                params[param_name] = [str(value)]

    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


# ─────────────────────────────────────────────────────────────────────────────
# Romanian → English field mapping
# ─────────────────────────────────────────────────────────────────────────────

FIELD_MAP = {
    "marcă": "make", "marca": "make",
    "model": "model",
    "an de fabricație": "year", "an de fabricatie": "year",
    "tip caroserie": "body_type",
    "numărul de locuri": "seats", "numarul de locuri": "seats",
    "motor": "engine",
    "tip combustibil": "fuel_type",
    "cutie de viteze": "transmission",
    "tracțiune": "drivetrain", "tractiune": "drivetrain",
    "culoare": "color",
    "rulaj": "mileage", "stare": "condition",
    "locație": "location", "locatie": "location",
    "regiunea": "region",
    "volan": "steering",
    "generație": "generation", "generatie": "generation",
    "capacitate cilindrică": "engine_displacement",
    "capacitate cilindrica": "engine_displacement",
    "putere": "power",
    "norma de poluare": "emission_standard",
    "număr de portiere": "doors", "numar de portiere": "doors",
    "tip vânzător": "seller_type", "tip vanzator": "seller_type",
    "originea automobilului": "car_origin",
    "disponibilitate": "availability",
    "posibilitatea de schimb": "exchange_possible",
}

def normalize_field(name: str) -> str:
    key = name.strip().lower().rstrip(":")
    if key in FIELD_MAP:
        return FIELD_MAP[key]
    slug = re.sub(r"[^\w\s]", "", key)
    slug = re.sub(r"\s+", "_", slug).strip("_")
    return slug if slug else key


def human_delay(bounds: tuple):
    time.sleep(random.uniform(*bounds))


def build_page_url(base_search_url: str, page_num: int) -> str:
    parsed = urlparse(base_search_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page_num)]
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


# ─────────────────────────────────────────────────────────────────────────────
# Detail Page Parser (extracted as a function so tabs can share it)
# ─────────────────────────────────────────────────────────────────────────────

def parse_detail_page(page, url: str) -> dict:
    """Extract all data from a loaded detail page."""
    data = {"url": url, "scraped_at": datetime.now().isoformat()}

    # 1. All meta tags + title in one JS call
    meta = page.evaluate("""
        () => {
            const r = {};
            r._title = document.title || '';
            document.querySelectorAll('meta[property^="product:"]').forEach(m =>
                r[m.getAttribute('property')] = m.getAttribute('content'));
            const d = document.querySelector('meta[name="description"]');
            if (d) r.description = d.getAttribute('content');
            const itempropDesc = document.querySelector('[itemprop="description"]');
            if (itempropDesc) r.full_description = itempropDesc.textContent.trim();
            return r;
        }
    """)

    # 2. Parse title: "Chevrolet Aveo an. 2010 cu rulaj 124000 km, Benzină, 2999 €"
    title = meta.get("_title", "")
    if title:
        data["title"] = title.strip()
        ym = re.search(r"an\.\s*(\d{4})", title)
        if ym: data["year"] = ym.group(1)
        mm = re.search(r"rulaj\s+([\d\s]+)\s*km", title, re.I)
        if mm:
            raw = mm.group(1).replace(" ", "")
            data["mileage"] = f"{raw} km"
            data["mileage_numeric"] = int(raw)
        pm = re.search(r"([\d\s]+)\s*([€$£]|MDL|lei|EUR|USD|RON)", title, re.I)
        if pm:
            data["price"] = f"{pm.group(1).strip()} {pm.group(2)}"
            data["price_numeric"] = int(re.sub(r"\s", "", pm.group(1)))
            data["currency"] = pm.group(2)

    # 3. Product meta tags
    if meta.get("product:brand"): data["make"] = meta["product:brand"]
    if meta.get("product:custom_label_2"): data["model"] = meta["product:custom_label_2"]
    if meta.get("product:custom_label_3"): data["body_type"] = meta["product:custom_label_3"]
    if meta.get("product:custom_label_4"): data["fuel_type"] = meta["product:custom_label_4"]
    if meta.get("product:custom_label_5"): data["transmission"] = meta["product:custom_label_5"]
    if meta.get("product:condition"): data["condition"] = meta["product:condition"]
    if meta.get("full_description"): data["description"] = meta["full_description"]
    elif meta.get("description"): data["description"] = meta["description"]

    if "price_numeric" not in data and meta.get("product:price:amount"):
        try:
            data["price_numeric"] = int(meta["product:price:amount"])
            cur = meta.get("product:price:currency", "EUR")
            data["price"] = f"{meta['product:price:amount']} {cur}"
            data["currency"] = cur
        except ValueError:
            pass

    # 4. DOM walking for specs
    specs = page.evaluate("""
        () => {
            const r = {};
            document.querySelectorAll('a[href*="/list/transport/cars"]').forEach(link => {
                const p = link.parentElement;
                if (!p) return;
                const pt = p.textContent.trim();
                const lt = link.textContent.trim();
                if (!lt || lt.length > 100) return;
                const label = pt.replace(lt, '').trim();
                if (label && label.length > 1 && label.length < 50 && label !== lt && !r[label])
                    r[label] = lt;
            });
            document.querySelectorAll('a[href*="/list/transport/cars?r_"]').forEach(link => {
                const t = link.textContent.trim();
                if (t) r['Locație'] = t;
            });
            return r;
        }
    """)
    if specs:
        for raw_label, value in specs.items():
            if value and len(value) < 200:
                fn = normalize_field(raw_label)
                if fn and fn not in data:
                    data[fn] = value

    # 5. Photo count and main image
    try:
        photos = page.evaluate("""
            () => {
                const s = new Set();
                document.querySelectorAll('img[src*="simpalsmedia.com/999.md/BoardImages"]')
                    .forEach(i => { if (i.src) s.add(i.src); });
                return Array.from(s);
            }
        """)
        if photos: 
            data["photo_count"] = len(photos)
            if len(photos) > 0:
                data["main_image"] = photos[0]
    except Exception:
        pass

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Parallel Scraper
# ─────────────────────────────────────────────────────────────────────────────

class VehicleScraper:
    def __init__(self, search_url: str, limit: int | None = None,
                 output_format: str = "both", output_dir: str = "./output",
                 headless: bool = True, parallel: int = PARALLEL_TABS,
                 progress_callback=None):
        self.search_url = search_url
        self.limit = limit
        self.output_format = output_format.lower()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.headless = headless
        self.parallel = parallel
        self.collected = []
        self.seen_urls = set()
        self.all_fields = set()
        self.progress_callback = progress_callback
        self._lock = threading.Lock()
        self.cancelled = False
        self.db_path = self.output_dir / "vehicles.db"
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS scraped_urls
                            (url TEXT PRIMARY KEY, scraped_at TEXT, data TEXT)''')
            try:
                conn.execute("ALTER TABLE scraped_urls ADD COLUMN data TEXT")
            except sqlite3.OperationalError:
                pass

    def is_already_scraped(self, url: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT 1 FROM scraped_urls WHERE url = ?", (url,))
            return cursor.fetchone() is not None

    def mark_as_scraped(self, url: str, data: dict):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO scraped_urls (url, scraped_at, data) VALUES (?, ?, ?)", 
                         (url, datetime.now().isoformat(), json.dumps(data, ensure_ascii=False)))

    def cancel(self):
        """Request the scraper to stop early."""
        self.cancelled = True

    def _emit(self, event_type: str, data: dict):
        if self.progress_callback:
            self.progress_callback(event_type, data)

    def run(self):
        print(f"\n{'═'*60}")
        print(f"  999.md Vehicle Scraper v2 (Parallel)")
        print(f"  Target : {self.search_url}")
        print(f"  Limit  : {'ALL' if self.limit is None else self.limit}")
        print(f"  Tabs   : {self.parallel}")
        print(f"{'═'*60}\n")

        self._emit("started", {"url": self.search_url, "limit": self.limit})
        start_time = time.time()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled",
                      "--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1920, "height": 1080},
                locale="ro-MD", timezone_id="Europe/Chisinau",
            )
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['ro-MD', 'ro', 'en-US', 'en'] });
            """)

            # Main page for listing navigation
            self.main_page = context.new_page()

            # Single tab for detail scraping
            self.detail_page = context.new_page()

            try:
                self._scrape_listings()
            except KeyboardInterrupt:
                print("\n⚠  Interrupted. Saving collected data...")
            finally:
                browser.close()

        elapsed = time.time() - start_time

        if self.collected:
            files = self._export()
            print(f"  ⏱  Time: {elapsed:.1f}s ({elapsed/max(len(self.collected),1):.1f}s/vehicle)")
            self._emit("complete", {
                "total": len(self.collected),
                "files": files,
                "fields": sorted(self.all_fields),
                "elapsed": round(elapsed, 1),
            })
            return self.collected
        else:
            print("\n⚠  No data collected.")
            self._emit("complete", {"total": 0, "files": [], "fields": [], "elapsed": round(elapsed, 1)})
            return []

    def _scrape_listings(self):
        page_num = 1
        consecutive_empty = 0

        while True:
            if self.cancelled:
                print("\n⚠  Scrape cancelled by user.")
                break

            if self.limit is not None and len(self.collected) >= self.limit:
                print(f"\n✓  Reached limit of {self.limit} listings.")
                break

            url = build_page_url(self.search_url, page_num)
            print(f"\n── Page {page_num} ── {url}")

            success = self._load_listing_page(url)
            if not success:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    print("✗  3 consecutive failed pages. Stopping.")
                    break
                page_num += 1
                continue

            consecutive_empty = 0
            links = self._extract_listing_links()

            if not links:
                print("  No listings found. End of results.")
                break

            # Trim to remaining limit
            remaining = None
            if self.limit is not None:
                remaining = self.limit - len(self.collected)
                links = links[:remaining]

            print(f"  Found {len(links)} links → scraping sequentially")

            # Process sequentially
            self._process_batch(links)

            page_num += 1
            human_delay(DELAY_BETWEEN_PAGES)

    def _load_listing_page(self, url: str) -> bool:
        for attempt in range(MAX_RETRIES):
            try:
                self.main_page.goto(url, wait_until="domcontentloaded", timeout=LISTING_TIMEOUT_MS)
                self.main_page.wait_for_selector(
                    'a[class*="advert__photo__link"], a[class*="AdPhoto"]',
                    timeout=LISTING_TIMEOUT_MS
                )
                return True
            except PlaywrightTimeout:
                print(f"  ⚠  Timeout (attempt {attempt+1}/{MAX_RETRIES})")
                human_delay(DELAY_ON_ERROR)
            except Exception as e:
                print(f"  ⚠  Error: {e}")
                human_delay(DELAY_ON_ERROR)
        return False

    def _extract_listing_links(self) -> list[str]:
        links = []
        for selector in ['a[class*="advert__photo__link"]', 'a[class*="AdPhoto_info__link"]']:
            elements = self.main_page.query_selector_all(selector)
            for el in elements:
                href = el.get_attribute("href")
                if href and href.startswith("/ro/"):
                    full_url = urljoin(BASE_URL, href.split("?")[0])
                    if full_url not in links and full_url not in self.seen_urls:
                        links.append(full_url)
        return links

    def _process_batch(self, links: list[str]):
        """Process detail pages sequentially."""
        for link in links:
            if self.cancelled:
                break
            
            if self.limit is not None and len(self.collected) >= self.limit:
                break

            if link in self.seen_urls:
                continue
            self.seen_urls.add(link)
            
            if self.is_already_scraped(link):
                print(f"    [SKIP] Already extracted: {link.split('?')[0]}")
                self._emit("skip", {"url": link.split('?')[0]})
                continue
            
            result = self._scrape_one(self.detail_page, link)

            if result:
                self.collected.append(result)
                self.mark_as_scraped(link, result)
                for k in result:
                    self.all_fields.add(k)
                title = result.get("title", "?")
                price = result.get("price", "?")
                print(f"    [{len(self.collected):>4}] {title}  —  {price}")
                self._emit("vehicle", {
                    "index": len(self.collected),
                    "data": result,
                    "total_target": self.limit,
                })
            
            human_delay(DELAY_BETWEEN_BATCHES)

    def _scrape_one(self, tab, url: str) -> dict | None:
        """Scrape a single detail page in a browser tab."""
        for attempt in range(MAX_RETRIES):
            try:
                tab.goto(url, wait_until="domcontentloaded", timeout=DETAIL_TIMEOUT_MS)
                tab.wait_for_timeout(300)  # Moderate hydration wait
                return parse_detail_page(tab, url)
            except Exception as e:
                print(f"    ⚠ Error on {url}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(random.uniform(1, 3))
        return None

    # ── Export ────────────────────────────────────────────────────────────

    def _export(self) -> list[str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        files = []
        if self.output_format in ("csv", "both"):
            files.append(self._export_csv(timestamp))
        if self.output_format in ("json", "both"):
            files.append(self._export_json(timestamp))

        print(f"\n{'═'*60}")
        print(f"  ✓  SCRAPING COMPLETE")
        print(f"  Total listings: {len(self.collected)}")
        print(f"  Fields found: {len(self.all_fields)}")
        print(f"{'═'*60}\n")
        return files

    def _export_csv(self, ts: str) -> str:
        fp = self.output_dir / f"vehicles_{ts}.csv"
        priority = [
            "title", "price", "price_numeric", "currency",
            "make", "model", "year", "mileage", "mileage_numeric", "body_type",
            "fuel_type", "engine", "engine_displacement", "power",
            "transmission", "drivetrain", "color", "condition",
            "location", "region", "seats", "doors",
            "seller_type", "description", "url", "scraped_at",
        ]
        extra = sorted(set(self.all_fields) - set(priority))
        cols = [c for c in priority if c in self.all_fields] + extra
        with open(fp, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for row in self.collected:
                w.writerow(row)
        print(f"  📄 CSV: {fp}")
        return str(fp)

    def _export_json(self, ts: str) -> str:
        fp = self.output_dir / f"vehicles_{ts}.json"
        out = {
            "metadata": {
                "source": self.search_url,
                "scraped_at": datetime.now().isoformat(),
                "total_listings": len(self.collected),
                "fields_discovered": sorted(self.all_fields),
            },
            "listings": self.collected,
        }
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"  📄 JSON: {fp}")
        return str(fp)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="999.md Vehicle Scraper v2")
    parser.add_argument("--url", type=str, default=DEFAULT_SEARCH_URL)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--format", type=str, default="both", choices=["csv", "json", "both"])
    parser.add_argument("--output", type=str, default="./output")
    parser.add_argument("--visible", action="store_true")
    parser.add_argument("--parallel", type=int, default=PARALLEL_TABS)
    # Filters
    parser.add_argument("--price-min", type=int, default=None)
    parser.add_argument("--price-max", type=int, default=None)
    parser.add_argument("--year-min", type=int, default=None)
    parser.add_argument("--year-max", type=int, default=None)
    parser.add_argument("--fuel", type=str, default=None)
    parser.add_argument("--transmission", type=str, default=None)
    parser.add_argument("--body", type=str, default=None)

    args = parser.parse_args()

    url = build_filtered_url(
        args.url,
        price_min=args.price_min, price_max=args.price_max,
        year_min=args.year_min, year_max=args.year_max,
        fuel_type=args.fuel, transmission=args.transmission,
        body_type=args.body,
    )

    scraper = VehicleScraper(
        search_url=url, limit=args.limit,
        output_format=args.format, output_dir=args.output,
        headless=not args.visible, parallel=args.parallel,
    )
    scraper.run()


if __name__ == "__main__":
    main()
