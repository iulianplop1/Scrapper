"""
analyze.py — Vehicle Database Analyzer
========================================
Reads vehicles.db in READ-ONLY mode. Never modifies the original database.
All rich output is written to output/analysis_log.txt to avoid terminal
encoding issues on Windows.

Usage:
    python analyze.py
    python analyze.py --budget 15000 --max-km 200000
    python analyze.py --budget 20000 --max-km 150000 --min-year 2015
"""

import sqlite3
import json
import csv
import sys
import argparse
import statistics
from pathlib import Path
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "output" / "vehicles.db"
OUT_DIR = Path(__file__).parent / "output"
JUNK_KEYWORDS = [
    "defect", "avariat", "fara acte", "piese",
    "accident", "rebut", "lovit", "damaged", "broken",
]
ABSOLUTE_MAX_KM    = 300_000
ABSOLUTE_MIN_YEAR  = 1995
PRICE_SANITY_MULT  = 3.0
MIN_GROUP_SIZE     = 5


# ─────────────────────────────────────────────────────────────────────────────
# Logger — writes to file AND prints safe ASCII summary to terminal
# ─────────────────────────────────────────────────────────────────────────────

_log_lines = []

def log(msg: str = ""):
    """Buffer a log line — written to file at end."""
    _log_lines.append(msg)

def terminal(msg: str = ""):
    """Print ASCII-safe progress to terminal."""
    safe = msg.encode("ascii", "replace").decode("ascii")
    print(safe)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Load data (read-only)
# ─────────────────────────────────────────────────────────────────────────────

def load_vehicles() -> list:
    uri = f"file:{DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    rows = conn.execute(
        "SELECT data FROM scraped_urls WHERE data IS NOT NULL"
    ).fetchall()
    conn.close()
    vehicles = []
    for (raw,) in rows:
        try:
            vehicles.append(json.loads(raw))
        except Exception:
            continue
    return vehicles


# ─────────────────────────────────────────────────────────────────────────────
# 2. Hard Cull
# ─────────────────────────────────────────────────────────────────────────────

def hard_cull(vehicles: list, budget, max_km, min_year) -> list:
    survivors = []
    dropped = defaultdict(int)

    for v in vehicles:
        price = v.get("price_numeric")
        km    = v.get("mileage_numeric")
        year  = _int(v.get("year"))
        desc  = (v.get("description", "") or "").lower()
        title = (v.get("title", "") or "").lower()
        text  = desc + " " + title

        if not price:              dropped["no price"] += 1;  continue
        if not km:                 dropped["no mileage"] += 1; continue
        if not year:               dropped["no year"] += 1;   continue
        if budget and price > budget: dropped["over budget"] += 1; continue

        eff_km   = max_km   or ABSOLUTE_MAX_KM
        eff_year = min_year or ABSOLUTE_MIN_YEAR
        if km > eff_km:            dropped["over max km"] += 1; continue
        if year < eff_year:        dropped["too old"] += 1;    continue
        if any(kw in text for kw in JUNK_KEYWORDS):
            dropped["junk keywords"] += 1; continue

        survivors.append(v)

    # Price sanity per model
    model_avg = defaultdict(list)
    for v in survivors:
        model_avg[_model_key(v)].append(v["price_numeric"])
    model_mean = {k: statistics.mean(vv) for k, vv in model_avg.items()}

    final = []
    for v in survivors:
        mean = model_mean.get(_model_key(v), v["price_numeric"])
        if v["price_numeric"] > mean * PRICE_SANITY_MULT:
            dropped["price outlier"] += 1; continue
        final.append(v)

    log("\n[Step 1] Hard Cull")
    log(f"  Started with : {len(vehicles):,}")
    for r, c in sorted(dropped.items(), key=lambda x: -x[1]):
        log(f"  Dropped ({r}): {c:,}")
    log(f"  Surviving    : {len(final):,}")
    terminal(f"[Step 1] Cull done — {len(final):,} vehicles remaining")
    return final


# ─────────────────────────────────────────────────────────────────────────────
# 3. Regression helpers
# ─────────────────────────────────────────────────────────────────────────────

def _simple_ols(xs, ys):
    n = len(xs)
    if n < 2:
        return 0.0, (statistics.mean(ys) if ys else 0.0)
    mx, my = statistics.mean(xs), statistics.mean(ys)
    num   = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    denom = sum((x - mx) ** 2 for x in xs)
    slope = num / denom if denom else 0.0
    return slope, my - slope * mx


def _multi_ols(year_list, km_list, price_list):
    n = len(year_list)
    if n < MIN_GROUP_SIZE:
        return 0.0, 0.0, (statistics.mean(price_list) if price_list else 0.0)

    my = statistics.mean(year_list)
    mk = statistics.mean(km_list)
    mp = statistics.mean(price_list)
    dy = [y - my for y in year_list]
    dk = [k - mk for k in km_list]
    dp = [p - mp for p in price_list]

    syy = sum(y * y for y in dy)
    skk = sum(k * k for k in dk)
    syk = sum(y * k for y, k in zip(dy, dk))
    syp = sum(y * p for y, p in zip(dy, dp))
    skp = sum(k * p for k, p in zip(dk, dp))

    det = syy * skk - syk * syk
    if abs(det) < 1e-10:
        a, _ = _simple_ols(year_list, price_list)
        b, _ = _simple_ols(km_list, price_list)
        return a, b, mp - a * my - b * mk

    a = (syp * skk - skp * syk) / det
    b = (syy * skp - syk * syp) / det
    return a, b, mp - a * my - b * mk


# ─────────────────────────────────────────────────────────────────────────────
# 4. Data-Derived Depreciation
# ─────────────────────────────────────────────────────────────────────────────

def compute_fair_prices(vehicles: list) -> list:
    groups = defaultdict(list)
    for v in vehicles:
        groups[_model_key(v)].append(v)

    all_years  = [_int(v["year"])         for v in vehicles]
    all_kms    = [v["mileage_numeric"]    for v in vehicles]
    all_prices = [v["price_numeric"]      for v in vehicles]
    ga, gb, gc = _multi_ols(all_years, all_kms, all_prices)

    model_coeffs = {}
    for key, grp in groups.items():
        if len(grp) >= MIN_GROUP_SIZE:
            ys = [_int(v["year"])       for v in grp]
            ks = [v["mileage_numeric"]  for v in grp]
            ps = [v["price_numeric"]    for v in grp]
            model_coeffs[key] = _multi_ols(ys, ks, ps)

    used_group = used_global = 0
    for v in vehicles:
        key  = _model_key(v)
        year = _int(v["year"])
        km   = v["mileage_numeric"]
        if key in model_coeffs:
            a, b, c = model_coeffs[key]; used_group += 1
        else:
            a, b, c = ga, gb, gc;       used_global += 1

        fair = max(a * year + b * km + c, 0)
        v["fair_price"] = round(fair)
        v["value_gap"]  = round(fair - v["price_numeric"])

    log(f"\n[Step 2] Data-Derived Depreciation")
    log(f"  Model-specific regression : {used_group:,} vehicles")
    log(f"  Global regression fallback: {used_global:,} vehicles")
    terminal(f"[Step 2] Regression done — {len(model_coeffs)} unique model curves fitted")
    return vehicles


# ─────────────────────────────────────────────────────────────────────────────
# 5. Scoring
# ─────────────────────────────────────────────────────────────────────────────

def compute_scores(vehicles: list) -> list:
    gaps = [v["value_gap"] for v in vehicles]
    min_gap, max_gap = min(gaps), max(gaps)
    gap_range = max_gap - min_gap or 1

    for v in vehicles:
        v["value_score"]    = (v["value_gap"] - min_gap) / gap_range
        v["longevity_score"] = max(0.0, 1.0 - v["mileage_numeric"] / ABSOLUTE_MAX_KM)
    return vehicles


# ─────────────────────────────────────────────────────────────────────────────
# 6. Pareto Frontier — O(n log n) using sorted sweep
# ─────────────────────────────────────────────────────────────────────────────

def pareto_frontier(vehicles: list) -> list:
    """
    Sort by value_score descending. Sweep keeping track of max longevity seen.
    A car is Pareto-optimal only if no previously seen car has both a higher
    value_score AND a higher longevity_score.
    Because we sort by value_score desc, any car that also has longevity >=
    max_longevity_so_far is NOT dominated (it has longevity >= all previous
    cars' longevity at their value_score level).
    """
    sorted_v = sorted(vehicles, key=lambda v: (-v["value_score"], -v["longevity_score"]))
    pareto = []
    max_lon = -1.0
    for v in sorted_v:
        if v["longevity_score"] >= max_lon:
            pareto.append(v)
            max_lon = v["longevity_score"]

    log(f"\n[Step 3] Pareto Frontier")
    log(f"  Pareto-optimal cars: {len(pareto):,} (from {len(vehicles):,})")
    terminal(f"[Step 3] Pareto frontier — {len(pareto):,} optimal cars found")
    return pareto


# ─────────────────────────────────────────────────────────────────────────────
# 7. Regret Minimization Sort
# ─────────────────────────────────────────────────────────────────────────────

def regret_sort(pareto: list) -> list:
    n = len(pareto)
    if n == 0:
        return pareto

    by_value     = sorted(pareto, key=lambda v: -v["value_score"])
    by_longevity = sorted(pareto, key=lambda v: -v["longevity_score"])

    val_rank = {id(v): i / (n - 1) if n > 1 else 0 for i, v in enumerate(by_value)}
    lon_rank = {id(v): i / (n - 1) if n > 1 else 0 for i, v in enumerate(by_longevity)}

    for v in pareto:
        v["regret_score"] = round((val_rank[id(v)] + lon_rank[id(v)]) / 2, 4)

    ranked = sorted(pareto, key=lambda v: v["regret_score"])
    log(f"\n[Step 4] Regret Minimization sort complete")
    terminal(f"[Step 4] Regret scores computed — final ranking ready")
    return ranked


# ─────────────────────────────────────────────────────────────────────────────
# 8. Model Groups
# ─────────────────────────────────────────────────────────────────────────────

def build_model_groups(ranked: list) -> dict:
    groups = defaultdict(list)
    for v in ranked:
        groups[_model_key(v)].append(v)

    summary = {}
    for key, cars in groups.items():
        prices = [c["price_numeric"]   for c in cars]
        kms    = [c["mileage_numeric"] for c in cars]
        years  = [_int(c["year"])      for c in cars]
        summary[key] = {
            "make":      cars[0].get("make",  "?"),
            "model":     cars[0].get("model", "?"),
            "count":     len(cars),
            "avg_price": round(statistics.mean(prices)),
            "avg_km":    round(statistics.mean(kms)),
            "avg_year":  round(statistics.mean(years)),
            "best_car":  cars[0],
            "cars":      cars,
        }

    return dict(sorted(summary.items(), key=lambda x: x[1]["best_car"]["regret_score"]))


# ─────────────────────────────────────────────────────────────────────────────
# 9. Log Output Builders
# ─────────────────────────────────────────────────────────────────────────────

def log_top_listings(ranked: list, n: int = 20):
    log(f"\n{'='*95}")
    log(f"  TOP {n} INDIVIDUAL LISTINGS  (Regret Score: lower = better deal)")
    log(f"{'='*95}")
    hdr = f"{'#':>3}  {'Make':<18} {'Model':<16} {'Year':>5} {'Price':>9} {'Fair':>9} {'Gap':>9} {'Km':>9}  Regret"
    log(hdr)
    log("-" * 95)
    for i, v in enumerate(ranked[:n], 1):
        gap = v["value_gap"]
        gap_str = f"+{gap:,}" if gap >= 0 else f"{gap:,}"
        log(f"{i:>3}  {str(v.get('make') or '?'):<18.18} {str(v.get('model') or '?'):<16.16} "
            f"{str(v.get('year','?')):>5} "
            f"{v['price_numeric']:>9,} {v['fair_price']:>9,} {gap_str:>9} "
            f"{int(v['mileage_numeric']):>9,}  {v['regret_score']:.4f}")
        log(f"     URL: {v.get('url','')}")
        log("")


def log_model_groups(groups: dict, n: int = 15):
    log(f"\n{'='*95}")
    log(f"  TOP {n} MODEL GROUPS  (ranked by best car in each group)")
    log(f"{'='*95}")
    log(f"{'#':>3}  {'Make':<18} {'Model':<18} {'Cars':>5}  {'Avg Price':>10} {'Avg Km':>10}  {'Avg Yr':>6}  Best Regret")
    log("-" * 95)
    for i, (key, g) in enumerate(list(groups.items())[:n], 1):
        log(f"{i:>3}  {str(g['make']):<18.18} {str(g['model']):<18.18} {g['count']:>5}  "
            f"{g['avg_price']:>10,} {g['avg_km']:>10,}  {g['avg_year']:>6}  {g['best_car']['regret_score']:.4f}")
        for j, car in enumerate(g["cars"][:3], 1):
            gap = car["value_gap"]
            gap_str = f"+{gap:,}" if gap >= 0 else f"{gap:,}"
            log(f"      [{j}] {car.get('year','?')} | {int(car['mileage_numeric']):,} km | "
                f"{car['price_numeric']:,} EUR (gap {gap_str}) | {car.get('url','')}")
        log("")


# ─────────────────────────────────────────────────────────────────────────────
# 10. Save files
# ─────────────────────────────────────────────────────────────────────────────

def save_outputs(ranked: list, groups: dict):
    OUT_DIR.mkdir(exist_ok=True)

    # Log file (all detailed output)
    log_path = OUT_DIR / "analysis_log.txt"
    log_path.write_text("\n".join(_log_lines), encoding="utf-8")

    # JSON
    json_path = OUT_DIR / "analysis_results.json"
    out_data = {
        "total_pareto": len(ranked),
        "top_listings": ranked[:100],
        "model_groups": {
            k: {kk: vv for kk, vv in g.items() if kk != "best_car"}
            for k, g in groups.items()
        }
    }
    json_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # CSV — flat ranked list
    csv_path = OUT_DIR / "analysis_results.csv"
    fields = ["rank", "make", "model", "year", "price_numeric", "fair_price",
              "value_gap", "mileage_numeric", "fuel_type", "transmission",
              "body_type", "value_score", "longevity_score", "regret_score", "url"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for i, v in enumerate(ranked, 1):
            row = {k: v.get(k, "") for k in fields}
            row["rank"] = i
            writer.writerow(row)

    terminal(f"\n[Output]")
    terminal(f"  Full log  -> output/analysis_log.txt")
    terminal(f"  JSON      -> output/analysis_results.json")
    terminal(f"  CSV       -> output/analysis_results.csv")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _int(val) -> int:
    try:
        return int(str(val).strip().split()[0])
    except Exception:
        return 0


def _model_key(v: dict) -> str:
    make  = (v.get("make")  or "unknown").strip().lower()
    model = (v.get("model") or "unknown").strip().lower()
    return f"{make}|{model}"


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Vehicle Database Analyzer")
    parser.add_argument("--budget",   type=int, default=None)
    parser.add_argument("--max-km",   type=int, default=None)
    parser.add_argument("--min-year", type=int, default=None)
    parser.add_argument("--top",      type=int, default=20)
    args = parser.parse_args()

    log("=" * 60)
    log("  Vehicle Database Analyzer")
    log("=" * 60)
    terminal("=" * 60)
    terminal("  Vehicle Database Analyzer")
    terminal("=" * 60)

    if args.budget:   log(f"  Budget   : <= {args.budget:,} EUR"); terminal(f"  Budget   : <= {args.budget:,} EUR")
    if args.max_km:   log(f"  Max KM   : <= {args.max_km:,} km");  terminal(f"  Max KM   : <= {args.max_km:,} km")
    if args.min_year: log(f"  Min Year : >= {args.min_year}");      terminal(f"  Min Year : >= {args.min_year}")

    terminal(f"\nLoading from: {DB_PATH}")
    vehicles = load_vehicles()
    log(f"\nLoaded {len(vehicles):,} vehicles")
    terminal(f"  Loaded {len(vehicles):,} vehicles")

    if not vehicles:
        terminal("ERROR: No vehicles in database. Run an extraction first.")
        return

    vehicles = hard_cull(vehicles, args.budget, args.max_km, args.min_year)
    if len(vehicles) < 10:
        terminal("ERROR: Too few vehicles survived the cull. Loosen your filters.")
        return

    vehicles = compute_fair_prices(vehicles)
    vehicles = compute_scores(vehicles)
    pareto   = pareto_frontier(vehicles)

    if len(pareto) < 3:
        terminal("WARNING: Very few Pareto cars found. Falling back to top 50 by value.")
        pareto = sorted(vehicles, key=lambda v: -v["value_score"])[:50]

    ranked = regret_sort(pareto)
    groups = build_model_groups(ranked)

    log_top_listings(ranked, n=args.top)
    log_model_groups(groups, n=15)
    save_outputs(ranked, groups)

    terminal("\n" + "=" * 60)
    terminal("  Analysis complete! Open output/analysis_log.txt for full results.")
    terminal("=" * 60)


if __name__ == "__main__":
    main()
