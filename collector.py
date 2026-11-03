"""
Collecteur de nouveaux tokens Solana (GeckoTerminal, API publique gratuite).

A chaque execution :
  1. recupere les nouvelles pools Solana et enregistre celles jamais vues
     -> data/pools/AAAA-MM-JJ.csv
  2. revient sur les pools deja vues a +1h, +6h, +24h, +7j pour noter leur etat
     -> data/snapshots/AAAA-MM-JJ.csv

Aucune dependance externe (bibliotheque standard Python uniquement).
Respecte la limite GeckoTerminal (~30 requetes/minute).
"""
import csv
import datetime as dt
import glob
import json
import os
import time
import urllib.error
import urllib.request

BASE = "https://api.geckoterminal.com/api/v2"
NETWORK = "solana"
DATA = os.getenv("DATA_DIR", "data")
PAGES = int(os.getenv("PAGES", "10"))              # 20 pools par page, 10 pages max
MIN_INTERVAL = 2.2                                  # secondes entre deux requetes
TIME_BUDGET = int(os.getenv("TIME_BUDGET", "480"))  # secondes max par execution
CHECKPOINTS = [("1h", 1), ("6h", 6), ("24h", 24), ("7d", 168)]
LOOKBACK_DAYS = 9

FIELDS = [
    "pool", "name", "base_token", "dex", "created_at",
    "price_usd", "liquidity_usd", "fdv_usd", "mcap_usd",
    "vol_m5", "vol_h1", "vol_h6", "vol_h24",
    "buys_m5", "sells_m5", "buyers_m5", "sellers_m5",
    "buys_h1", "sells_h1", "buyers_h1", "sellers_h1",
    "buys_h24", "sells_h24", "buyers_h24", "sellers_h24",
    "chg_m5", "chg_h1", "chg_h6", "chg_h24",
]
POOL_COLS = ["seen_at", "age_min_at_seen"] + FIELDS
SNAP_COLS = ["observed_at", "checkpoint", "age_h", "late", "status"] + FIELDS

_last_call = 0.0
_start = time.time()


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def parse_ts(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def out_of_time():
    return time.time() - _start > TIME_BUDGET


def api_get(path):
    """GET avec respect du rate limit et quelques reessais."""
    global _last_call
    for attempt in range(5):
        wait = MIN_INTERVAL - (time.time() - _last_call)
        if wait > 0:
            time.sleep(wait)
        _last_call = time.time()
        req = urllib.request.Request(
            BASE + path,
            headers={"Accept": "application/json", "User-Agent": "memecoin-research/1.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(15 * (attempt + 1))
                continue
            if e.code >= 500:
                time.sleep(5)
                continue
            print(f"HTTP {e.code} sur {path}")
            return None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            print(f"Erreur reseau sur {path}: {e}")
            time.sleep(5)
    return None


def flatten(p):
    a = p.get("attributes") or {}
    rel = p.get("relationships") or {}
    tx = a.get("transactions") or {}
    vol = a.get("volume_usd") or {}
    chg = a.get("price_change_percentage") or {}
    base_id = (((rel.get("base_token") or {}).get("data")) or {}).get("id", "")
    dex_id = (((rel.get("dex") or {}).get("data")) or {}).get("id", "")
    row = {
        "pool": a.get("address"),
        "name": a.get("name"),
        "base_token": base_id.split("_", 1)[-1] if base_id else "",
        "dex": dex_id,
        "created_at": a.get("pool_created_at"),
        "price_usd": a.get("base_token_price_usd"),
        "liquidity_usd": a.get("reserve_in_usd"),
        "fdv_usd": a.get("fdv_usd"),
        "mcap_usd": a.get("market_cap_usd"),
    }
    for w in ("m5", "h1", "h6", "h24"):
        row[f"vol_{w}"] = vol.get(w)
        row[f"chg_{w}"] = chg.get(w)
    for w in ("m5", "h1", "h24"):
        t = tx.get(w) or {}
        for k in ("buys", "sells", "buyers", "sellers"):
            row[f"{k}_{w}"] = t.get(k)
    return row


def append_rows(folder, cols, rows):
    if not rows:
        return
    path = os.path.join(DATA, folder, now_utc().strftime("%Y-%m-%d") + ".csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerows(rows)


def read_recent(folder):
    rows = []
    cutoff = (now_utc() - dt.timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    for path in sorted(glob.glob(os.path.join(DATA, folder, "*.csv"))):
        if os.path.basename(path)[:10] < cutoff:
            continue
        with open(path, newline="", encoding="utf-8") as f:
            rows.extend(csv.DictReader(f))
    return rows


def collect_new(known):
    now = now_utc()
    new_rows = []
    for page in range(1, PAGES + 1):
        if out_of_time():
            break
        res = api_get(f"/networks/{NETWORK}/new_pools?page={page}")
        if not res or not res.get("data"):
            break
        for p in res["data"]:
            row = flatten(p)
            if not row["pool"] or row["pool"] in known:
                continue
            created = parse_ts(row["created_at"])
            row["seen_at"] = now.isoformat(timespec="seconds")
            row["age_min_at_seen"] = round((now - created).total_seconds() / 60, 1) if created else ""
            known[row["pool"]] = row["created_at"]
            new_rows.append(row)
    append_rows("pools", POOL_COLS, new_rows)
    return len(new_rows)


def due_checkpoints(known, done):
    now = now_utc()
    due = []
    for pool, created_s in known.items():
        created = parse_ts(created_s)
        if not created:
            continue
        age_h = (now - created).total_seconds() / 3600
        for label, hours in CHECKPOINTS:
            if age_h >= hours and (pool, label) not in done:
                due.append((hours, pool, label, age_h))
    due.sort()  # les checkpoints les plus courts d'abord
    return due


def collect_snapshots(due):
    now = now_utc()
    rows = []
    for i in range(0, len(due), 30):
        if out_of_time():
            break
        batch = due[i:i + 30]
        addrs = sorted({pool for _, pool, _, _ in batch})
        res = api_get(f"/networks/{NETWORK}/pools/multi/{','.join(addrs)}")
        if res is None:
            continue
        found = {}
        for p in res.get("data") or []:
            r = flatten(p)
            found[r["pool"]] = r
        for hours, pool, label, age_h in batch:
            base = found.get(pool) or {"pool": pool}
            row = dict(base)
            row.update({
                "observed_at": now.isoformat(timespec="seconds"),
                "checkpoint": label,
                "age_h": round(age_h, 2),
                "late": int(age_h > hours * 1.25 + 0.5),
                "status": "ok" if pool in found else "missing",
            })
            rows.append(row)
    append_rows("snapshots", SNAP_COLS, rows)
    return len(rows)


def main():
    known = {r["pool"]: r["created_at"] for r in read_recent("pools")}
    done = {(r["pool"], r["checkpoint"]) for r in read_recent("snapshots")}
    n_new = collect_new(known)
    due = due_checkpoints(known, done)
    n_snap = collect_snapshots(due)
    print(f"Nouvelles pools: {n_new} | checkpoints dus: {len(due)} | enregistres: {n_snap} "
          f"| duree: {time.time() - _start:.0f}s")


if __name__ == "__main__":
    main()
