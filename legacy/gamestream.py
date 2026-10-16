"""Synthetic game events and idempotent bronze/silver/gold local pipeline."""
import argparse
import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

KINDS = ("session_start", "page_view", "search", "add_to_wishlist", "purchase", "play_start", "play_end", "review_posted")


def events(n=1000, seed=7):
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(n):
        ts = start + timedelta(minutes=i * 2)
        if i % 100 == 0:
            ts -= timedelta(minutes=30)
        kind = rng.choices(KINDS, weights=[10, 25, 8, 8, 3, 15, 15, 2])[0]
        row = {"event_id": f"ev-{i}", "user_id": f"u{rng.randrange(40)}", "game_id": f"g{rng.randrange(12)}", "kind": kind, "ts": ts.isoformat(), "schema_version": 2 if i >= n//2 else 1}
        if row["schema_version"] == 2:
            row["device_type"] = rng.choice(["desktop", "mobile"])
        yield row
        if i % 50 == 0:
            yield dict(row)


def connect(path="gamestream.sqlite"):
    db = sqlite3.connect(path)
    db.executescript("""
    CREATE TABLE IF NOT EXISTS bronze(seq INTEGER PRIMARY KEY, raw TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS silver(event_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,game_id TEXT NOT NULL,kind TEXT NOT NULL,ts TEXT NOT NULL,device_type TEXT);
    CREATE INDEX IF NOT EXISTS ix_silver_ts ON silver(ts);
    """)
    return db


def ingest(db, rows):
    for row in rows:
        db.execute("INSERT INTO bronze(raw) VALUES(?)", (json.dumps(row),))
    db.commit()


def validate(row):
    if row.get("kind") not in KINDS or not all(row.get(k) for k in ("event_id", "user_id", "game_id", "ts")):
        return False
    if row.get("schema_version") not in (1, 2):
        return False
    if row["schema_version"] == 2 and row.get("device_type") not in ("desktop", "mobile"):
        return False
    try:
        datetime.fromisoformat(row["ts"])
    except (TypeError, ValueError):
        return False
    return True


def materialize(db):
    accepted = rejected = 0
    for (raw,) in db.execute("SELECT raw FROM bronze ORDER BY seq"):
        try:
            row = json.loads(raw)
            if not validate(row):
                rejected += 1
                continue
            cur = db.execute("INSERT OR IGNORE INTO silver VALUES(?,?,?,?,?,?)", (row["event_id"], row["user_id"], row["game_id"], row["kind"], row["ts"], row.get("device_type")))
            accepted += cur.rowcount
        except (ValueError, KeyError):
            rejected += 1
    db.commit()
    return {"accepted": accepted, "rejected": rejected, "duplicates": db.execute("SELECT count(*) FROM bronze").fetchone()[0] - rejected - db.execute("SELECT count(*) FROM silver").fetchone()[0]}


def daily_active_users(db):
    return db.execute("SELECT substr(ts,1,10) day, count(DISTINCT user_id) dau FROM silver GROUP BY day ORDER BY day").fetchall()


def funnel(db):
    return {kind: db.execute("SELECT count(DISTINCT user_id) FROM silver WHERE kind=?", (kind,)).fetchone()[0] for kind in ("page_view", "add_to_wishlist", "purchase")}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["demo", "report"])
    p.add_argument("--n", type=int, default=1000)
    args = p.parse_args()
    db = connect()
    if args.command == "demo":
        ingest(db, events(args.n))
        print(json.dumps(materialize(db)))
    else:
        print(json.dumps({"dau": daily_active_users(db), "funnel_unique_users": funnel(db)}, indent=2))


if __name__ == "__main__":
    main()
