"""Deterministic, fully synthetic game-store clickstream generation."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

EVENT_TYPES = (
    "session_start", "page_view", "search", "add_to_wishlist", "purchase",
    "play_start", "play_end", "review_posted",
)
EVENT_WEIGHTS = (9, 29, 9, 8, 4, 18, 18, 5)
DEVICES = ("desktop", "mobile", "tablet")
GENRES = ("action", "strategy", "simulation", "rpg", "puzzle", "sports")
HOUR_WEIGHTS = (1, 1, 1, 1, 1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 9, 10, 12, 14, 16, 17, 14, 10, 6, 3)


def generate_events(
    count: int,
    *,
    seed: int = 7,
    start: datetime | None = None,
    duplicate_rate: float = 0.02,
    late_rate: float = 0.01,
    users: int = 200,
    games: int = 40,
) -> list[dict]:
    """Return ingress-ordered events; duplicates preserve ID and content.

    The rates are exact counts rounded to the nearest integer for reproducible tests.
    `ingest_ts` is when Kafka receives a record; `event_ts` is user event time.
    """
    if count < 0 or users < 1 or games < 1:
        raise ValueError("count must be nonnegative and users/games positive")
    if not 0 <= duplicate_rate < 1 or not 0 <= late_rate < 1:
        raise ValueError("rates must be between zero and one")
    rng = random.Random(seed)
    origin = start or datetime(2026, 1, 1, tzinfo=UTC)
    if origin.tzinfo is None:
        raise ValueError("start must have a timezone")
    late_ids = set(rng.sample(range(count), round(count * late_rate)))
    duplicate_ids = set(rng.sample(range(count), round(count * duplicate_rate)))
    rows = []
    for i in range(count):
        day = i // 1000
        hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        observed = origin + timedelta(days=day, hours=hour, minutes=rng.randrange(60),
                                      seconds=rng.randrange(60))
        occurred = observed - timedelta(minutes=30) if i in late_ids else observed
        kind = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        game_number = rng.randrange(games)
        version = 1 if i < count // 2 else 2
        row = {
            "event_id": f"e-{seed}-{i:09d}",
            "user_id": f"u-{rng.randrange(users):06d}",
            "game_id": f"g-{game_number:05d}",
            "event_type": kind,
            "event_ts": int(occurred.timestamp() * 1000),
            "ingest_ts": int(observed.timestamp() * 1000),
            "schema_version": version,
            "genre": GENRES[game_number % len(GENRES)],
            "amount_cents": rng.choice((499, 999, 1999, 2999)) if kind == "purchase" else None,
            "review_positive": rng.choice((True, False)) if kind == "review_posted" else None,
        }
        if version == 2:
            row["device_type"] = rng.choice(DEVICES)
        rows.append(row)
    rows.sort(key=lambda row: (row["ingest_ts"], row["event_id"]))
    delivered = []
    for row in rows:
        delivered.append(row)
        if int(row["event_id"].rsplit("-", 1)[1]) in duplicate_ids:
            delivered.append(row.copy())
    return delivered
