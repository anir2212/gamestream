"""Versioned fixture exports must be deterministic and self-checking."""

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest
from gamestream_producer import cli, fixture
from gamestream_producer.generator import GENERATOR_VERSION

START = datetime(2026, 2, 1, tzinfo=UTC)


def export(tmp_path, name="a", **overrides):
    options = {"count": 1000, "seed": 20261020, "start": START} | overrides
    return fixture.write_fixture(tmp_path / name, "parity-v1", **options)


def test_export_is_byte_identical_and_records_inputs(tmp_path):
    manifest = export(tmp_path, "a")
    export(tmp_path, "b")
    for name in (fixture.EVENTS_FILE, fixture.MANIFEST_FILE):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
    assert manifest["fixture_version"] == "parity-v1"
    assert manifest["generator"]["seed"] == 20261020
    assert manifest["generator"]["count"] == 1000
    assert manifest["generator"]["start_utc"] == "2026-02-01T00:00:00Z"
    assert manifest["generator"]["generator_version"] == GENERATOR_VERSION
    assert (manifest["deliveries"], manifest["unique_event_ids"]) == (1020, 1000)
    assert (manifest["duplicate_deliveries"], manifest["late_events"]) == (20, 10)
    assert manifest["schema_versions"] == {"1": 500, "2": 500}
    assert manifest["event_dates_utc"] == ["2026-02-01"]
    assert "observed_at" not in json.dumps(manifest)


def test_manifest_hashes_match_an_independent_computation(tmp_path):
    manifest = export(tmp_path)
    content = (tmp_path / "a" / fixture.EVENTS_FILE).read_bytes()
    rows = [json.loads(line) for line in content.decode().splitlines()]
    ids = sorted({row["event_id"] for row in rows})
    assert manifest["events_sha256"] == hashlib.sha256(content).hexdigest()
    assert manifest["event_ids_sha256"] == hashlib.sha256("\n".join(ids).encode()).hexdigest()
    assert all(value.startswith("e-20261020-") for value in ids)
    assert (manifest["first_event_id"], manifest["last_event_id"]) == (ids[0], ids[-1])
    assert len(ids) == 1000


def test_expected_answers_match_deduplicated_rows(tmp_path):
    manifest = export(tmp_path)
    rows, _ = fixture.load_fixture(tmp_path / "a")
    events = {row["event_id"]: row for row in rows}.values()
    purchases = [row for row in events if row["event_type"] == "purchase"]
    games = Counter(row["game_id"] for row in events)
    top = max(games.values())
    answers = manifest["expected_answers"]
    assert answers["row_count"] == ["1000"]
    assert answers["purchase_count"] == [str(len(purchases))]
    assert answers["active_users"] == [str(len({row["user_id"] for row in events}))]
    assert answers["revenue"] == [str(sum(row["amount_cents"] for row in purchases))]
    assert answers["top_game"] == [min(g for g, n in games.items() if n == top), str(top)]


def test_seed_changes_prefix_and_hash(tmp_path):
    first = export(tmp_path, "a")
    second = fixture.write_fixture(tmp_path / "b", "parity-v2", count=1000, seed=20261021,
                                   start=START)
    assert second["event_id_prefix"] == "e-20261021-"
    assert second["event_ids_sha256"] != first["event_ids_sha256"]


def test_tampered_events_or_manifest_are_refused(tmp_path):
    export(tmp_path)
    events = tmp_path / "a" / fixture.EVENTS_FILE
    original = events.read_bytes()
    events.write_bytes(original.replace(b"e-20261020-000000001", b"e-20261020-999999999", 1))
    with pytest.raises(ValueError, match="events file hash"):
        fixture.load_fixture(tmp_path / "a")
    events.write_bytes(original)
    manifest_path = tmp_path / "a" / fixture.MANIFEST_FILE
    manifest = json.loads(manifest_path.read_text())
    manifest["event_ids_sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="event-ID hash"):
        fixture.load_fixture(tmp_path / "a")


def test_conflicting_reexport_is_refused_but_identical_one_is_allowed(tmp_path):
    export(tmp_path)
    export(tmp_path)  # identical content: no error
    with pytest.raises(FileExistsError, match="new version"):
        export(tmp_path, seed=20261021)


@pytest.mark.parametrize(("version", "options", "message"), [
    ("bad version", {}, "fixture version"),
    ("ok", {"count": 0}, "at least one"),
    ("ok", {"seed": -1}, "nonnegative"),
    ("ok", {"start": datetime(2026, 2, 1)}, "timezone"),
])
def test_invalid_inputs_are_rejected(version, options, message):
    arguments = {"count": 10, "seed": 1, "start": START} | options
    with pytest.raises(ValueError, match=message):
        fixture.build_fixture(version, **arguments)


def test_manifest_with_unsafe_prefix_is_refused(tmp_path):
    export(tmp_path)
    path = tmp_path / "a" / fixture.MANIFEST_FILE
    manifest = json.loads(path.read_text())
    manifest["event_id_prefix"] = "e-1-' OR 1=1 --"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="event_id_prefix"):
        fixture.load_manifest(path)


def test_fixture_cli_writes_default_layout(tmp_path, capsys):
    fixture.main(["--version", "parity-v1", "--seed", "5", "--count", "50",
                  "--start", "2026-02-01T00:00:00Z", "--out", str(tmp_path / "f")])
    printed = json.loads(capsys.readouterr().out)
    assert printed["unique_event_ids"] == 50
    assert (tmp_path / "f" / fixture.MANIFEST_FILE).exists()
    with pytest.raises(SystemExit):
        fixture.main(["--version", "x", "--seed", "5", "--start", "2026-02-01T00:00:00"])


def test_producer_publishes_exact_fixture_rows(tmp_path, monkeypatch, capsys):
    manifest = export(tmp_path)
    published = {}

    def fake_publish(rows, **kwargs):
        published["rows"] = rows
        return {"delivered": len(rows)}

    monkeypatch.setattr(cli, "publish", fake_publish)
    cli.main(["--fixture", str(tmp_path / "a")])
    expected, _ = fixture.build_fixture("parity-v1", count=1000, seed=20261020, start=START)
    assert published["rows"] == expected
    printed = json.loads(capsys.readouterr().out)
    assert printed["event_ids_sha256"] == manifest["event_ids_sha256"]
    with pytest.raises(SystemExit):
        cli.main(["--fixture", str(tmp_path / "a"), "--jsonl", str(tmp_path / "x.jsonl")])


def test_committed_parity_manifest_matches_current_generator():
    """A generator change that alters parity-v1 must fail here, not in a live run."""
    path = Path(__file__).resolve().parents[1] / "data/fixtures/parity-v1/manifest.json"
    committed = fixture.load_manifest(path)
    inputs = committed["generator"]
    start = datetime.fromisoformat(inputs["start_utc"].replace("Z", "+00:00"))
    _rows, rebuilt = fixture.build_fixture(committed["fixture_version"], count=inputs["count"],
                                           seed=inputs["seed"], start=start)
    assert rebuilt == committed
