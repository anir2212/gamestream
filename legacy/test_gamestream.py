import unittest
from gamestream import connect, daily_active_users, events, ingest, materialize, validate


class StreamTests(unittest.TestCase):
    def test_schema_evolution(self):
        v2 = next(row for row in events(4) if row["schema_version"] == 2)
        self.assertTrue(validate(v2))
        v2.pop("device_type")
        self.assertFalse(validate(v2))

    def test_dedup_and_restart(self):
        db = connect(":memory:")
        ingest(db, events(101))
        first = materialize(db)
        self.assertEqual(first["accepted"], 101)
        self.assertGreater(first["duplicates"], 0)
        self.assertEqual(materialize(db)["accepted"], 0)
        self.assertTrue(daily_active_users(db))
        self.assertIsNotNone(db.execute("SELECT device_type FROM silver WHERE event_id='ev-100'").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
