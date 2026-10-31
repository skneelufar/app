import unittest

from loadlab.config import STRATEGIES, TARGET_TABLES
from loadlab.ingest import ingest_file
from loadlab.reconcile import _target_state
from loadlab.strategies import get_watermark, publish, publish_all, reset_watermark
from helpers import DbTestCase, line


class StrategyTests(DbTestCase):
    def land(self, name, lines):
        r = ingest_file(self.conn, self.write(name, lines))
        self.assertEqual(r["status"], "SUCCESS", r)

    def state(self, s):
        return _target_state(self.conn, TARGET_TABLES[s])

    def assert_all_equal(self):
        base = self.state("full_refresh")
        for s in STRATEGIES:
            self.assertEqual(self.state(s), base, s)
            self.assert_matches_oracle(s)

    def test_happy_path_all_strategies_equal(self):
        self.land("b1.csv", [line("o1"), line("o2", cust="c2", ts="2026-01-02T08:00:00Z"),
                             line("o1", status="paid", amount="12.00", upd="2026-01-01T11:00:00Z")])
        for r in publish_all(self.conn):
            self.assertEqual(r["status"], "SUCCESS")
        self.assert_all_equal()
        o1 = self.conn.execute("SELECT * FROM orders_incremental_merge WHERE order_id='o1'").fetchone()
        self.assertEqual((o1["status"], o1["amount_cents"], o1["net_amount_cents"]), ("paid", 1200, 1200))
        o2 = self.conn.execute("SELECT * FROM orders_full_refresh WHERE order_id='o2'").fetchone()
        self.assertEqual((o2["status"], o2["net_amount_cents"]), ("created", 0))   # net only for paid/shipped

    def test_empty_input_is_a_safe_noop(self):
        for r in publish_all(self.conn):
            self.assertEqual((r["status"], r["rows_out"]), ("SUCCESS", 0))
        self.land("empty.csv", [])
        for r in publish_all(self.conn):
            self.assertEqual((r["status"], r["rows_out"]), ("SUCCESS", 0))
        self.assert_all_equal()

    def test_late_arriving_older_event_never_overwrites_newer_state(self):
        self.land("b1.csv", [line("o1", status="shipped", upd="2026-01-03T10:00:00Z")])
        publish_all(self.conn)
        # batch 2 delivers an OLDER event (paid, Jan 2) for the same order: arrives late
        self.land("b2.csv", [line("o1", status="paid", upd="2026-01-02T10:00:00Z")])
        publish_all(self.conn)
        for s in STRATEGIES:
            self.assertEqual(self.state(s)["o1"][4], "shipped", s)
        self.assert_all_equal()

    def test_out_of_order_events_inside_one_batch(self):
        self.land("b1.csv", [line("o1", status="shipped", upd="2026-01-03T10:00:00Z"),
                             line("o1", status="paid", upd="2026-01-02T10:00:00Z"),
                             line("o1", status="created")])
        publish_all(self.conn)
        self.assert_all_equal()
        self.assertEqual(self.state("incremental_merge")["o1"][4], "shipped")

    def test_tie_on_updated_at_is_resolved_deterministically(self):
        a = line("o1", status="paid", amount="10.00", upd="2026-01-02T10:00:00Z")
        b = line("o1", status="paid", amount="99.00", upd="2026-01-02T10:00:00Z")
        results = []
        for order, d in ((a, b), (b, a)):               # opposite arrival orders, separate DBs
            self.setUp()
            self.land("x.csv", [order, d])
            publish_all(self.conn)
            self.assert_all_equal()
            results.append(self.state("full_refresh")["o1"][5])
            self.tearDown()
        self.assertEqual(results[0], results[1])        # same winner regardless of arrival order

    def test_incremental_updates_only_changed_and_late_new_partition(self):
        self.land("b1.csv", [line("o1", ts="2026-01-01T10:00:00Z"), line("o2", ts="2026-01-02T10:00:00Z")])
        publish_all(self.conn)
        # late-arriving brand-new order for an old date + an update on o2
        self.land("b2.csv", [line("o3", ts="2026-01-01T09:00:00Z"),
                             line("o2", ts="2026-01-02T10:00:00Z", status="paid", upd="2026-01-02T12:00:00Z")])
        im = publish(self.conn, "incremental_merge")
        self.assertEqual((im["rows_in"], im["rows_written"]), (2, 2))
        publish(self.conn, "full_refresh"); publish(self.conn, "partition_replace")
        self.assert_all_equal()

    def test_partition_replace_only_touches_affected_partitions(self):
        self.land("b1.csv", [line("o1", ts="2026-01-01T10:00:00Z"), line("o2", ts="2026-01-02T10:00:00Z"),
                             line("o3", ts="2026-01-03T10:00:00Z")])
        first = publish(self.conn, "partition_replace")
        self.assertEqual(first["partitions_touched"], 3)
        self.land("b2.csv", [line("o2", ts="2026-01-02T10:00:00Z", status="paid", upd="2026-01-02T12:00:00Z")])
        second = publish(self.conn, "partition_replace")
        self.assertEqual((second["partitions_touched"], second["rows_deleted"], second["rows_written"]), (1, 1, 1))
        runs = {r["order_id"]: r["loaded_run_id"] for r in self.conn.execute("SELECT * FROM orders_partition_replace")}
        self.assertEqual(runs["o1"], first["run_id"])     # untouched partitions keep their lineage
        self.assertEqual(runs["o3"], first["run_id"])
        self.assertEqual(runs["o2"], second["run_id"])

    def test_rerun_same_input_is_idempotent(self):
        self.land("b1.csv", [line("o1"), line("o2", ts="2026-01-02T10:00:00Z"),
                             line("o1", status="paid", upd="2026-01-01T12:00:00Z")])
        publish_all(self.conn)
        before = {s: self.state(s) for s in STRATEGIES}
        again = publish_all(self.conn)            # nothing new: incremental/partition are no-ops
        publish_all(self.conn)
        self.assertEqual(again[1]["rows_in"], 0)
        self.assertEqual(again[2]["partitions_touched"], 0)
        for s in STRATEGIES:
            self.assertEqual(self.state(s), before[s], s)
            self.assertEqual(self.scalar(f"SELECT COUNT(*) FROM {TARGET_TABLES[s]}"), 2)
        ingest_file(self.conn, self.tmp / "b1.csv")            # re-delivering the file changes nothing
        publish_all(self.conn)
        for s in STRATEGIES:
            self.assertEqual(self.state(s), before[s], s)

    def test_backfill_by_resetting_watermark_converges_without_duplicates(self):
        self.land("b1.csv", [line("o1"), line("o2", ts="2026-01-02T10:00:00Z", status="paid", upd="2026-01-02T11:00:00Z")])
        publish_all(self.conn)
        before = {s: self.state(s) for s in STRATEGIES}
        for s in ("incremental_merge", "partition_replace"):
            reset_watermark(self.conn, s)
            self.assertEqual(get_watermark(self.conn, s), 0)
            self.assertEqual(publish(self.conn, s)["status"], "SUCCESS")
            self.assertEqual(self.state(s), before[s], s)

    def test_explicit_partition_backfill_repairs_a_corrupted_partition(self):
        self.land("b1.csv", [line("o1", ts="2026-01-01T10:00:00Z"), line("o2", ts="2026-01-02T10:00:00Z")])
        publish_all(self.conn)
        self.conn.execute("UPDATE orders_partition_replace SET amount_cents=1 WHERE order_id='o2'")
        wm = get_watermark(self.conn, "partition_replace")
        r = publish(self.conn, "partition_replace", partitions=["2026-01-02"])
        self.assertEqual(r["partitions_touched"], 1)
        self.assertEqual(get_watermark(self.conn, "partition_replace"), wm)   # watermark untouched
        self.assert_matches_oracle("partition_replace")

    # ---- failure & recovery ------------------------------------------------------------------
    def test_full_refresh_failure_leaves_previous_target_intact_then_retry_works(self):
        self.land("b1.csv", [line("o1")])
        publish(self.conn, "full_refresh")
        good = self.state("full_refresh")
        self.land("b2.csv", [line("o2", ts="2026-01-02T10:00:00Z")])
        r = publish(self.conn, "full_refresh", fail_after=1)
        self.assertEqual(r["status"], "FAILED")
        self.assertEqual(self.state("full_refresh"), good)                  # old data still served
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '%__next'"), 0)
        self.assertEqual(publish(self.conn, "full_refresh")["status"], "SUCCESS")
        self.assert_matches_oracle("full_refresh")

    def test_incremental_failure_rolls_back_data_and_watermark_together(self):
        self.land("b1.csv", [line("o1")])
        publish(self.conn, "incremental_merge")
        wm, good = get_watermark(self.conn, "incremental_merge"), self.state("incremental_merge")
        self.land("b2.csv", [line("o2", ts="2026-01-02T10:00:00Z")])
        r = publish(self.conn, "incremental_merge", fail_after=1)
        self.assertEqual(r["status"], "FAILED")
        self.assertEqual((get_watermark(self.conn, "incremental_merge"), self.state("incremental_merge")), (wm, good))
        self.assertEqual(publish(self.conn, "incremental_merge")["status"], "SUCCESS")
        self.assert_matches_oracle("incremental_merge")

    def test_partition_replace_partial_failure_is_resumable(self):
        self.land("b1.csv", [line("o1", ts="2026-01-01T10:00:00Z"), line("o2", ts="2026-01-02T10:00:00Z"),
                             line("o3", ts="2026-01-03T10:00:00Z")])
        r = publish(self.conn, "partition_replace", fail_after=2)   # crash after 2 of 3 partitions committed
        self.assertEqual(r["status"], "FAILED")
        self.assertEqual(get_watermark(self.conn, "partition_replace"), 0)            # not advanced
        self.assertEqual(sorted(self.state("partition_replace")), ["o1", "o2"])       # partial but consistent per partition
        retry = publish(self.conn, "partition_replace")                                # restart = just run again
        self.assertEqual(retry["status"], "SUCCESS")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders_partition_replace"), 3)
        self.assert_matches_oracle("partition_replace")

    def test_partition_replace_atomic_mode_is_all_or_nothing(self):
        self.land("b1.csv", [line("o1", ts="2026-01-01T10:00:00Z"), line("o2", ts="2026-01-02T10:00:00Z")])
        r = publish(self.conn, "partition_replace", fail_after=1, atomic_partitions=True)
        self.assertEqual(r["status"], "FAILED")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM orders_partition_replace"), 0)
        self.assertEqual(publish(self.conn, "partition_replace", atomic_partitions=True)["status"], "SUCCESS")
        self.assert_matches_oracle("partition_replace")

    def test_failed_run_is_recorded_with_reason(self):
        self.land("b1.csv", [line("o1")])
        r = publish(self.conn, "incremental_merge", fail_after=1)
        row = self.conn.execute("SELECT * FROM pipeline_runs WHERE run_id=?", (r["run_id"],)).fetchone()
        self.assertEqual(row["status"], "FAILED")
        self.assertIn("InjectedFailure", row["error"])

    def test_unknown_strategy(self):
        with self.assertRaises(ValueError):
            publish(self.conn, "nope")


if __name__ == "__main__":
    unittest.main()
