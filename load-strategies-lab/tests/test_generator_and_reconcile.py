import hashlib
import unittest
from pathlib import Path

from loadlab.cli import main
from loadlab.config import STRATEGIES
from loadlab.generator import generate
from loadlab.ingest import ingest_path, load_customers
from loadlab.reconcile import reconcile
from loadlab.strategies import publish
from helpers import DbTestCase


def tree_hash(root):
    h = hashlib.sha256()
    for p in sorted(Path(root).rglob("*.csv")):
        h.update(p.name.encode()); h.update(p.read_bytes())
    return h.hexdigest()


class GeneratorTests(DbTestCase):
    def test_same_seed_same_bytes_different_seed_differs(self):
        generate(self.tmp / "a", seed=7); generate(self.tmp / "b", seed=7); generate(self.tmp / "c", seed=8)
        self.assertEqual(tree_hash(self.tmp / "a"), tree_hash(self.tmp / "b"))
        self.assertNotEqual(tree_hash(self.tmp / "a"), tree_hash(self.tmp / "c"))

    def test_labels_are_separate_from_inputs(self):
        generate(self.tmp / "d")
        self.assertTrue((self.tmp / "d" / "labels" / "expected_rejects.csv").exists())
        for f in (self.tmp / "d" / "batches").glob("*.csv"):
            self.assertNotIn("expected", f.read_text().splitlines()[0])


class EndToEndTests(DbTestCase):
    def run_pipeline(self, **gen):
        d = self.tmp / "data"
        generate(d, **gen)
        load_customers(self.conn, d / "customers.csv")
        for f in sorted((d / "batches").glob("*.csv")):
            self.assertEqual(ingest_path(self.conn, f)[0]["status"], "SUCCESS")
            for s in STRATEGIES:
                self.assertEqual(publish(self.conn, s)["status"], "SUCCESS")
        return d

    def test_generated_rejects_match_answer_key_and_strategies_reconcile(self):
        d = self.run_pipeline(n_orders=300, n_customers=25, days=6, seed=11)
        res = reconcile(self.conn, labels_path=d / "labels" / "expected_rejects.csv")
        failed = [c for c in res["checks"] if not c["passed"]]
        self.assertEqual(failed, [])
        self.assertIsNotNone(res["labels"])
        self.assertGreater(res["labels"]["expected"], 10)
        reasons = res["totals"]["rejects_by_reason"]
        for needed in ("duplicate_event", "order_ts_changed", "malformed_row", "unknown_customer"):
            self.assertIn(needed, reasons)

    def test_ten_runs_of_the_same_inputs_converge(self):
        d = self.run_pipeline(n_orders=80, days=3, seed=3)
        snap = reconcile(self.conn)["strategies"]
        for _ in range(3):
            ingest_path(self.conn, d / "batches")      # all SKIPPED
            for s in STRATEGIES:
                publish(self.conn, s)
        self.assertEqual(reconcile(self.conn)["strategies"], snap)

    def test_reconcile_detects_tampering(self):
        self.run_pipeline(n_orders=60, days=3, seed=5)
        self.assertTrue(reconcile(self.conn)["passed"])
        self.conn.execute("UPDATE orders_incremental_merge SET amount_cents = amount_cents + 1 "
                          "WHERE order_id = (SELECT MIN(order_id) FROM orders_incremental_merge)")
        res = reconcile(self.conn)
        self.assertFalse(res["passed"])
        self.assertTrue(any("incremental_merge" in c["name"] and not c["passed"] for c in res["checks"]))

    def test_reconcile_detects_orphan_reference(self):
        self.run_pipeline(n_orders=40, days=2, seed=9)
        self.conn.execute("DELETE FROM dim_customers WHERE customer_id = (SELECT MIN(customer_id) FROM orders_full_refresh)")
        res = reconcile(self.conn)
        self.assertTrue(any("referential integrity" in c["name"] and not c["passed"] for c in res["checks"]))

    def test_cli_demo_runs_clean(self):
        d = self.tmp / "cli_data"
        self.assertEqual(main(["--log-level", "ERROR", "generate", "--out", str(d), "--orders", "60", "--days", "3"]), 0)
        rc = main(["--log-level", "ERROR", "demo", "--data", str(d), "--db", str(self.tmp / "demo.db"),
                   "--out", str(self.tmp / "out"), "--fresh"])
        self.assertEqual(rc, 0)
        self.assertTrue((self.tmp / "out" / "reconciliation_report.md").exists())


if __name__ == "__main__":
    unittest.main()
