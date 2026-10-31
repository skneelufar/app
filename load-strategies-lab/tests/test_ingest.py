import json
import unittest

from loadlab.generator import write_edge_cases
from loadlab.ingest import ingest_file, ingest_path, load_customers
from helpers import DbTestCase, HEADER, line


class IngestTests(DbTestCase):
    def test_happy_path(self):
        p = self.write("b1.csv", [line("o1"), line("o2", cust="c2"), line("o1", status="paid", upd="2026-01-01T11:00:00Z")])
        r = ingest_file(self.conn, p)
        self.assertEqual((r["status"], r["rows_in"], r["rows_out"], r["rows_rejected"]), ("SUCCESS", 3, 3, 0))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 3)
        run = self.conn.execute("SELECT * FROM pipeline_runs WHERE run_id=?", (r["run_id"],)).fetchone()
        self.assertEqual((run["stage"], run["status"], run["rows_in"], run["rows_out"]), ("ingest", "SUCCESS", 3, 3))
        self.assertIsNotNone(run["ended_at"])

    def test_empty_batch_header_only(self):
        r = ingest_file(self.conn, self.write("empty.csv", []))
        self.assertEqual((r["status"], r["rows_in"], r["rows_out"]), ("SUCCESS", 0, 0))
        self.assertIn("empty_batch", r["warnings"])

    def test_zero_byte_file_is_empty_batch(self):
        p = self.tmp / "zero.csv"
        p.write_text("")
        r = ingest_file(self.conn, p)
        self.assertEqual((r["status"], r["rows_in"]), ("SUCCESS", 0))
        self.assertIn("empty_file", r["warnings"])

    def test_malformed_and_missing_fields_are_quarantined_with_line_numbers(self):
        p = self.write("bad.csv", [line("o1"), "o2,c1,2026-01-01T10:00:00Z", line("", cust="c1"),
                                    line("o3", amount=""), line("o4")])
        r = ingest_file(self.conn, p)
        self.assertEqual((r["rows_in"], r["rows_out"], r["rows_rejected"]), (5, 2, 3))
        rej = {(x["line_no"], x["reason"]) for x in self.conn.execute("SELECT * FROM rejects")}
        self.assertEqual(rej, {(3, "malformed_row"), (4, "missing_order_id"), (5, "missing_amount")})
        raw = json.loads(self.scalar("SELECT raw_record FROM rejects WHERE line_no=3"))
        self.assertEqual(raw[0], "o2")                      # original record preserved
        self.assertEqual(self.scalar("SELECT COUNT(DISTINCT source_file) FROM rejects"), 1)

    def test_nulls_and_blank_lines(self):
        p = self.write("nulls.csv", [line("o1", cust=""), "", line("o2")])
        r = ingest_file(self.conn, p)
        self.assertEqual((r["rows_in"], r["rows_out"], r["rows_rejected"]), (2, 1, 1))   # blank line ignored

    def test_duplicate_rows_same_file_and_across_files(self):
        r1 = ingest_file(self.conn, self.write("a.csv", [line("o1"), line("o1")]))
        self.assertEqual((r1["rows_out"], r1["reasons"]), (1, {"duplicate_event": 1}))
        r2 = ingest_file(self.conn, self.write("b.csv", [line("o1"), line("o2")]))   # different file, same event
        self.assertEqual((r2["rows_out"], r2["reasons"]), (1, {"duplicate_event": 1}))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 2)

    def test_rerun_of_same_file_is_noop(self):
        p = self.write("a.csv", [line("o1"), line("o2")])
        ingest_file(self.conn, p)
        r = ingest_file(self.conn, p)
        self.assertEqual(r["status"], "SKIPPED")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 2)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM rejects"), 0)

    def test_same_content_renamed_file_creates_no_duplicates(self):
        ingest_file(self.conn, self.write("a.csv", [line("o1")]))
        r = ingest_file(self.conn, self.write("a_copy.csv", [line("o1")]))
        self.assertEqual(r["status"], "SKIPPED")             # identical bytes => same sha256

    def test_schema_change_missing_column_fails_file_and_lands_nothing(self):
        p = self.write("bad_schema.csv", ["o1,c1,2026-01-01T10:00:00Z,created,10.00,2026-01-01T10:00:00Z"],
                       header="order_id,customer_id,order_ts,status,amount,updated_at")
        r = ingest_file(self.conn, p)
        self.assertEqual(r["status"], "FAILED")
        self.assertIn("currency", r["error"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 0)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ingested_files"), 0)   # can be re-delivered
        self.assertEqual(self.scalar("SELECT status FROM pipeline_runs WHERE run_id=?", r["run_id"]), "FAILED")

    def test_schema_change_extra_and_reordered_columns_tolerated(self):
        p = self.write("extra.csv", ["web,10.00,o1,c1,2026-01-01T10:00:00Z,created,USD,2026-01-01T10:00:00Z"],
                       header="channel,amount,order_id,customer_id,order_ts,status,currency,updated_at")
        r = ingest_file(self.conn, p)
        self.assertEqual((r["status"], r["rows_out"]), ("SUCCESS", 1))
        self.assertTrue(any("unexpected_columns_ignored:channel" in w for w in r["warnings"]))

    def test_invalid_reference_key(self):
        r = ingest_file(self.conn, self.write("a.csv", [line("o1", cust="ghost")]))
        self.assertEqual(r["reasons"], {"unknown_customer": 1})
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 0)

    def test_order_ts_is_immutable(self):
        ingest_file(self.conn, self.write("a.csv", [line("o1")]))
        r = ingest_file(self.conn, self.write("b.csv", [line("o1", ts="2026-01-05T10:00:00Z", status="paid")]))
        self.assertEqual(r["reasons"], {"order_ts_changed": 1})

    def test_partial_failure_rolls_back_and_retry_succeeds_without_duplicates(self):
        p = self.write("a.csv", [line(f"o{i}") for i in range(10)])
        r = ingest_file(self.conn, p, fail_after_rows=4)
        self.assertEqual(r["status"], "FAILED")
        self.assertIn("InjectedFailure", r["error"])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 0)       # nothing half-landed
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM ingested_files"), 0)
        r2 = ingest_file(self.conn, p)                                             # retry same file
        self.assertEqual((r2["status"], r2["rows_out"]), ("SUCCESS", 10))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_events"), 10)

    def test_non_utf8_file_fails_cleanly(self):
        p = self.tmp / "binary.csv"
        p.write_bytes(HEADER.encode() + b"\n\xff\xfe\x00bad\n")
        r = ingest_file(self.conn, p)
        self.assertEqual(r["status"], "FAILED")
        self.assertFalse(self.conn.in_transaction)

    def test_edge_case_files_do_not_crash_the_pipeline(self):
        write_edge_cases(self.tmp / "edge")
        self.conn.execute("INSERT INTO dim_customers VALUES ('cust_00000','IN',NULL,'t')")
        res = {r["source_file"]: r for r in ingest_path(self.conn, self.tmp / "edge")}
        self.assertEqual(res["edge_01_header_only.csv"]["status"], "SUCCESS")
        self.assertEqual(res["edge_02_zero_bytes.csv"]["status"], "SUCCESS")
        self.assertEqual(res["edge_03_missing_currency_column.csv"]["status"], "FAILED")
        self.assertEqual(res["edge_04_extra_column.csv"]["rows_out"], 1)
        self.assertEqual(res["edge_05_all_malformed.csv"]["rows_rejected"], 2)

    def test_load_customers_is_idempotent(self):
        p = self.tmp / "cust.csv"
        p.write_text("customer_id,region,signup_date\nc9,US,2025-01-01\n,EU,2025-01-01\n")
        r1 = load_customers(self.conn, p)
        r2 = load_customers(self.conn, p)
        self.assertEqual((r1["rows_out"], r1["rows_rejected"]), (1, 1))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM dim_customers WHERE customer_id='c9'"), 1)
        self.assertEqual(r2["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
