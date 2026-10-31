import shutil
import tempfile
import unittest
from pathlib import Path

from loadlab.db import connect
from loadlab.reconcile import oracle_state, _target_state
from loadlab.config import TARGET_TABLES
from loadlab.strategies import get_watermark

HEADER = "order_id,customer_id,order_ts,status,amount,currency,updated_at"


def line(order_id="o1", cust="c1", ts="2026-01-01T10:00:00Z", status="created", amount="10.00",
         cur="USD", upd=None):
    return ",".join([order_id, cust, ts, status, amount, cur, upd or ts])


class DbTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="loadlab_test_"))
        self.conn = connect(self.tmp / "t.db")
        for c in ("c1", "c2"):
            self.conn.execute("INSERT INTO dim_customers VALUES (?, 'IN', NULL, 'test')", (c,))

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, name, lines, header=HEADER):
        p = self.tmp / name
        p.write_text(header + "\n" + "\n".join(lines) + ("\n" if lines else ""))
        return p

    def scalar(self, sql, *args):
        return self.conn.execute(sql, args).fetchone()[0]

    def assert_matches_oracle(self, strategy):
        expected = oracle_state(self.conn, get_watermark(self.conn, strategy))
        actual = _target_state(self.conn, TARGET_TABLES[strategy])
        self.assertEqual(expected, actual, f"{strategy} differs from oracle")
