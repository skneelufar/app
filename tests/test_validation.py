import unittest

from loadlab.validate import compute_event_id, parse_amount_cents, parse_ts, validate_row

KNOWN = {"c1"}


def good(**over):
    r = {"order_id": "o1", "customer_id": "c1", "order_ts": "2026-01-01T10:00:00Z", "status": "paid",
         "amount": "10.50", "currency": "usd", "updated_at": "2026-01-01T11:00:00+00:00"}
    r.update(over)
    return r


class ValidationTests(unittest.TestCase):
    def test_valid_row_is_normalized(self):
        ev, reason = validate_row(good(status=" PAID "), KNOWN)
        self.assertIsNone(reason)
        self.assertEqual((ev["status"], ev["currency"], ev["amount_cents"], ev["order_date"]),
                         ("paid", "USD", 1050, "2026-01-01"))
        self.assertEqual(ev["updated_at"], "2026-01-01T11:00:00Z")

    def test_event_id_is_deterministic(self):
        a, _ = validate_row(good(), KNOWN)
        b, _ = validate_row(good(), KNOWN)
        self.assertEqual(a["event_id"], b["event_id"])
        self.assertEqual(a["event_id"], compute_event_id(a))
        c, _ = validate_row(good(amount="10.51"), KNOWN)
        self.assertNotEqual(a["event_id"], c["event_id"])

    def test_reject_reasons(self):
        cases = {
            "missing_order_id": good(order_id=""), "missing_customer_id": good(customer_id=" "),
            "missing_amount": good(amount=""), "missing_currency": good(currency=""),
            "invalid_order_id": good(order_id="a b"), "invalid_order_ts": good(order_ts="yesterday"),
            "invalid_updated_at": good(updated_at="2026-02-30T00:00:00Z"),
            "invalid_status": good(status="lost"), "invalid_amount": good(amount="1,000.00"),
            "negative_amount": good(amount="-0.01"), "amount_out_of_range": good(amount="1000000.01"),
            "invalid_currency": good(currency="GBP"),
            "updated_before_order_ts": good(updated_at="2026-01-01T09:59:59Z"),
            "unknown_customer": good(customer_id="zzz"),
        }
        for expected, row in cases.items():
            with self.subTest(expected):
                ev, reason = validate_row(row, KNOWN)
                self.assertIsNone(ev)
                self.assertEqual(reason, expected)

    def test_amount_boundaries(self):
        self.assertEqual(parse_amount_cents("0.00"), 0)            # lower boundary valid
        self.assertEqual(parse_amount_cents("1000000.00"), 100_000_000)  # upper boundary valid
        self.assertEqual(parse_amount_cents("7"), 700)
        self.assertEqual(parse_amount_cents("10.990"), 1099)
        for bad in ("10.999", "1e3", "NaN", "", "1_000", "$5"):
            with self.subTest(bad):
                with self.assertRaises(ValueError):
                    parse_amount_cents(bad)
        ev, reason = validate_row(good(amount="1000000.00"), KNOWN)
        self.assertIsNone(reason)

    def test_timestamps_normalized_to_utc(self):
        self.assertEqual(parse_ts("2026-01-01T10:00:00+05:30").hour, 4)
        self.assertEqual(parse_ts("2026-01-01T10:00:00").hour, 10)   # naive == UTC
        ev, _ = validate_row(good(order_ts="2026-01-01T23:30:00-02:00", updated_at="2026-01-02T05:00:00Z"), KNOWN)
        self.assertEqual(ev["order_date"], "2026-01-02")             # date is the UTC date


if __name__ == "__main__":
    unittest.main()
