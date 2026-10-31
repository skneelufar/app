"""Row-level schema + business-rule validation. Pure functions, no I/O."""
import hashlib
import re
from datetime import datetime, timezone
from decimal import Decimal

from .config import (MAX_AMOUNT_CENTS, REQUIRED_COLUMNS, VALID_CURRENCIES, VALID_STATUSES)

ORDER_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
AMOUNT_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")
CENT = Decimal("0.01")


def parse_ts(value: str) -> datetime:
    """ISO-8601 -> aware UTC datetime. Naive timestamps are assumed to be UTC."""
    s = value.strip()
    if s[-1:] in ("Z", "z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_amount_cents(value: str) -> int:
    """'12.34' -> 1234. Max 2 decimal places; no exponents, NaN or thousands separators."""
    s = value.strip()
    if not AMOUNT_RE.match(s):
        raise ValueError(f"not a decimal amount: {value!r}")
    d = Decimal(s)
    if d != d.quantize(CENT):
        raise ValueError(f"more than 2 decimal places: {value!r}")
    return int((d * 100).to_integral_value())


def compute_event_id(ev: dict) -> str:
    """Deterministic ID of an event's normalized payload (same input -> same id, always)."""
    payload = "|".join(str(ev[k]) for k in (
        "order_id", "customer_id", "order_ts", "status", "amount_cents", "currency", "updated_at"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def validate_row(raw: dict, known_customers):
    """Return (event_dict, None) if valid, else (None, reject_reason). First failing rule wins."""
    f = {c: (raw.get(c) or "").strip() for c in REQUIRED_COLUMNS}
    for col in REQUIRED_COLUMNS:
        if not f[col]:
            return None, f"missing_{col}"
    if not ORDER_ID_RE.match(f["order_id"]):
        return None, "invalid_order_id"
    try:
        order_ts = parse_ts(f["order_ts"])
    except ValueError:
        return None, "invalid_order_ts"
    try:
        updated_at = parse_ts(f["updated_at"])
    except ValueError:
        return None, "invalid_updated_at"
    status = f["status"].lower()
    if status not in VALID_STATUSES:
        return None, "invalid_status"
    try:
        amount_cents = parse_amount_cents(f["amount"])
    except ValueError:
        return None, "invalid_amount"
    if amount_cents < 0:
        return None, "negative_amount"
    if amount_cents > MAX_AMOUNT_CENTS:
        return None, "amount_out_of_range"
    currency = f["currency"].upper()
    if currency not in VALID_CURRENCIES:
        return None, "invalid_currency"
    if updated_at < order_ts:
        return None, "updated_before_order_ts"
    if f["customer_id"] not in known_customers:
        return None, "unknown_customer"
    ev = {
        "order_id": f["order_id"], "customer_id": f["customer_id"],
        "order_ts": fmt_ts(order_ts), "order_date": fmt_ts(order_ts)[:10],
        "status": status, "amount_cents": amount_cents, "currency": currency,
        "updated_at": fmt_ts(updated_at),
    }
    ev["event_id"] = compute_event_id(ev)
    return ev, None
