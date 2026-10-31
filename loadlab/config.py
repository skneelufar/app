"""Central configuration and business-rule constants."""

REQUIRED_COLUMNS = (
    "order_id", "customer_id", "order_ts", "status", "amount", "currency", "updated_at",
)
VALID_STATUSES = frozenset({"created", "paid", "shipped", "cancelled", "refunded"})
REVENUE_STATUSES = ("paid", "shipped")          # net_amount_cents counts only these
VALID_CURRENCIES = frozenset({"USD", "EUR", "INR"})
MAX_AMOUNT_CENTS = 100_000_000                  # 1,000,000.00 (inclusive upper bound)

STRATEGIES = ("full_refresh", "incremental_merge", "partition_replace")
TARGET_TABLES = {s: f"orders_{s}" for s in STRATEGIES}

# Business columns compared across strategies (run lineage columns excluded).
BUSINESS_COLUMNS = (
    "order_id", "customer_id", "order_ts", "order_date", "status", "amount_cents",
    "net_amount_cents", "currency", "updated_at", "event_id",
)
