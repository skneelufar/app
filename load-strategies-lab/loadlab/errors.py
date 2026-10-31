class SchemaError(Exception):
    """Input file does not match the expected schema (whole file is refused)."""


class InjectedFailure(Exception):
    """Deliberate crash used to demonstrate and test partial-failure recovery."""


class FailureInjector:
    """Raises InjectedFailure when the N-th unit of work completes.

    full_refresh / incremental_merge: the single unit is "just before commit".
    partition_replace: one unit per partition, ticked right after its commit.
    """

    def __init__(self, fail_after=None):
        self.fail_after = fail_after
        self.count = 0

    def tick(self, label=""):
        self.count += 1
        if self.fail_after is not None and self.count >= self.fail_after:
            raise InjectedFailure(f"injected failure after unit {self.count} ({label})")
