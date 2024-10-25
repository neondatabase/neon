from __future__ import annotations

from enum import StrEnum

from typing_extensions import override

"""
This fixture is used to determine which version of Postgres to use for tests.
"""


# Inherit PgVersion from str rather than int to make it easier to pass as a command-line argument
class PgVersion(StrEnum):
    V14 = "14"
    V15 = "15"
    V16 = "16"
    V17 = "17"

    # Postgres Version for tests that uses `fixtures.utils.run_only_on_default_postgres`
    DEFAULT = V17

    # Instead of making version an optional parameter in methods, we can use this fake entry
    # to explicitly rely on the default server version (could be different from pg_version fixture value)
    NOT_SET = "<-POSTRGRES VERSION IS NOT SET->"

    # Make it less confusing in logs
    @override
    def __repr__(self) -> str:
        return f"'{self.value}'"

    @override
    def __str__(self) -> str:
        return self.value

    # In GitHub workflows we use Postgres version with v-prefix (e.g. v14 instead of just 14),
    # sometime we need to do so in tests.
    @property
    def v_prefixed(self) -> str:
        return f"v{self.value}"

    @classmethod
    @override
    def _missing_(cls, value: object) -> PgVersion | None:
        known_values = {v.value for _, v in cls.__members__.items()}

        # Allow passing version as a string with "v" prefix (e.g. "v14")
        if isinstance(value, str) and value.lower().startswith("v") and value[1:] in known_values:
            return cls(value[1:])
        # Allow passing version as an int (e.g. 15 or 150002, both will be converted to PgVersion.V15)
        elif isinstance(value, int) and str(value)[:2] in known_values:
            return cls(str(value)[:2])

        # Make mypy happy
        # See https://github.com/python/mypy/issues/3974
        return None
