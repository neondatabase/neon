from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from psycopg2.errors import InsufficientPrivilege

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_unstable_extensions_installation(neon_simple_env: NeonEnv):
    """
    Test that the unstable extension support within the neon extension can
    block extension installation.
    """
    env = neon_simple_env

    neon_unstable_extensions = "pg_prewarm,amcheck"

    endpoint = env.endpoints.create(
        "main",
        config_lines=[
            "neon.allow_unstable_extensions=false",
            f"neon.unstable_extensions='{neon_unstable_extensions}'",
        ],
    )
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    with endpoint.cursor() as cursor:
        cursor.execute("SELECT current_setting('neon.unstable_extensions')")
        result = cursor.fetchone()
        assert result is not None
        setting = cast("str", result[0])
        assert setting == neon_unstable_extensions

        with pytest.raises(InsufficientPrivilege):
            cursor.execute("CREATE EXTENSION pg_prewarm")

        with pytest.raises(InsufficientPrivilege):
            cursor.execute("CREATE EXTENSION amcheck")

        # Make sure that we can install a "stable" extension
        cursor.execute("CREATE EXTENSION pageinspect")

        cursor.execute("BEGIN")
        cursor.execute("SET neon.allow_unstable_extensions TO true")
        cursor.execute("CREATE EXTENSION pg_prewarm")
        cursor.execute("COMMIT")
