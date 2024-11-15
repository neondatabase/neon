from __future__ import annotations

from typing import TYPE_CHECKING, cast

from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fixtures.neon_fixtures import NeonEnv


def test_default_locales(neon_simple_env: NeonEnv):
    """
    Test that the default locales for compute databases is C.UTF-8.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    domain_locales = cast(
        "Sequence[str]",
        endpoint.safe_psql(
            "SELECT current_setting('lc_messages') AS lc_messages,"
            + "current_setting('lc_monetary') AS lc_monetary,"
            + "current_setting('lc_numeric') AS lc_numeric,"
            + "current_setting('lc_time') AS lc_time"
        )[0],
    )
    for dl in domain_locales:
        assert dl == "C.UTF-8"

    # Postgres 15 added the locale providers
    if env.pg_version < PgVersion.V15:
        results = cast(
            "Sequence[str]",
            endpoint.safe_psql(
                "SELECT datcollate, datctype FROM pg_database WHERE datname = current_database()"
            )[0],
        )

        datcollate = results[0]
        datctype = results[1]
    else:
        results = cast(
            "Sequence[str]",
            endpoint.safe_psql(
                "SELECT datlocprovider, datcollate, datctype FROM pg_database WHERE datname = current_database()"
            )[0],
        )
        datlocprovider = results[0]
        datcollate = results[1]
        datctype = results[2]

        if env.pg_version >= PgVersion.V17:
            assert datlocprovider == "b", "The locale provider is not builtin"
        else:
            assert datlocprovider == "c", "The locale provider is not libc"

    assert datcollate == "C.UTF-8"
    assert datctype == "C.UTF-8"
