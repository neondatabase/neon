from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from fixtures.compute_migrations import NUM_COMPUTE_MIGRATIONS

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


MIGRATIONS: tuple[str, ...] = (
    "0001-neon_superuser_bypass_rls",
    "0002-alter_roles",
    "0003-grant_pg_create_subscription_to_neon_superuser",
    "0004-grant_pg_monitor_to_neon_superuser",
    "0005-grant_all_on_tables_to_neon_superuser",
    "0006-grant_all_on_sequences_to_neon_superuser",
    "0007-grant_all_on_tables_to_neon_superuser_with_grant_option",
    "0008-grant_all_on_sequences_to_neon_superuser_with_grant_option",
    "0009-revoke_replication_for_previously_allowed_roles",
    "0010-grant_snapshot_synchronization_funcs_to_neon_superuser",
    "0011-grant_pg_show_replication_origin_status_to_neon_superuser",
)


def test_migrations_smoke(neon_simple_env: NeonEnv, compute_migrations_dir: Path):
    """
    Test that migrations ran and succeeded, or were skipped.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)
    # Tell compute_ctl to error when the second migration is about to run, so
    # that can test the restart functionality. We would typically want to do
    # this via the HTTP endpoint, but we can't guarantee that we will be able to
    # send our HTTP request before the migrations are ran.
    endpoint.start(env={"FAILPOINTS": "the-second-compute-migration=return"})

    # Make sure that the first migration ran
    endpoint.wait_for_migrations(wait_for=1)

    # Confirm that we correctly recorded that in the neon_migration.migration_id
    # table
    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cast("int", cur.fetchall()[0][0])
        assert migration_id == 1

    endpoint.stop()

    endpoint.start()

    # Now wait for the rest of the migrations
    endpoint.wait_for_migrations()

    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cast("int", cur.fetchall()[0][0])
        assert migration_id == NUM_COMPUTE_MIGRATIONS

    for i, m in enumerate(MIGRATIONS, start=1):
        migration = (compute_migrations_dir / f"{m}.sql").read_text(encoding="utf-8")
        if not migration.startswith("-- SKIP"):
            pattern = rf"Skipping migration id={i}"
        else:
            pattern = rf"Running migration id={i}"

        endpoint.log_contains(pattern)


@pytest.mark.parametrize(
    "migration_id",
    (pytest.param(m, id=str(i)) for i, m in enumerate(MIGRATIONS, start=1)),
)
def test_migration_e2e(
    neon_simple_env: NeonEnv,
    compute_migrations_dir: Path,
    compute_migrations_test_dir: Path,
    migration_id: str,
):
    """
    Test that the migration performs as advertised.
    """
    env = neon_simple_env

    migration = (compute_migrations_dir / f"{migration_id}.sql").read_text(encoding="utf-8")
    if migration.startswith("-- SKIP"):
        pytest.skip("The migration is marked as SKIP")

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    endpoint.wait_for_migrations()

    check_query = (compute_migrations_test_dir / f"{migration_id}.sql").read_text(encoding="utf-8")
    endpoint.safe_psql(check_query)
