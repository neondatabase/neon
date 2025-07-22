from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from fixtures.log_helper import log
from fixtures.paths import BASE_DIR

if TYPE_CHECKING:
    from pathlib import Path

    from fixtures.neon_fixtures import (
        NeonEnvBuilder,
    )
    from fixtures.pg_version import PgVersion


# use neon_env_builder_local fixture to override the default neon_env_builder fixture
# and use a test-specific pg_install instead of shared one
@pytest.fixture(scope="function")
def neon_env_builder_event_trigger_extension(
    neon_env_builder_local: NeonEnvBuilder,
    test_output_dir: Path,
    pg_version: PgVersion,
) -> NeonEnvBuilder:
    test_local_pginstall = test_output_dir / "pg_install"

    # Now copy the SQL only extension test_event_trigger_extension in the local
    # pginstall extension directory on-disk
    test_event_trigger_extension_dir = (
        BASE_DIR / "test_runner" / "regress" / "data" / "test_event_trigger_extension"
    )

    test_local_extension_dir = (
        test_local_pginstall / f"v{pg_version}" / "share" / "postgresql" / "extension"
    )

    log.info(f"copy {test_event_trigger_extension_dir} to {test_local_extension_dir}")

    for f in [
        test_event_trigger_extension_dir / "test_event_trigger_extension.control",
        test_event_trigger_extension_dir / "test_event_trigger_extension--1.0.sql",
    ]:
        shutil.copy(f, test_local_extension_dir)

    return neon_env_builder_local


def test_event_trigger_extension(neon_env_builder_event_trigger_extension: NeonEnvBuilder):
    """
    Test installing an extension that contains an Event Trigger.

    The Event Trigger function is owned by the extension owner, which at
    CREATE EXTENSION is going to be the Postgres bootstrap user, per the
    extension control file where both superuser = true and trusted = true.

    Also this function is SECURTY DEFINER, to allow for making changes to
    the extension SQL objects, in our case a sequence.

    This test makes sure that the event trigger function is fired correctly
    by non-privileged user DDL actions such as CREATE TABLE.
    """
    env = neon_env_builder_event_trigger_extension.init_start()
    env.create_branch("test_event_trigger_extension")

    endpoint = env.endpoints.create_start("test_event_trigger_extension")
    extension = "test_event_trigger_extension"
    database = "test_event_trigger_extension"

    endpoint.safe_psql(f"CREATE DATABASE {database}")
    endpoint.safe_psql(f"CREATE EXTENSION {extension}", dbname=database)

    # check that the extension is owned by the bootstrap superuser (cloud_admin)
    pg_bootstrap_superuser_name = "cloud_admin"
    with endpoint.connect(dbname=database) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                f"select rolname from pg_roles r join pg_extension e on r.oid = e.extowner where extname = '{extension}'"
            )
            owner = cast("tuple[str]", cur.fetchone())[0]
            assert owner == pg_bootstrap_superuser_name, (
                f"extension {extension} is not owned by bootstrap user '{pg_bootstrap_superuser_name}'"
            )

    # test that the SQL-only Event Trigger (SECURITY DEFINER function) runs
    # correctly now that the extension has been installed
    #
    # create table to trigger the event trigger, twice, check sequence count
    with endpoint.connect(dbname=database) as pg_conn:
        log.info("creating SQL objects (tables)")
        with pg_conn.cursor() as cur:
            cur.execute("CREATE TABLE foo1(id int primary key)")
            cur.execute("CREATE TABLE foo2(id int)")

            cur.execute("SELECT event_trigger.get_schema_version()")
            res = cast("tuple[int]", cur.fetchone())
            ver = res[0]

            log.info(f"schema version is now {ver}")
            assert ver == 2, "schema version is not 2"
