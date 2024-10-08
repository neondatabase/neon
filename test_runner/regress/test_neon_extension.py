from __future__ import annotations

import time
from contextlib import closing

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


# Verify that the neon extension is installed and has the correct version.
def test_neon_extension(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.create_branch("test_create_extension_neon")

    endpoint_main = env.endpoints.create("test_create_extension_neon")
    # don't skip pg_catalog updates - it runs CREATE EXTENSION neon
    endpoint_main.respec(skip_pg_catalog_updates=False)
    endpoint_main.start()

    with closing(endpoint_main.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT extversion from pg_extension where extname='neon'")
            # If this fails, it means the extension is either not installed
            # or was updated and the version is different.
            #
            # IMPORTANT:
            # If the version has changed, the test should be updated.
            # Ensure that the default version is also updated in the neon.control file
            assert cur.fetchone() == ("1.5",)
            cur.execute("SELECT * from neon.NEON_STAT_FILE_CACHE")
            res = cur.fetchall()
            log.info(res)
            assert len(res) == 1
            assert len(res[0]) == 5


# Verify that the neon extension can be upgraded/downgraded.
def test_neon_extension_compatibility(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.create_branch("test_neon_extension_compatibility")

    endpoint_main = env.endpoints.create("test_neon_extension_compatibility")
    # don't skip pg_catalog updates - it runs CREATE EXTENSION neon
    endpoint_main.respec(skip_pg_catalog_updates=False)
    endpoint_main.start()

    with closing(endpoint_main.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT extversion from pg_extension where extname='neon'")
            # IMPORTANT:
            # If the version has changed, the test should be updated.
            # Ensure that the default version is also updated in the neon.control file
            assert cur.fetchone() == ("1.5",)
            cur.execute("SELECT * from neon.NEON_STAT_FILE_CACHE")
            all_versions = ["1.5", "1.4", "1.3", "1.2", "1.1", "1.0"]
            current_version = "1.5"
            for idx, begin_version in enumerate(all_versions):
                for target_version in all_versions[idx + 1 :]:
                    if current_version != begin_version:
                        cur.execute(
                            f"ALTER EXTENSION neon UPDATE TO '{begin_version}'; -- {current_version}->{begin_version}"
                        )
                        current_version = begin_version
                    # downgrade
                    cur.execute(
                        f"ALTER EXTENSION neon UPDATE TO '{target_version}'; -- {begin_version}->{target_version}"
                    )
                    # upgrade
                    cur.execute(
                        f"ALTER EXTENSION neon UPDATE TO '{begin_version}'; -- {target_version}->{begin_version}"
                    )


# Verify that the neon extension can be auto-upgraded to the latest version.
def test_neon_extension_auto_upgrade(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.create_branch("test_neon_extension_auto_upgrade")

    endpoint_main = env.endpoints.create("test_neon_extension_auto_upgrade")
    # don't skip pg_catalog updates - it runs CREATE EXTENSION neon
    endpoint_main.respec(skip_pg_catalog_updates=False)
    endpoint_main.start()

    with closing(endpoint_main.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER EXTENSION neon UPDATE TO '1.0';")
            cur.execute("SELECT extversion from pg_extension where extname='neon'")
            assert cur.fetchone() == ("1.0",)  # Ensure the extension gets downgraded

    endpoint_main.stop()
    time.sleep(1)
    endpoint_main.start()
    time.sleep(1)

    with closing(endpoint_main.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT extversion from pg_extension where extname='neon'")
            assert cur.fetchone() != ("1.0",)  # Ensure the extension gets upgraded
