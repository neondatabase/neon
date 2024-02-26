from contextlib import closing

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


# Verify that the neon extension is installed and has the correct version.
def test_neon_extension(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_create_extension_neon")

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
            assert cur.fetchone() == ("1.2",)
            cur.execute("SELECT * from neon.NEON_STAT_FILE_CACHE")
            res = cur.fetchall()
            log.info(res)
            assert len(res) == 1
            assert len(res[0])==5
