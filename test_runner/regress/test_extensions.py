from logging import info

from fixtures.neon_fixtures import NeonEnv


def test_extensions(neon_simple_env: NeonEnv):
    """basic test for the extensions endpoint testing installing extensions"""

    env = neon_simple_env

    env.create_branch("test_extensions")

    endpoint = env.endpoints.create_start("test_extensions")
    extension = "neon_test_utils"
    database = "test_extensions"

    endpoint.safe_psql("CREATE DATABASE test_extensions")

    with endpoint.connect(dbname=database) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT default_version FROM pg_available_extensions WHERE name = 'neon_test_utils'"
            )
            res = cur.fetchone()
            assert res is not None
            version = res[0]

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT extname, extversion FROM pg_extension WHERE extname = 'neon_test_utils'",
            )
            res = cur.fetchone()
            assert not res, "The 'neon_test_utils' extension is installed"

    client = endpoint.http_client()
    install_res = client.extensions(extension, version, database)

    info("Extension install result: %s", res)
    assert install_res["extension"] == extension and install_res["version"] == version

    with endpoint.connect(dbname=database) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT extname, extversion FROM pg_extension WHERE extname = 'neon_test_utils'",
            )
            res = cur.fetchone()
            assert res is not None
            (db_extension_name, db_extension_version) = res

    assert db_extension_name == extension and db_extension_version == version
