from logging import info

from fixtures.neon_fixtures import NeonEnv


def test_extensions(neon_simple_env: NeonEnv):
    """basic test for the extensions endpoint testing installing extensions"""

    env = neon_simple_env

    env.create_branch("test_extensions")

    endpoint = env.endpoints.create_start("test_extensions")
    extension = "pg_session_jwt"
    version = "0.0.1"
    database = "test_extensions"

    endpoint.safe_psql("CREATE DATABASE test_extensions")

    client = endpoint.http_client()

    with endpoint.connect(dbname=database) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT extname, extversion FROM pg_extension WHERE extname = pg_session_jwt",
            )
            res = cur.fetchall()
            assert not res, "The 'pg_session_jwt' extension is installed"

    res = client.extensions(extension, version, database)

    info("Extension install result: %s", res)
    assert res["extension"] == extension and res["version"] == version

    with endpoint.connect(dbname=database) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT extname, extversion FROM pg_extension WHERE extname = pg_session_jwt",
            )
            res = cur.fetchall()
            db_extension_name, db_extension_version = res

    assert db_extension_name == extension and db_extension_version == version
