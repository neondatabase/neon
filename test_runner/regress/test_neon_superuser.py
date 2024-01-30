import time

from fixtures.neon_fixtures import NeonEnv
from fixtures.pg_version import PgVersion


def test_neon_superuser(neon_simple_env: NeonEnv, pg_version: PgVersion):
    env = neon_simple_env
    env.neon_cli.create_branch("test_neon_superuser", "empty")
    endpoint = env.endpoints.create("test_neon_superuser")
    endpoint.respec(skip_pg_catalog_updates=False, features=["migrations"])
    endpoint.start()

    time.sleep(1)  # Sleep to let migrations run

    with endpoint.cursor() as cur:
        cur.execute(
            "CREATE ROLE mr_whiskers WITH PASSWORD 'cat' LOGIN INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser"
        )
        cur.execute("CREATE DATABASE neondb WITH OWNER mr_whiskers")
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE neondb TO neon_superuser")

    with endpoint.cursor(dbname="neondb", user="mr_whiskers", password="cat") as cur:
        cur.execute("SELECT pg_has_role('mr_whiskers', 'neon_superuser', 'member')")
        assert cur.fetchall()[0][0]
        cur.execute("SELECT pg_has_role('mr_whiskers', 'neon_superuser', 'usage')")
        assert cur.fetchall()[0][0]

        if pg_version == PgVersion.V16:
            cur.execute("SELECT pg_has_role('mr_whiskers', 'neon_superuser', 'set')")
            assert cur.fetchall()[0][0]

        cur.execute("CREATE PUBLICATION pub FOR ALL TABLES")
        cur.execute("CREATE ROLE definitely_not_a_superuser WITH PASSWORD 'nope'")
