from __future__ import annotations

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.pg_version import PgVersion
from fixtures.utils import wait_until


def test_neon_superuser(neon_simple_env: NeonEnv, pg_version: PgVersion):
    env = neon_simple_env
    env.create_branch("test_neon_superuser_publisher", ancestor_branch_name="main")
    pub = env.endpoints.create("test_neon_superuser_publisher")

    env.create_branch("test_neon_superuser_subscriber")
    sub = env.endpoints.create("test_neon_superuser_subscriber")

    pub.respec(skip_pg_catalog_updates=False)
    pub.start()

    sub.respec(skip_pg_catalog_updates=False)
    sub.start()

    pub.wait_for_migrations()
    sub.wait_for_migrations()

    with pub.cursor() as cur:
        cur.execute(
            "CREATE ROLE mr_whiskers WITH PASSWORD 'cat' LOGIN INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser"
        )
        cur.execute("CREATE DATABASE neondb WITH OWNER mr_whiskers")
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE neondb TO neon_superuser")

        # If we don't do this, creating the subscription will fail later on PG16
        pub.edit_hba(["host all mr_whiskers 0.0.0.0/0 md5"])

    with sub.cursor() as cur:
        cur.execute(
            "CREATE ROLE mr_whiskers WITH PASSWORD 'cat' LOGIN INHERIT CREATEROLE CREATEDB BYPASSRLS REPLICATION IN ROLE neon_superuser"
        )
        cur.execute("CREATE DATABASE neondb WITH OWNER mr_whiskers")
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE neondb TO neon_superuser")

    with pub.cursor(dbname="neondb", user="mr_whiskers", password="cat") as cur:
        cur.execute("SELECT pg_has_role('mr_whiskers', 'neon_superuser', 'member')")
        assert cur.fetchall()[0][0]
        cur.execute("SELECT pg_has_role('mr_whiskers', 'neon_superuser', 'usage')")
        assert cur.fetchall()[0][0]

        if pg_version == PgVersion.V16:
            cur.execute("SELECT pg_has_role('mr_whiskers', 'neon_superuser', 'set')")
            assert cur.fetchall()[0][0]

        cur.execute("CREATE PUBLICATION pub FOR ALL TABLES")
        cur.execute("CREATE ROLE definitely_not_a_superuser WITH PASSWORD 'nope'")
        cur.execute("CREATE DATABASE definitely_a_database")
        cur.execute("CREATE TABLE t (a int)")
        cur.execute("INSERT INTO t VALUES (10), (20)")
        cur.execute("SELECT * from t")
        res = cur.fetchall()
        assert [r[0] for r in res] == [10, 20]

    with sub.cursor(dbname="neondb", user="mr_whiskers", password="cat") as cur:
        cur.execute("CREATE TABLE t (a int)")

        pub_conn = f"host=localhost port={pub.pg_port} dbname=neondb user=mr_whiskers password=cat"
        query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
        log.info(f"Creating subscription: {query}")
        cur.execute(query)

        with pub.cursor(dbname="neondb", user="mr_whiskers", password="cat") as pcur:
            pcur.execute("INSERT INTO t VALUES (30), (40)")

        def check_that_changes_propagated():
            cur.execute("SELECT * FROM t")
            res = cur.fetchall()
            log.info(res)
            assert len(res) == 4
            assert [r[0] for r in res] == [10, 20, 30, 40]

        wait_until(check_that_changes_propagated)

        # Test that pg_monitor is working for neon_superuser role
        cur.execute("SELECT query from pg_stat_activity LIMIT 1")
        assert cur.fetchall()[0][0] != "<insufficient privilege>"
        # Test that pg_monitor is not working for non neon_superuser role without grant
        cur.execute("CREATE ROLE not_a_superuser LOGIN PASSWORD 'Password42!'")
        cur.execute("GRANT not_a_superuser TO neon_superuser WITH ADMIN OPTION")
        cur.execute("SET ROLE not_a_superuser")
        cur.execute("SELECT query from pg_stat_activity LIMIT 1")
        assert cur.fetchall()[0][0] == "<insufficient privilege>"
        cur.execute("RESET ROLE")
        # Test that pg_monitor is working for non neon_superuser role with grant
        cur.execute("GRANT pg_monitor TO not_a_superuser")
        cur.execute("SET ROLE not_a_superuser")
        cur.execute("SELECT query from pg_stat_activity LIMIT 1")
        assert cur.fetchall()[0][0] != "<insufficient privilege>"
        cur.execute("RESET ROLE")
        cur.execute("DROP ROLE not_a_superuser")
        query = "DROP SUBSCRIPTION sub CASCADE"
        log.info(f"Dropping subscription: {query}")
        cur.execute(query)
