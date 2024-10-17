import psycopg2
from fixtures.neon_fixtures import NeonEnv


def test_role_grants(neon_simple_env: NeonEnv):
    """basic test for the endpoint that grants permissions for a role against a schema"""

    env = neon_simple_env

    env.create_branch("test_role_grants")

    endpoint = env.endpoints.create_start("test_role_grants")

    endpoint.safe_psql("CREATE DATABASE test_role_grants")
    endpoint.safe_psql("CREATE SCHEMA IF NOT EXISTS test_schema", dbname="test_role_grants")
    endpoint.safe_psql("CREATE ROLE test_role WITH LOGIN", dbname="test_role_grants")

    # confirm we do not yet have access
    pg_conn = endpoint.connect(dbname="test_role_grants", user="test_role")
    with pg_conn.cursor() as cur:
        try:
            cur.execute('CREATE TABLE "test_schema"."test_table" (id integer primary key)')
            raise ValueError("create table should not succeed")
        except psycopg2.errors.InsufficientPrivilege:
            pass
        except BaseException as e:
            raise e

    client = endpoint.http_client()
    res = client.set_role_grants(
        "test_role_grants", "test_role", "test_schema", ["CREATE", "USAGE"]
    )

    # confirm we have access
    with pg_conn.cursor() as cur:
        cur.execute('CREATE TABLE "test_schema"."test_table" (id integer primary key)')
        cur.execute('INSERT INTO "test_schema"."test_table" (id) VALUES (1)')
        cur.execute('SELECT id from "test_schema"."test_table"')
        res = cur.fetchall()

        assert res == [(1,)], "select should not succeed"
