from __future__ import annotations

import requests
from fixtures.neon_fixtures import NeonEnv


def test_compute_catalog(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main", config_lines=["log_min_messages=debug1"])
    client = endpoint.http_client()

    objects = client.dbs_and_roles()

    # Assert that 'cloud_admin' role exists in the 'roles' list
    assert any(
        role["name"] == "cloud_admin" for role in objects["roles"]
    ), "The 'cloud_admin' role is missing"

    # Assert that 'postgres' database exists in the 'databases' list
    assert any(
        db["name"] == "postgres" for db in objects["databases"]
    ), "The 'postgres' database is missing"

    ddl = client.database_schema(database="postgres")

    assert "-- PostgreSQL database dump" in ddl

    try:
        client.database_schema(database="nonexistentdb")
        raise AssertionError("Expected HTTPError was not raised")
    except requests.exceptions.HTTPError as e:
        assert (
            e.response.status_code == 404
        ), f"Expected 404 status code, but got {e.response.status_code}"


def test_compute_create_databases(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can create and work with databases with special
    characters (whitespaces, %, tabs) in the name.
    """
    env = neon_simple_env

    # Create and start endpoint so that neon_local put all the generated
    # stuff into the spec.json file.
    endpoint = env.endpoints.create_start("main")

    databases = [
        {
            "name": "neondb",
            "owner": "cloud_admin",
        },
        {
            "name": "db with spaces",
            "owner": "cloud_admin",
        },
        {
            "name": "db with%20spaces ",
            "owner": "cloud_admin",
        },
        {
            "name": "db with whitespaces	",
            "owner": "cloud_admin",
        },
    ]

    # Update the spec.json file to include new databases
    # and reconfigure the endpoint to apply the changes.
    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "cluster": {
                "databases": databases,
            },
        }
    )
    endpoint.reconfigure()

    for db in databases:
        # Check that database has a correct name in the system catalog
        catalog_db = endpoint.safe_psql(
            f"SELECT datname FROM pg_database WHERE datname = '{db['name']}'"
        )
        assert catalog_db is not None
        assert len(catalog_db) == 1
        assert catalog_db[0][0] == db["name"]

        # Check that we can connect to this database without any issues
        with endpoint.cursor(dbname=db["name"]) as cursor:
            cursor.execute("SELECT * FROM current_database()")
            curr_db = cursor.fetchone()
            assert curr_db is not None
            assert len(curr_db) == 1
            assert curr_db[0] == db["name"]
