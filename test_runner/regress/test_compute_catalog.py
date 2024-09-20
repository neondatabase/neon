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
