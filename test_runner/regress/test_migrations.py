import time

from fixtures.neon_fixtures import NeonEnv


def test_migrations(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_migrations", "empty")

    endpoint = env.endpoints.create("test_migrations")
    log_path = endpoint.endpoint_path() / "compute.log"

    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    endpoint.wait_for_migrations()

    num_migrations = 3

    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cur.fetchall()
        assert migration_id[0][0] == num_migrations

    with open(log_path, "r") as log_file:
        logs = log_file.read()
        assert "INFO handle_migrations: Ran 3 migrations" in logs

    endpoint.stop()
    endpoint.start()
    # We don't have a good way of knowing that the migrations code path finished executing
    # in compute_ctl in the case that no migrations are being run
    time.sleep(1)
    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cur.fetchall()
        assert migration_id[0][0] == num_migrations

    with open(log_path, "r") as log_file:
        logs = log_file.read()
        assert "INFO handle_migrations: Ran 0 migrations" in logs
