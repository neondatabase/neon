import time

from fixtures.neon_fixtures import NeonEnv


def test_migrations(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_migrations", "empty")

    endpoint = env.endpoints.create("test_migrations")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    num_migrations = 10
    endpoint.wait_for_migrations(num_migrations=num_migrations)

    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cur.fetchall()
        assert migration_id[0][0] == num_migrations

    endpoint.stop()
    endpoint.start()
    # We don't have a good way of knowing that the migrations code path finished executing
    # in compute_ctl in the case that no migrations are being run
    time.sleep(1)
    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cur.fetchall()
        assert migration_id[0][0] == num_migrations
