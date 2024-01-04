from fixtures.neon_fixtures import NeonEnv


def test_migrations(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_migrations", "empty")
    endpoint = env.endpoints.create("test_migrations")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cur.fetchall()
        assert migration_id[0][0] == 1

    endpoint.stop()
    endpoint.start()
    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cur.fetchall()
        assert migration_id[0][0] == 1

    log_path = endpoint.endpoint_path() / "compute.log"
    with open(log_path, "r") as log_file:
        logs = log_file.read()
        assert "INFO start_compute:apply_config:handle_migrations: Ran 1 migrations" in logs
        assert "INFO start_compute:apply_config:handle_migrations: Ran 0 migrations" in logs
