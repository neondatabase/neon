import pytest
from contextlib import closing
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.benchmark_fixture import NeonBenchmarker


# This test sometimes runs for longer than the global 5 minute timeout.
@pytest.mark.timeout(600)
def test_startup(neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    # Start
    env.neon_cli.create_branch('test_startup')
    with zenbenchmark.record_duration("startup_time"):
        pg = env.postgres.create_start('test_startup')
        pg.safe_psql("select 1;")

    # Restart
    pg.stop_and_destroy()
    with zenbenchmark.record_duration("restart_time"):
        pg.create_start('test_startup')
        pg.safe_psql("select 1;")

    # Fill up
    num_rows = 1000000  # 30 MB
    num_tables = 100
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            for i in range(num_tables):
                cur.execute(f'create table t_{i} (i integer);')
                cur.execute(f'insert into t_{i} values (generate_series(1,{num_rows}));')

    # Read
    with zenbenchmark.record_duration("read_time"):
        pg.safe_psql("select * from t_0;")

    # Read again
    with zenbenchmark.record_duration("second_read_time"):
        pg.safe_psql("select * from t_0;")

    # Restart
    pg.stop_and_destroy()
    with zenbenchmark.record_duration("restart_with_data"):
        pg.create_start('test_startup')
        pg.safe_psql("select 1;")

    # Read
    with zenbenchmark.record_duration("read_after_restart"):
        pg.safe_psql("select * from t_0;")
