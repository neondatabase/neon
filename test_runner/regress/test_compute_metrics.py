from fixtures.neon_fixtures import NeonEnv


def test_compute_metrics(neon_simple_env: NeonEnv):
    """
    Test compute metrics, exposed in the neon_backend_perf_counters and
    neon_perf_counters views
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    # We don't check that the values make sense, this is just a very
    # basic check that the server doesn't crash or something like that.
    #
    # 1.5 is the minimum version to contain these views.
    cur.execute("CREATE EXTENSION neon VERSION '1.5'")
    cur.execute("SELECT * FROM neon_perf_counters")
    cur.execute("SELECT * FROM neon_backend_perf_counters")
