from fixtures.neon_fixtures import NeonEnvBuilder, PgBin


# Test gc_cutoff
#
# This test sets fail point at the end of GC, and checks that pageserver
# normally restarts after it. Also, there should be GC ERRORs in the log,
# but the fixture checks the log for any unexpected ERRORs after every
# test anyway, so it doesn't need any special attention here.
def test_gc_cutoff(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start()

    # These warnings are expected, when the pageserver is restarted abruptly
    env.pageserver.allowed_errors.append(".*found future image layer.*")
    env.pageserver.allowed_errors.append(".*found future delta layer.*")

    pageserver_http = env.pageserver.http_client()

    # Use aggressive GC and checkpoint settings, so that we also exercise GC during the test
    tenant_id, _ = env.neon_cli.create_tenant(
        conf={
            "gc_period": "10 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_period": "5 s",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "1 s",
            "compaction_threshold": "3",
            "image_creation_threshold": "2",
        }
    )
    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    connstr = pg.connstr(options="-csynchronous_commit=off")
    pg_bin.run_capture(["pgbench", "-i", "-s10", connstr])

    pageserver_http.configure_failpoints(("after-timeline-gc-removed-layers", "exit"))

    for i in range(5):
        try:
            pg_bin.run_capture(["pgbench", "-N", "-c5", "-T100", "-Mprepared", connstr])
        except Exception:
            env.pageserver.stop()
            env.pageserver.start()
            pageserver_http.configure_failpoints(("after-timeline-gc-removed-layers", "exit"))
