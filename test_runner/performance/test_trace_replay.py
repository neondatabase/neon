from fixtures.neon_fixtures import NeonEnvBuilder, ReplayBin


def test_trace_replay(neon_env_builder: NeonEnvBuilder, replay_bin: ReplayBin):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "trace_read_requests": "true",
        }
    )
    timeline = env.neon_cli.create_timeline("test_trace_replay", tenant_id=tenant)
    pg = env.postgres.create_start("test_trace_replay", "main", tenant)

    pg.safe_psql("select 1;")

    trace_path = env.repo_dir / "traces" / str(tenant) / str(timeline) / str(timeline)
    assert trace_path.exists()

    print("replaying")
    ps_connstr = env.pageserver.connstr().replace("'", "\\'")
    output = replay_bin.replay_all(ps_connstr)
    print(output)
