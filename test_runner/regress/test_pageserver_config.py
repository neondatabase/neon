import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    last_flush_lsn_upload,
)


@pytest.mark.parametrize("kind", ["sync", "async"])
def test_walredo_process_kind_config(neon_env_builder: NeonEnvBuilder, kind: str):
    neon_env_builder.pageserver_init_overrides = f"walredo_process_kind = '{kind}'"
    # ensure it starts
    env = neon_env_builder.init_start()
    # ensure the metric is set
    ps_http = env.pageserver.http_client()
    metrics = ps_http.get_metrics()
    samples = metrics.query_all("pageserver_wal_redo_process_kind")
    assert [(s.labels, s.value) for s in samples] == [({"kind": kind}, 1)]
    # ensure default tenant's config kind matches
    # => write some data to force-spawn walredo
    ep = env.endpoints.create_start("main")
    with ep.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("create table foo(bar text)")
            cur.execute("insert into foo select from generate_series(1, 100)")
    last_flush_lsn_upload(env, ep, env.initial_tenant, env.initial_timeline)
    ep.stop()
    ep.start()
    with ep.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from foo")
            [(count,)] = cur.fetchall()
            assert count == 100

    status = ps_http.tenant_status(env.initial_tenant)
    assert status["walredo"]["process"]["kind"] == kind
