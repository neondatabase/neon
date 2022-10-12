from threading import Thread

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    NeonPageserverApiException,
    NeonPageserverHttpClient,
)
from fixtures.types import TenantId, TimelineId


def do_gc_target(
    pageserver_http: NeonPageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    """Hack to unblock main, see https://github.com/neondatabase/neon/issues/2211"""
    try:
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)
    except Exception as e:
        log.error("do_gc failed: %s", e)


def test_tenant_detach_smoke(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # first check for non existing tenant
    tenant_id = TenantId.generate()
    with pytest.raises(
        expected_exception=NeonPageserverApiException,
        match=f"Tenant not found for id {tenant_id}",
    ):
        pageserver_http.tenant_detach(tenant_id)

    # create new nenant
    tenant_id, timeline_id = env.neon_cli.create_tenant()

    # assert tenant exists on disk
    assert (env.repo_dir / "tenants" / str(tenant_id)).exists()

    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    pg.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        ]
    )

    # gc should not try to even start
    with pytest.raises(
        expected_exception=NeonPageserverApiException, match="gc target timeline does not exist"
    ):
        bogus_timeline_id = TimelineId.generate()
        pageserver_http.timeline_gc(tenant_id, bogus_timeline_id, 0)

    # try to concurrently run gc and detach
    gc_thread = Thread(target=lambda: do_gc_target(pageserver_http, tenant_id, timeline_id))
    gc_thread.start()

    last_error = None
    for i in range(3):
        try:
            pageserver_http.tenant_detach(tenant_id)
        except Exception as e:
            last_error = e
            log.error(f"try {i} error detaching tenant: {e}")
            continue
        else:
            break
    # else is called if the loop finished without reaching "break"
    else:
        pytest.fail(f"could not detach tenant: {last_error}")

    gc_thread.join(timeout=10)

    # check that nothing is left on disk for deleted tenant
    assert not (env.repo_dir / "tenants" / str(tenant_id)).exists()

    with pytest.raises(
        expected_exception=NeonPageserverApiException, match=f"Tenant {tenant_id} not found"
    ):
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)
