from threading import Thread
from uuid import uuid4
import psycopg2
import pytest

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, NeonPageserverApiException


def test_tenant_detach_smoke(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # first check for non existing tenant
    tenant_id = uuid4()
    with pytest.raises(expected_exception=NeonPageserverApiException,
                       match=f'Tenant not found for id {tenant_id.hex}'):
        pageserver_http.tenant_detach(tenant_id)

    # create new nenant
    tenant_id, timeline_id = env.neon_cli.create_tenant()

    # assert tenant exists on disk
    assert (env.repo_dir / "tenants" / tenant_id.hex).exists()

    pg = env.postgres.create_start('main', tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    pg.safe_psql_many(queries=[
        'CREATE TABLE t(key int primary key, value text)',
        'INSERT INTO t SELECT generate_series(1,100000), \'payload\'',
    ])

    # gc should not try to even start
    with pytest.raises(expected_exception=psycopg2.DatabaseError,
                       match='gc target timeline does not exist'):
        env.pageserver.safe_psql(f'do_gc {tenant_id.hex} {uuid4().hex} 0')

    # try to concurrently run gc and detach
    gc_thread = Thread(
        target=lambda: env.pageserver.safe_psql(f'do_gc {tenant_id.hex} {timeline_id.hex} 0'), )
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
        pytest.fail(f"could not detach timeline: {last_error}")

    gc_thread.join(timeout=10)

    # check that nothing is left on disk for deleted tenant
    assert not (env.repo_dir / "tenants" / tenant_id.hex).exists()

    with pytest.raises(expected_exception=psycopg2.DatabaseError,
                       match=f'Tenant {tenant_id.hex} not found'):
        env.pageserver.safe_psql(f'do_gc {tenant_id.hex} {timeline_id.hex} 0')
