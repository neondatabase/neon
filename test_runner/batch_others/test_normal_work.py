from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, NeonPageserverHttpClient
import pytest


def check_tenant(env: NeonEnv, pageserver_http: NeonPageserverHttpClient):
    tenant_id, timeline_id = env.neon_cli.create_tenant()
    pg = env.postgres.create_start('main', tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    res_1 = pg.safe_psql_many(queries=[
        'CREATE TABLE t(key int primary key, value text)',
        'INSERT INTO t SELECT generate_series(1,100000), \'payload\'',
        'SELECT sum(key) FROM t',
    ])

    assert res_1[-1][0] == (5000050000, )
    # TODO check detach on live instance
    log.info("stopping compute")
    pg.stop()
    log.info("compute stopped")

    pg.start()
    res_2 = pg.safe_psql('SELECT sum(key) FROM t')
    assert res_2[0] == (5000050000, )

    pg.stop()
    pageserver_http.tenant_detach(tenant_id)


@pytest.mark.parametrize('num_timelines,num_safekeepers', [(3, 1)])
def test_normal_work(neon_env_builder: NeonEnvBuilder, num_timelines: int, num_safekeepers: int):
    """
    Basic test:
    * create new tenant with a timeline
    * write some data
    * ensure that it was successfully written
    * restart compute
    * check that the data is there
    * stop compute
    * detach timeline

    Repeat check for several tenants/timelines.
    """

    neon_env_builder.num_safekeepers = num_safekeepers
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    for _ in range(num_timelines):
        check_tenant(env, pageserver_http)
