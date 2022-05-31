from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder, ZenithPageserverHttpClient


def check_tenant(env: ZenithEnv, pageserver_http: ZenithPageserverHttpClient):
    tenant_id, timeline_id = env.zenith_cli.create_tenant()
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
    pageserver_http.timeline_detach(tenant_id, timeline_id)


def test_normal_work(zenith_env_builder: ZenithEnvBuilder):
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

    env = zenith_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    for _ in range(3):
        check_tenant(env, pageserver_http)
