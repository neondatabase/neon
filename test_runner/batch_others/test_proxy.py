import pytest


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1;", options="project=generic-project-name")


# Pass extra options to the server.
#
# Currently, proxy eats the extra connection options, so this fails.
# See https://github.com/neondatabase/neon/issues/1287
@pytest.mark.xfail
def test_proxy_options(static_proxy):
    with static_proxy.connect(options="-cproxytest.option=value") as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW proxytest.option;")
            value = cur.fetchall()[0][0]
            assert value == 'value'
