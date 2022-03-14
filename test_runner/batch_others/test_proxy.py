import pytest


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1;")


@pytest.mark.xfail  # Proxy eats the extra connection options
def test_proxy_options(static_proxy):
    schema_name = "tmp_schema_1"
    with static_proxy.connect(schema=schema_name) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW search_path;")
            search_path = cur.fetchall()[0][0]
            assert schema_name == search_path
