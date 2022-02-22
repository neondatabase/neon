

def test_proxy_select_1(vanilla_pg_1mb, static_proxy):
    static_proxy.safe_psql("select 1;")
