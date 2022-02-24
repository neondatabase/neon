def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1;")
