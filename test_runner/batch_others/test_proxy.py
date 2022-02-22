import pytest
import subprocess
import signal
import time


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1;")


def test_proxy_cancel(static_proxy):
    """Test that we can cancel a big generate_series query."""
    conn = static_proxy.connect()
    conn.cancel()

    with conn.cursor() as cur:
        from psycopg2.errors import QueryCanceled
        with pytest.raises(QueryCanceled):
            cur.execute("select * from generate_series(1, 100000000);")


def test_proxy_pgbench_cancel(static_proxy, pg_bin):
    """Test that we can cancel the init phase of pgbench."""
    start_time = static_proxy.safe_psql("select now();")[0]

    def get_running_queries():
        magic_string = "fsdsdfhdfhfgbcbfgbfgbf"
        with static_proxy.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    -- {magic_string}
                    select query
                    from pg_stat_activity
                    where pg_stat_activity.query_start > %s
                """, start_time)
                return [
                    row[0]
                    for row in cur.fetchall()
                    if not magic_string in row[0]
                ]

    # Let pgbench init run for 1 second
    p = subprocess.Popen(['pgbench', '-s500', '-i', static_proxy.connstr()])
    time.sleep(1)

    # Make sure something is still running, and that get_running_queries works
    assert len(get_running_queries()) > 0

    # Send sigint, which would cancel any pgbench queries
    p.send_signal(signal.SIGINT)

    # Assert that nothing is running
    time.sleep(1)
    assert len(get_running_queries()) == 0
