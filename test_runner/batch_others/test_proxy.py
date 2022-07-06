import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import PSQL


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1;", options="project=generic-project-name")


@pytest.mark.asyncio
async def test_psql_session_id(link_proxy):
    """
    Test copied and modified from: test_project_psql_link_auth test from cloud/tests_e2e/tests/test_project.py
    This is only one half of the test: the half that establishes connection to the proxy and retrieves the session_id
    The second half of the test required more heavy machinery to be ported from the cloud codebase
        (such as: project_state(..), and psql_project_connect)
    to connect to postgres and make sure the end-to-end behavior is satisfied.
    """
    psql = PSQL(
        host=link_proxy.host,
        port=link_proxy.port,
    )
    proc = await psql.run()

    # Read line by line output of psql.
    # We expect to see result in first 3 lines. [this is flexible, change if need be]
    # Expected output of form:
    # "
    # NOTICE:  Welcome to Neon!
    # Authenticate by visiting:
    #    http://dummy-uri/<psql_session_id>
    # "

    max_attempts = 3
    attempt = 0
    psql_session_id = None

    for attempt in range(max_attempts):
        raw_line = await proc.stderr.readline()
        if not raw_line:
            # output ended
            break
        log.debug(f"{attempt=} proc.stderr.readline() = {raw_line}")
        # log.info(f"{attempt=} proc.stderr.readline() = {raw_line}") # for local debugging
        line = raw_line.decode("utf-8").strip()
        if line.startswith("http"):
            psql_session_id = line.split("/")[-1]
            break
        log.debug("line %d does not contain expected result: %s", attempt, line)

    assert attempt <= max_attempts, "exhausted given attempts, did not get the result"
    assert psql_session_id is not None
    log.debug(f"proc.stderr.readline() #{attempt} has the result: {psql_session_id=}")
    log.info(f"proc.stderr.readline() #{attempt} has the result: {psql_session_id=}")

    # todo: second part of: test_project_psql_link_auth from cloud/tests_e2e/tests/test_project.py


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
