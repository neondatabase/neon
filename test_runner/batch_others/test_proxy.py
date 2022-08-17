import subprocess

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import PSQL
import psycopg2


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql('select 1', options='project=generic-project-name')


def test_password_hack(static_proxy):
    user = 'borat'
    password = 'password'
    static_proxy.safe_psql(f"create role {user} with login password '{password}'",
                           options='project=irrelevant')

    # Note the format of `magic`!
    magic = f"project=irrelevant;{password}"
    static_proxy.safe_psql('select 1', sslsni=0, user=user, password=magic)

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.errors.OperationalError):
        magic = "broken"
        static_proxy.safe_psql('select 1', sslsni=0, user=user, password=magic)


@pytest.mark.asyncio
async def test_psql_session_id(vanilla_pg, link_proxy):
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
    # Expected output:
    #   A notice that contains a welcome to neon message that contains a http url in the form of
    #   "http://dummy-uri/<psql_session_id>"
    #   (here dummy-uri was passed from the neon_fixture that created the link_proxy
    #   The url has to be on its own line.
    #   Example:
    #   "
    #   NOTICE:  Welcome to Neon!
    #   Authenticate by visiting:
    #      http://dummy-uri/<psql_session_id>
    #   "

    max_num_lines_of_welcome_message = 15
    attempt = 0
    psql_session_id = None

    log.info(f"Neon Welcome Message:")
    for attempt in range(max_num_lines_of_welcome_message):
        raw_line = await proc.stderr.readline()
        if not raw_line:
            # output ended
            break
        log.info(f"{raw_line}")
        line = raw_line.decode("utf-8").strip()
        if line.startswith("http"):
            line_parts = line.split("/")
            psql_session_id = line_parts[-1]
            link_auth_uri = '/'.join(line_parts[:-1])
            assert link_auth_uri == link_proxy.link_auth_uri, \
                f"Line='{line}' should contain a http auth link of form '{link_proxy.link_auth_uri}/<psql_session_id>'."
            break
        log.debug("line %d does not contain expected result: %s", attempt, line)

    assert attempt <= max_num_lines_of_welcome_message, "exhausted given attempts, did not get the result"
    assert psql_session_id is not None, "psql_session_id not found from output of proc.stderr.readline()"
    log.debug(f"proc.stderr.readline() #{attempt} has the result: {psql_session_id=}")
    log.info(f"proc.stderr.readline() #{attempt} has the result: {psql_session_id=}")



    # send select 1.
    # make a db and send to proxy.

    # get vanilla_pg
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user proxy_auth with password 'pytest1' superuser")
    vanilla_pg.safe_psql("create user proxy_user with password 'pytest2'")



    cmd_line_args__to__mgmt = [
        "psql", "-h", "127.0.0.1", "-p", "7000", '-c',
        '{"session_id": "'+str(psql_session_id)+'"'
            ',"result":'
                '{"Success":'
                    '{"host":"127.0.0.1","port":5432,"dbname":"stas","user":"proxy_auth","password":"pytest1"}'
                '}'
        '}'
    ]

    for arg_id, arg in enumerate(cmd_line_args__to__mgmt):
        log.info(f"arg_id={arg_id}: {arg}")

    log.info(f"running cmd line: {cmd_line_args__to__mgmt}")
    p = subprocess.Popen(cmd_line_args__to__mgmt, stdout=subprocess.PIPE)

    out, err = p.communicate()
    log.info(f"output of running cmd line: out={out}; err={err}")


# Pass extra options to the server.
#
# Currently, proxy eats the extra connection options, so this fails.
# See https://github.com/neondatabase/neon/issues/1287
@pytest.mark.xfail
def test_proxy_options(static_proxy):
    with static_proxy.connect(options='-cproxytest.option=value') as conn:
        with conn.cursor() as cur:
            cur.execute('SHOW proxytest.option')
            value = cur.fetchall()[0][0]
            assert value == 'value'
