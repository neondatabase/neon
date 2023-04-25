from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder
from fixtures.types import Lsn, TenantId


# TODO kb scenario docs + comments/logs
def test_pageserver_lsn_wait_error_start(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.pageserver_config_override = "wait_lsn_timeout = '1s'"
    env = neon_env_builder.init_start()
    env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant()
    expected_timeout_error = f"Timed out while waiting for WAL record at LSN {future_lsn} to arrive"
    env.pageserver.allowed_errors.append(f".*{expected_timeout_error}.*")

    try:
        basebackup_with_huge_lsn(env, tenant_id)
    except Exception as e:
        exception_string = str(e)
        assert expected_timeout_error in exception_string
        assert "walreceiver status: Not active" in exception_string

    insert_test_elements(env, tenant_id, start=0, count=1_000)
    try:
        basebackup_with_huge_lsn(env, tenant_id)
    except Exception as e:
        exception_string = str(e)
        assert expected_timeout_error in exception_string
        assert "walreceiver status: Not active" not in exception_string
        assert "walreceiver status" in exception_string


def test_pageserver_lsn_wait_error_safekeeper_stop(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.pageserver_config_override = "wait_lsn_timeout = '1s'"
    neon_env_builder.safekeepers_id_start = 12345
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant()

    elements_to_insert = 1_000_000
    expected_timeout_error = f"Timed out while waiting for WAL record at LSN {future_lsn} to arrive"
    env.pageserver.allowed_errors.append(f".*{expected_timeout_error}.*")

    insert_test_elements(env, tenant_id, start=0, count=elements_to_insert)

    try:
        basebackup_with_huge_lsn(env, tenant_id)
    except Exception as e:
        exception_string = str(e)
        assert expected_timeout_error in exception_string

        for safekeeper in env.safekeepers:
            assert str(safekeeper.id) in exception_string

    stopped_safekeeper = env.safekeepers[-1]
    stopped_safekeeper_id = stopped_safekeeper.id
    log.info(f"Stopping safekeeper {stopped_safekeeper.id}")
    stopped_safekeeper.stop()

    insert_test_elements(env, tenant_id, start=elements_to_insert + 1, count=elements_to_insert)

    try:
        basebackup_with_huge_lsn(env, tenant_id)
    except Exception as e:
        exception_string = str(e)
        assert expected_timeout_error in exception_string

        for safekeeper in env.safekeepers:
            if safekeeper.id == stopped_safekeeper_id:
                assert str(safekeeper.id) not in exception_string
            else:
                assert str(safekeeper.id) in exception_string


def insert_test_elements(env: NeonEnv, tenant_id: TenantId, start: int, count: int):
    first_element_id = start
    last_element_id = first_element_id + count
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS t(key serial primary key, value text)")
            cur.execute(
                f"INSERT INTO t SELECT i, CONCAT('payload_', i) FROM generate_series({first_element_id},{last_element_id}) as i"
            )


future_lsn = Lsn("0/FFFFFFFF")


def basebackup_with_huge_lsn(env: NeonEnv, tenant_id: TenantId):
    with env.endpoints.create_start(
        "main",
        tenant_id=tenant_id,
        lsn=future_lsn,
    ) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("SELECT 1")
