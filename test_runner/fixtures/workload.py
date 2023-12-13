from typing import Optional

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_for_upload
from fixtures.types import TenantId, TimelineId


class Workload:
    """
    This is not a general purpose load generator: it exists for storage tests that need to inject some
    high level types of storage work via the postgres interface:
    - layer writes (`write_rows`)
    - work for compaction (`churn_rows`)
    - reads, checking we get the right data (`validate`)
    """

    def __init__(self, env: NeonEnv, tenant_id: TenantId, timeline_id: TimelineId):
        self.env = env
        self.tenant_id = tenant_id
        self.timeline_id = timeline_id
        self.table = "foo"

        self.expect_rows = 0
        self.churn_cursor = 0

        self._endpoint: Optional[Endpoint] = None

    def endpoint(self, pageserver_id: int) -> Endpoint:
        if self._endpoint is None:
            self._endpoint = self.env.endpoints.create(
                "main",
                tenant_id=self.tenant_id,
                pageserver_id=pageserver_id,
                endpoint_id="ep-workload",
            )
            self._endpoint.start(pageserver_id=pageserver_id)
        else:
            self._endpoint.reconfigure(pageserver_id=pageserver_id)

        connstring = self._endpoint.safe_psql(
            "SELECT setting FROM pg_settings WHERE name='neon.pageserver_connstring'"
        )
        log.info(f"Workload.endpoint: connstr={connstring}")

        return self._endpoint

    def __del__(self):
        if self._endpoint is not None:
            self._endpoint.stop()

    def init(self, pageserver_id: int):
        endpoint = self.endpoint(pageserver_id)

        endpoint.safe_psql(f"CREATE TABLE {self.table} (id INTEGER PRIMARY KEY, val text);")
        endpoint.safe_psql("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")
        last_flush_lsn_upload(
            self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
        )

    def write_rows(self, n, pageserver_id):
        endpoint = self.endpoint(pageserver_id)
        start = self.expect_rows
        end = start + n - 1
        self.expect_rows += n
        dummy_value = "blah"
        endpoint.safe_psql(
            f"""
            INSERT INTO {self.table} (id, val)
            SELECT g, '{dummy_value}'
            FROM generate_series({start}, {end}) g
            """
        )

        return last_flush_lsn_upload(
            self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
        )

    def churn_rows(self, n, pageserver_id, upload=True):
        assert self.expect_rows >= n

        max_iters = 10
        endpoint = self.endpoint(pageserver_id)
        todo = n
        i = 0
        while todo > 0:
            i += 1
            if i > max_iters:
                raise RuntimeError("oops")
            start = self.churn_cursor % self.expect_rows
            n_iter = min((self.expect_rows - start), todo)
            todo -= n_iter

            end = start + n_iter - 1

            log.info(
                f"start,end = {start},{end}, cursor={self.churn_cursor}, expect_rows={self.expect_rows}"
            )

            assert end < self.expect_rows

            self.churn_cursor += n_iter
            dummy_value = "blah"
            endpoint.safe_psql_many(
                [
                    f"""
                INSERT INTO {self.table} (id, val)
                SELECT g, '{dummy_value}'
                FROM generate_series({start}, {end}) g
                ON CONFLICT (id) DO UPDATE
                SET val = EXCLUDED.val
                """,
                    f"VACUUM {self.table}",
                ]
            )

        last_flush_lsn = wait_for_last_flush_lsn(
            self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
        )
        ps_http = self.env.get_pageserver(pageserver_id).http_client()
        wait_for_last_record_lsn(ps_http, self.tenant_id, self.timeline_id, last_flush_lsn)

        if upload:
            # force a checkpoint to trigger upload
            ps_http.timeline_checkpoint(self.tenant_id, self.timeline_id)
            wait_for_upload(ps_http, self.tenant_id, self.timeline_id, last_flush_lsn)
            log.info(f"Churn: waiting for remote LSN {last_flush_lsn}")
        else:
            log.info(f"Churn: not waiting for upload, disk LSN {last_flush_lsn}")

    def validate(self, pageserver_id):
        endpoint = self.endpoint(pageserver_id)
        result = endpoint.safe_psql_many(
            [
                "select clear_buffer_cache()",
                f"""
            SELECT COUNT(*) FROM {self.table}
            """,
            ]
        )

        log.info(f"validate({self.expect_rows}): {result}")
        assert result == [[("",)], [(self.expect_rows,)]]
