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

        self.endpoints: dict[int, Endpoint] = {}

    def endpoint(self, pageserver_id: int):
        if pageserver_id not in self.endpoints:
            self.endpoints[pageserver_id] = self.env.endpoints.create(
                "main",
                tenant_id=self.tenant_id,
                pageserver_id=pageserver_id,
                endpoint_id=f"ep-{pageserver_id}",
            )

        endpoint = self.endpoints[pageserver_id]
        assert not endpoint.running
        endpoint.start(pageserver_id=pageserver_id)
        return endpoint

    def init(self, pageserver_id: int):
        with self.endpoint(pageserver_id) as endpoint:
            endpoint.safe_psql(f"CREATE TABLE {self.table} (id INTEGER PRIMARY KEY, val text);")
            endpoint.safe_psql("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")
            last_flush_lsn_upload(
                self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
            )

    def write_rows(self, n, pageserver_id):
        with self.endpoint(pageserver_id) as endpoint:
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
        with self.endpoint(pageserver_id) as endpoint:
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

    def validate(self, pageserver_id):
        with self.endpoint(pageserver_id) as endpoint:
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
