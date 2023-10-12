from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, last_flush_lsn_upload
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

    def endpoint(self, pageserver_id):
        return self.env.endpoints.create_start(
            "main", tenant_id=self.tenant_id, pageserver_id=pageserver_id
        )

    def init(self, pageserver_id: int):
        with self.endpoint(pageserver_id) as endpoint:
            endpoint.safe_psql(f"CREATE TABLE {self.table} (id INTEGER PRIMARY KEY, val text)")
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

    def churn_rows(self, n, pageserver_id):
        assert self.expect_rows >= n

        with self.endpoint(pageserver_id) as endpoint:
            start = self.churn_cursor % (self.expect_rows)
            end = (self.churn_cursor + n - 1) % (self.expect_rows)
            self.churn_cursor += n
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

            return last_flush_lsn_upload(
                self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
            )

    def validate(self, pageserver_id):
        with self.endpoint(pageserver_id) as endpoint:
            result = endpoint.safe_psql(
                f"""
                SELECT COUNT(*) FROM {self.table}
                """
            )

            log.info(f"validate({self.expect_rows}): {result}")
            assert result == [(self.expect_rows,)]
