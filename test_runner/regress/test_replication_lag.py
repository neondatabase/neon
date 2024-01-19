import threading
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


def test_replication_lag(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    n_iterations = 10

    # Use aggressive GC and checkpoint settings
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "gc_period": "5 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "5 s",
        }
    )

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-T30", connstr])

    with env.endpoints.create_start(
        branch_name="main", endpoint_id="primary", tenant_id=tenant
    ) as primary:
        pg_bin.run_capture(["pgbench", "-i", "-s10", primary.connstr()])

        t = threading.Thread(target=run_pgbench, args=(primary.connstr(),), daemon=True)
        t.start()

        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            time.sleep(10)
            for _ in range(1, n_iterations):
                primary_lsn = primary.safe_psql_scalar(
                    "SELECT pg_current_wal_flush_lsn()::text", log_query=False
                )
                secondary_lsn = secondary.safe_psql_scalar(
                    "SELECT pg_last_wal_replay_lsn()", log_query=False
                )
                balance = secondary.safe_psql_scalar("select sum(abalance) from pgbench_accounts")
                log.info(
                    f"primary_lsn={primary_lsn}, secondary_lsn={secondary_lsn}, balance={balance}"
                )

        t.join()
