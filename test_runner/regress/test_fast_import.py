from __future__ import annotations

from fixtures.fast_import import FastImport
from fixtures.log_helper import log
from fixtures.neon_fixtures import VanillaPostgres, PgProtocol, PgBin
from fixtures.port_distributor import PortDistributor


def test_fast_import(
        test_output_dir,
        vanilla_pg: VanillaPostgres,
        port_distributor: PortDistributor,
        fast_import: FastImport,
):
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    pg_port = port_distributor.get_port()
    fast_import.run(pg_port, vanilla_pg.connstr())
    vanilla_pg.stop()

    pgbin = PgBin(test_output_dir, fast_import.pg_distrib_dir, fast_import.pg_version)
    new_pgdata_vanilla_pg = VanillaPostgres(fast_import.workdir / "pgdata", pgbin, pg_port, False)
    new_pgdata_vanilla_pg.start()

    # database name and user are hardcoded in fast_import binary, and they are different from normal vanilla postgres
    conn = PgProtocol(dsn=f"postgresql://cloud_admin@localhost:{pg_port}/neondb")
    res = conn.safe_psql("SELECT count(*) FROM foo;")
    log.info(f"Result: {res}")
    assert res[0][0] == 10
    new_pgdata_vanilla_pg.stop()


# TODO: Maybe test with pageserver?
# 1. run whole neon env
# 2. create timeline with some s3 path???
# 3. run fast_import with s3 prefix
# 4. ??? mock http where pageserver will report progress
# 5. run compute on this timeline and check if data is there