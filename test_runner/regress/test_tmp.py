import random
import time
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.types import TimelineId
from fixtures.utils import print_gc_result, query_scalar
import pytest


@pytest.mark.parametrize('execution_number', range(10000))
def test_tmp(neon_env_builder: NeonEnvBuilder, execution_number):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    try:
        endpoint = env.endpoints.create_start("main")
    except Exception as e:
        log.error(f"failed to create endpoint: {e}")
        time.sleep(100)
        raise e

    log.info("postgres is running on main branch")

    def exec(sql):
        before_ts = time.time()
        res = endpoint.safe_psql(sql)
        after_ts = time.time()
        log.info(f"executed in {after_ts - before_ts:.2f}s, result {res}")
        if after_ts - before_ts > 1:
            raise Exception(f"executed in {after_ts - before_ts:.2f}s, result {res}")
        return res

    time.sleep(random.random() * 4)

    exec("SELECT 1")
    exec("CREATE TABLE IF NOT EXISTS activity_v1 (\n\t\tid SERIAL PRIMARY KEY,\n\t\tnonce BIGINT,\n\t\tval FLOAT,\n\t\tcreated_at TIMESTAMP DEFAULT NOW()\n\t )")
    exec("INSERT INTO activity_v1(nonce,val) SELECT 5122547621334681000 AS nonce, avg(id) AS val FROM activity_v1 RETURNING *")

    time.sleep(random.random() * 4)

    exec("SELECT 1")
    exec("CREATE TABLE IF NOT EXISTS activity_v1 (\n\t\tid SERIAL PRIMARY KEY,\n\t\tnonce BIGINT,\n\t\tval FLOAT,\n\t\tcreated_at TIMESTAMP DEFAULT NOW()\n\t )")
    exec("INSERT INTO activity_v1(nonce,val) SELECT 2137073174395130327 AS nonce, avg(id) AS val FROM activity_v1 RETURNING *")
    
    exec("SELECT 1")
    exec("CREATE TABLE IF NOT EXISTS activity_v1 (\n\t\tid SERIAL PRIMARY KEY,\n\t\tnonce BIGINT,\n\t\tval FLOAT,\n\t\tcreated_at TIMESTAMP DEFAULT NOW()\n\t )")
    exec("INSERT INTO activity_v1(nonce,val) SELECT (random() * 100)::int AS nonce, avg(id) AS val FROM activity_v1 RETURNING *")
    
    exec("SELECT 1")
    exec("CREATE TABLE IF NOT EXISTS activity_v1 (\n\t\tid SERIAL PRIMARY KEY,\n\t\tnonce BIGINT,\n\t\tval FLOAT,\n\t\tcreated_at TIMESTAMP DEFAULT NOW()\n\t )")
    exec("INSERT INTO activity_v1(nonce,val) SELECT (random() * 100)::int AS nonce, avg(id) AS val FROM activity_v1 RETURNING *")

    exec("SELECT 1")
    exec("CREATE TABLE IF NOT EXISTS activity_v1 (\n\t\tid SERIAL PRIMARY KEY,\n\t\tnonce BIGINT,\n\t\tval FLOAT,\n\t\tcreated_at TIMESTAMP DEFAULT NOW()\n\t )")
    exec("INSERT INTO activity_v1(nonce,val) SELECT (random() * 100)::int AS nonce, avg(id) AS val FROM activity_v1 RETURNING *")

    # ERROR: could not read relation existence of rel 1663/16386/16427.1 from page server at lsn 0/014A0888
    # I2GH6gtFBpCz
    # 

