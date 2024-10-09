from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from _pytest.mark import ParameterSet
from fixtures.compare_fixtures import RemoteCompare
from fixtures.log_helper import log


@dataclass
class LabelledQuery:
    """An SQL query with a label for the test report."""

    label: str
    query: str


# This must run before all tests in this module
# create extension pg_stat_statements if it does not exist
# and TEST_OLAP_COLLECT_PG_STAT_STATEMENTS is set to true (default false)
# Theoretically this could be in a module or session scope fixture,
# however the code depends on other fixtures that have function scope
@pytest.mark.skipif(
    os.getenv("TEST_OLAP_COLLECT_PG_STAT_STATEMENTS", "false").lower() == "false",
    reason="Skipping - Creating extension pg_stat_statements",
)
@pytest.mark.remote_cluster
def test_clickbench_create_pg_stat_statements(remote_compare: RemoteCompare):
    log.info("Creating extension pg_stat_statements")
    query = LabelledQuery(
        "Q_CREATE_EXTENSION", r"CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
    )
    run_psql(remote_compare, query, times=1, explain=False)
    log.info("Reset pg_stat_statements")
    query = LabelledQuery("Q_RESET", r"SELECT pg_stat_statements_reset();")
    run_psql(remote_compare, query, times=1, explain=False)


# A list of queries to run.
# Please do not alter the label for the query, as it is used to identify it.
# Labels for ClickBench queries match the labels in ClickBench reports
# on https://benchmark.clickhouse.com/ (the DB size may differ).
#
# Disable auto formatting for the list of queries so that it's easier to read
# fmt: off
QUERIES: tuple[LabelledQuery, ...] = (
    ### ClickBench queries:
    LabelledQuery("Q0",  r"SELECT COUNT(*) FROM hits;"),
    LabelledQuery("Q1",  r"SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;"),
    LabelledQuery("Q2",  r"SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;"),
    LabelledQuery("Q3",  r"SELECT AVG(UserID) FROM hits;"),
    LabelledQuery("Q4",  r"SELECT COUNT(DISTINCT UserID) FROM hits;"),
    LabelledQuery("Q5",  r"SELECT COUNT(DISTINCT SearchPhrase) FROM hits;"),
    LabelledQuery("Q6",  r"SELECT MIN(EventDate), MAX(EventDate) FROM hits;"),
    LabelledQuery("Q7",  r"SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;"),
    LabelledQuery("Q8",  r"SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;"),
    LabelledQuery("Q9",  r"SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q10", r"SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;"),
    LabelledQuery("Q11", r"SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;"),
    LabelledQuery("Q12", r"SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q13", r"SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;"),
    LabelledQuery("Q14", r"SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q15", r"SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;"),
    LabelledQuery("Q16", r"SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;"),
    LabelledQuery("Q17", r"SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;"),
    LabelledQuery("Q18", r"SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;"),
    LabelledQuery("Q19", r"SELECT UserID FROM hits WHERE UserID = 435090932899640449;"),
    LabelledQuery("Q20", r"SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';"),
    LabelledQuery("Q21", r"SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q22", r"SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q23", r"SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;"),
    LabelledQuery("Q24", r"SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;"),
    LabelledQuery("Q25", r"SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;"),
    LabelledQuery("Q26", r"SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;"),
    LabelledQuery("Q27", r"SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"),
    LabelledQuery("Q28", r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;"),
    LabelledQuery("Q29", r"SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits;"),
    LabelledQuery("Q30", r"SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q31", r"SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q32", r"SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q33", r"SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q34", r"SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q35", r"SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;"),
    LabelledQuery("Q36", r"SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10;"),
    LabelledQuery("Q37", r"SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10;"),
    LabelledQuery("Q38", r"SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;"),
    LabelledQuery("Q39", r"SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;"),
    LabelledQuery("Q40", r"SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;"),
    LabelledQuery("Q41", r"SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;"),
    LabelledQuery("Q42", r"SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000;"),
    ### Custom Neon queries:
    # I suggest using the NQ prefix (which stands for Neon Query) instead of Q
    # to not intersect with the original ClickBench queries if their list is extended.
    #
    # LabelledQuery("NQ0", r"..."),
    # LabelledQuery("NQ1", r"..."),
    # ...
)
# fmt: on

# A list of pgvector HNSW index builds to run.
# Please do not alter the label for the query, as it is used to identify it.
#
# Disable auto formatting for the list of queries so that it's easier to read
# fmt: off
PGVECTOR_QUERIES: tuple[LabelledQuery, ...] = (
    LabelledQuery("PGVPREP",  r"ALTER EXTENSION VECTOR UPDATE;"),
    LabelledQuery("PGV0",  r"DROP TABLE IF EXISTS hnsw_test_table;"),
    LabelledQuery("PGV1",  r"CREATE TABLE hnsw_test_table AS TABLE documents WITH NO DATA;"),
    LabelledQuery("PGV2",  r"INSERT INTO hnsw_test_table SELECT * FROM documents;"),
    LabelledQuery("PGV3",  r"CREATE INDEX ON hnsw_test_table (_id);"),
    LabelledQuery("PGV4",  r"CREATE INDEX ON hnsw_test_table USING hnsw (embeddings vector_cosine_ops);"),
    LabelledQuery("PGV5",  r"CREATE INDEX ON hnsw_test_table USING hnsw (embeddings vector_ip_ops);"),
    LabelledQuery("PGV6",  r"CREATE INDEX ON hnsw_test_table USING hnsw (embeddings vector_l1_ops);"),
    LabelledQuery("PGV7",  r"CREATE INDEX ON hnsw_test_table USING hnsw ((binary_quantize(embeddings)::bit(1536)) bit_hamming_ops);"),
    LabelledQuery("PGV8",  r"CREATE INDEX ON hnsw_test_table USING hnsw ((binary_quantize(embeddings)::bit(1536)) bit_jaccard_ops);"),
    LabelledQuery("PGV9",  r"DROP TABLE IF EXISTS halfvec_test_table;"),
    LabelledQuery("PGV10", r"CREATE TABLE halfvec_test_table (_id text NOT NULL, title text, text text, embeddings halfvec(1536), PRIMARY KEY (_id));"),
    LabelledQuery("PGV11", r"INSERT INTO halfvec_test_table (_id, title, text, embeddings) SELECT _id, title, text, embeddings::halfvec FROM documents;"),
    LabelledQuery("PGV12", r"CREATE INDEX documents_half_precision_hnsw_idx ON halfvec_test_table USING hnsw (embeddings halfvec_cosine_ops) WITH (m = 64, ef_construction = 128);"),
)
# fmt: on


EXPLAIN_STRING: str = "EXPLAIN (ANALYZE, VERBOSE, BUFFERS, COSTS, SETTINGS, FORMAT JSON)"


def get_scale() -> list[str]:
    # We parametrize each tpc-h and clickbench test with scale
    # to distinguish them from each other, but don't really use it inside.
    # Databases are pre-created and passed through BENCHMARK_CONNSTR env variable.

    scale = os.getenv("TEST_OLAP_SCALE", "noscale")
    return [scale]


# run the query times times plus once with EXPLAIN VERBOSE if explain is requestd
def run_psql(
    env: RemoteCompare, labelled_query: LabelledQuery, times: int, explain: bool = False
) -> None:
    # prepare connstr:
    # - cut out password from connstr to pass it via env
    # - add options to connstr
    password = env.pg.default_options.get("password", None)
    options = f"-cstatement_timeout=0 {env.pg.default_options.get('options', '')}"
    connstr = env.pg.connstr(password=None, options=options)

    environ: dict[str, str] = {}
    if password is not None:
        environ["PGPASSWORD"] = password

    label, query = labelled_query.label, labelled_query.query

    log.info(f"Running query {label} {times} times")
    for i in range(times):
        run = i + 1
        log.info(f"Run {run}/{times}")
        with env.zenbenchmark.record_duration(f"{label}/{run}"):
            env.pg_bin.run_capture(["psql", connstr, "-c", query], env=environ)
    if explain:
        log.info(f"Explaining query {label}")
        run += 1
        with env.zenbenchmark.record_duration(f"{label}/EXPLAIN"):
            env.pg_bin.run_capture(
                ["psql", connstr, "-c", f"{EXPLAIN_STRING} {query}"], env=environ
            )


@pytest.mark.parametrize("scale", get_scale())
@pytest.mark.parametrize("query", QUERIES)
@pytest.mark.remote_cluster
def test_clickbench(query: LabelledQuery, remote_compare: RemoteCompare, scale: str):
    """
    An OLAP-style ClickHouse benchmark

    Based on https://github.com/ClickHouse/ClickBench/tree/c00135ca5b6a0d86fedcdbf998fdaa8ed85c1c3b/aurora-postgresql
    The DB prepared manually in advance.
    Important: after intial data load, run `VACUUM (DISABLE_PAGE_SKIPPING, FREEZE, ANALYZE) hits;`
    to ensure that Postgres optimizer chooses the same plans as RDS and Aurora.
    """
    explain: bool = os.getenv("TEST_OLAP_COLLECT_EXPLAIN", "false").lower() == "true"

    run_psql(remote_compare, query, times=3, explain=explain)


def tpch_queuies() -> tuple[ParameterSet, ...]:
    """
    A list of queries to run for the TPC-H benchmark.
    - querues in returning tuple are ordered by the query number
    - pytest parameters id is adjusted to match the query id (the numbering starts from 1)
    """
    queries_dir = Path(__file__).parent / "tpc-h" / "queries"
    assert queries_dir.exists(), f"TPC-H queries dir not found: {queries_dir}"

    return tuple(
        pytest.param(LabelledQuery(f"Q{f.stem}", f.read_text()), id=f"query{f.stem}")
        for f in sorted(queries_dir.glob("*.sql"), key=lambda f: int(f.stem))
    )


@pytest.mark.parametrize("scale", get_scale())
@pytest.mark.parametrize("query", tpch_queuies())
@pytest.mark.remote_cluster
def test_tpch(query: LabelledQuery, remote_compare: RemoteCompare, scale: str):
    """
    TCP-H Benchmark

    The DB prepared manually in advance:
    - schema: test_runner/performance/tpc-h/create-schema.sql
    - indexes: test_runner/performance/tpc-h/create-indexes.sql
    - data generated by `dbgen` program of the official TPC-H benchmark
    - `VACUUM (FREEZE, PARALLEL 0);`

    For query generation `1669822882` is used as a seed to the RNG
    """

    run_psql(remote_compare, query, times=1)


@pytest.mark.remote_cluster
def test_user_examples(remote_compare: RemoteCompare):
    query = LabelledQuery(
        "Q1",
        r"""
        SELECT
            v20.c2263 AS v1,
            v19.c2484 AS v2,
            DATE_TRUNC('month', v18.c37)::DATE AS v3,
            (ARRAY_AGG(c1840 order by v18.c37))[1] AS v4,
            (ARRAY_AGG(c1841 order by v18.c37 DESC))[1] AS v5,
            SUM(v17.c1843) AS v6,
            SUM(v17.c1844) AS v7,
            SUM(v17.c1848) AS v8,
            SUM(v17.c1845) AS v9,
            SUM(v17.c1846) AS v10,
            SUM(v17.c1861) AS v11,
            SUM(v17.c1860) AS v12,
            SUM(v17.c1869) AS v13,
            SUM(v17.c1856) AS v14,
            SUM(v17.c1855) AS v15,
            SUM(v17.c1854) AS v16
        FROM
            s3.t266 v17
            INNER JOIN s1.t41 v18 ON v18.c34 = v17.c1836
            INNER JOIN s3.t571 v19 ON v19.c2482 = v17.c1834
            INNER JOIN s3.t331 v20 ON v20.c2261 = v17.c1835
        WHERE
            (v17.c1835 = 4) AND
            (v18.c37 >= '2019-03-01') AND
            (v17.c1833 = 2)
        GROUP BY v1, v2, v3
        ORDER BY v1, v2, v3
        LIMIT 199;
        """,
    )
    run_psql(remote_compare, query, times=3)


# This must run after all tests in this module
# Collect pg_stat_statements after running the tests if TEST_OLAP_COLLECT_PG_STAT_STATEMENTS is set to true (default false)
@pytest.mark.skipif(
    os.getenv("TEST_OLAP_COLLECT_PG_STAT_STATEMENTS", "false").lower() == "false",
    reason="Skipping - Collecting pg_stat_statements",
)
@pytest.mark.remote_cluster
def test_clickbench_collect_pg_stat_statements(remote_compare: RemoteCompare):
    log.info("Collecting pg_stat_statements")
    query = LabelledQuery("Q_COLLECT_PG_STAT_STATEMENTS", r"SELECT * from pg_stat_statements;")
    run_psql(remote_compare, query, times=1, explain=False)


@pytest.mark.parametrize("query", PGVECTOR_QUERIES)
@pytest.mark.remote_cluster
def test_pgvector_indexing(query: LabelledQuery, remote_compare: RemoteCompare):
    """
    An pgvector test that tests HNSW index build performance and parallelism.

    The DB prepared manually in advance.
    See
    - test_runner/performance/pgvector/README.md
    - test_runner/performance/pgvector/loaddata.py
    - test_runner/performance/pgvector/HNSW_build.sql
    """
    run_psql(remote_compare, query, times=1, explain=False)
