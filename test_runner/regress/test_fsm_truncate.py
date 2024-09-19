from fixtures.shared_fixtures import TTimeline


def test_fsm_truncate(timeline: TTimeline):
    endpoint = timeline.primary
    endpoint.safe_psql(
        "CREATE TABLE t1(key int); CREATE TABLE t2(key int); TRUNCATE TABLE t1; TRUNCATE TABLE t2;"
    )
