from __future__ import annotations

import threading
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, logical_replication_sync
from fixtures.utils import query_scalar, wait_until


# This test checks that branching of timeline with logical subscriptions
# does not affect logical replication for parent.
# Endpoint on a new branch will drop all existing subscriptions at the start,
# so it will not receive any changes.
# If needed, user can create new subscriptions on the child branch.
def test_subscriber_branching(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.respec(
        skip_pg_catalog_updates=False,
        create_test_user=True,
    )
    pub.start(create_test_user=True)

    env.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    # Pass create_test_user flag to get properly filled spec.users and spec.databases fields.
    #
    # This test checks the per-database operations that happen at compute start
    # and these operations are applied to the databases that are present in the spec.
    sub.respec(
        skip_pg_catalog_updates=False,
        create_test_user=True,
    )
    sub.start(create_test_user=True)

    pub.wait_for_migrations()
    sub.wait_for_migrations()

    n_records = 1000

    def check_that_changes_propagated():
        scur.execute("SELECT count(*) FROM t")
        res = scur.fetchall()
        assert res[0][0] == n_records

    def insert_data(pub, start):
        with pub.cursor(dbname="neondb", user="test", password="pubtestpwd") as pcur:
            for i in range(start, start + n_records):
                pcur.execute("INSERT into t values (%s,random()*100000)", (i,))

    # create_test_user creates a user without password
    # but psycopg2 execute() requires a password
    with sub.cursor() as scur:
        scur.execute("ALTER USER test WITH PASSWORD 'testpwd'")
    with pub.cursor() as pcur:
        # Create a test user to avoid using superuser
        pcur.execute("ALTER USER test WITH PASSWORD 'pubtestpwd'")
        # If we don't do this, creating the subscription will fail
        pub.edit_hba(["host all test 0.0.0.0/0 md5"])

    with pub.cursor(dbname="neondb", user="test", password="pubtestpwd") as pcur:
        pcur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
        pcur.execute("CREATE PUBLICATION pub FOR TABLE t")

        with sub.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            scur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            pub_conn = (
                f"host=localhost port={pub.pg_port} dbname=neondb user=test password=pubtestpwd"
            )
            query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
            scur.execute(query)
            time.sleep(2)  # let initial table sync complete

        insert_data(pub, 0)

        with sub.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            wait_until(check_that_changes_propagated)
            latest_end_lsn = query_scalar(
                scur, "select latest_end_lsn from pg_catalog.pg_stat_subscription; "
            )
            last_insert_lsn = query_scalar(scur, "select pg_current_wal_insert_lsn();")

            log.info(f"latest_end_lsn = {latest_end_lsn}")
            log.info(f"last_insert_lsn = {last_insert_lsn}")

        # stop the parent subscriber so that it doesn't interfere with the test
        sub.stop()

        # 1. good scenario:
        # create subscriber_child_1
        # it will not get changes from publisher, because drop_subscriptions_before_start is set to True
        sub_child_1_timeline_id = env.create_branch(
            "subscriber_child_1",
            ancestor_branch_name="subscriber",
            ancestor_start_lsn=last_insert_lsn,
        )
        sub_child_1 = env.endpoints.create("subscriber_child_1")
        # Pass drop_subscriptions_before_start flag
        sub_child_1.respec(
            skip_pg_catalog_updates=False,
            create_test_user=True,
            drop_subscriptions_before_start=True,
        )
        sub_child_1.start(create_test_user=True)

        # ensure that subscriber_child_1 sees all the data
        with sub_child_1.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == n_records

            # ensure that there are no subscriptions in this database
            scur.execute("SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = 'sub'")
            assert len(scur.fetchall()) == 0

        # ensure that drop_subscriptions_done happened on this timeline
        with sub_child_1.cursor() as scur_postgres:
            scur_postgres.execute("SELECT timeline_id from neon.drop_subscriptions_done")
            res = scur_postgres.fetchall()
            assert len(res) == 1
            assert str(sub_child_1_timeline_id) == res[0][0]

        old_n_records = n_records
        # insert more data on publisher
        insert_data(pub, n_records)
        n_records += n_records

        pcur.execute("SELECT count(*) FROM t")
        res = pcur.fetchall()
        assert res[0][0] == n_records

        # ensure that subscriber_child_1 doesn't see the new data
        with sub_child_1.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == old_n_records

        # reenable logical replication on subscriber_child_1
        # using new publication
        # ensure that new publication works as expected
        with sub_child_1.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            scur.execute("TRUNCATE t")

            # create new subscription
            # with new pub name
            pcur.execute("CREATE PUBLICATION pub_new FOR TABLE t")
            query = f"CREATE SUBSCRIPTION sub_new CONNECTION '{pub_conn}' PUBLICATION pub_new"
            scur.execute(query)

            wait_until(check_that_changes_propagated)

            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == n_records

        # ensure that new publication works as expected after compute restart
        # first restart with drop_subscriptions_before_start=True
        # to emulate the case when compute restarts within the VM with stale spec
        sub_child_1.stop()
        sub_child_1.respec(
            skip_pg_catalog_updates=False,
            create_test_user=True,
            drop_subscriptions_before_start=True,
        )
        sub_child_1.start(create_test_user=True)

        with sub_child_1.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            # ensure that even though the flag is set, we didn't drop new subscription
            scur.execute("SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = 'sub_new'")
            assert len(scur.fetchall()) == 1

        # ensure that drop_subscriptions_done happened on this timeline
        with sub_child_1.cursor() as scur_postgres:
            scur_postgres.execute("SELECT timeline_id from neon.drop_subscriptions_done")
            res = scur_postgres.fetchall()
            assert len(res) == 1
            assert str(sub_child_1_timeline_id) == res[0][0]

        sub_child_1.stop()
        sub_child_1.respec(
            skip_pg_catalog_updates=False,
            create_test_user=True,
            drop_subscriptions_before_start=False,
        )
        sub_child_1.start(create_test_user=True)

        # insert more data on publisher
        insert_data(pub, n_records)
        n_records += n_records
        with sub_child_1.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            # ensure that there is a subscriptions in this database
            scur.execute("SELECT 1 FROM pg_catalog.pg_subscription WHERE subname = 'sub_new'")
            assert len(scur.fetchall()) == 1

            wait_until(check_that_changes_propagated)
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == n_records

        # ensure that drop_subscriptions_done happened on this timeline
        with sub_child_1.cursor() as scur_postgres:
            scur_postgres.execute("SELECT timeline_id from neon.drop_subscriptions_done")
            res = scur_postgres.fetchall()
            assert len(res) == 1
            assert str(sub_child_1_timeline_id) == res[0][0]

        # wake the sub and ensure that it catches up with the new data
        sub.start(create_test_user=True)
        with sub.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            wait_until(check_that_changes_propagated)
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == n_records

        # test that we can create a branch of a branch
        sub_child_2_timeline_id = env.create_branch(
            "subscriber_child_2",
            ancestor_branch_name="subscriber_child_1",
        )
        sub_child_2 = env.endpoints.create("subscriber_child_2")
        # Pass drop_subscriptions_before_start flag
        sub_child_2.respec(
            skip_pg_catalog_updates=False,
            drop_subscriptions_before_start=True,
        )
        sub_child_2.start(create_test_user=True)

        # ensure that subscriber_child_2 does not inherit subscription from child_1
        with sub_child_2.cursor(dbname="neondb", user="test", password="testpwd") as scur:
            # ensure that there are no subscriptions in this database
            scur.execute("SELECT count(*) FROM pg_catalog.pg_subscription")
            res = scur.fetchall()
            assert res[0][0] == 0

        # ensure that drop_subscriptions_done happened on this timeline
        with sub_child_2.cursor() as scur_postgres:
            scur_postgres.execute("SELECT timeline_id from neon.drop_subscriptions_done")
            res = scur_postgres.fetchall()
            assert len(res) == 1
            assert str(sub_child_2_timeline_id) == res[0][0]


def test_multiple_subscription_branching(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can handle concurrent deletion of subscriptions in a multiple databases
    """
    env = neon_simple_env

    NUMBER_OF_DBS = 5

    # Create and start endpoint so that neon_local put all the generated
    # stuff into the config.json file.
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "max_replication_slots = 10",
            "max_logical_replication_workers=10",
            "max_worker_processes=10",
        ],
    )

    TEST_DB_NAMES = [
        {
            "name": "neondb",
            "owner": "cloud_admin",
        },
        {
            "name": "publisher_db",
            "owner": "cloud_admin",
        },
    ]

    for i in range(NUMBER_OF_DBS):
        TEST_DB_NAMES.append(
            {
                "name": f"db{i}",
                "owner": "cloud_admin",
            }
        )

    # Update the config.json file to create the databases
    # and reconfigure the endpoint to apply the changes.
    endpoint.respec_deep(
        **{
            "spec": {
                "skip_pg_catalog_updates": False,
                "cluster": {
                    "databases": TEST_DB_NAMES,
                },
            },
        }
    )
    endpoint.reconfigure()

    connstr = endpoint.connstr(dbname="publisher_db").replace("'", "''")

    # create table, replication and subscription for each of the databases
    with endpoint.cursor(dbname="publisher_db") as publisher_cursor:
        for i in range(NUMBER_OF_DBS):
            publisher_cursor.execute(f"CREATE TABLE t{i}(a int)")
            publisher_cursor.execute(f"CREATE PUBLICATION mypub{i} FOR TABLE t{i}")
            publisher_cursor.execute(
                f"select pg_catalog.pg_create_logical_replication_slot('mysub{i}', 'pgoutput');"
            )
            publisher_cursor.execute(f"INSERT INTO t{i} VALUES ({i})")

            with endpoint.cursor(dbname=f"db{i}") as cursor:
                cursor.execute(f"CREATE TABLE t{i}(a int)")
                cursor.execute(
                    f"CREATE SUBSCRIPTION mysub{i} CONNECTION '{connstr}' PUBLICATION mypub{i}  WITH (create_slot = false) "
                )

    # wait for the subscription to be active
    for i in range(NUMBER_OF_DBS):
        logical_replication_sync(
            endpoint,
            endpoint,
            f"mysub{i}",
            sub_dbname=f"db{i}",
            pub_dbname="publisher_db",
        )

    # Check that replication is working
    for i in range(NUMBER_OF_DBS):
        with endpoint.cursor(dbname=f"db{i}") as cursor:
            cursor.execute(f"SELECT * FROM t{i}")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == i

            last_insert_lsn = query_scalar(cursor, "select pg_current_wal_insert_lsn();")

    def start_publisher_workload(table_num: int, duration: int):
        start = time.time()
        with endpoint.cursor(dbname="publisher_db") as cur:
            while time.time() - start < duration:
                cur.execute(f"INSERT INTO t{i} SELECT FROM generate_series(1,1000)")

    LOAD_DURATION = 5
    threads = [
        threading.Thread(target=start_publisher_workload, args=(i, LOAD_DURATION))
        for i in range(NUMBER_OF_DBS)
    ]

    for thread in threads:
        thread.start()

    sub_child_1_timeline_id = env.create_branch(
        "subscriber_child_1",
        ancestor_branch_name="main",
        ancestor_start_lsn=last_insert_lsn,
    )

    sub_child_1 = env.endpoints.create("subscriber_child_1")

    sub_child_1.respec(
        skip_pg_catalog_updates=False,
        reconfigure_concurrency=5,
        drop_subscriptions_before_start=True,
        cluster={
            "databases": TEST_DB_NAMES,
            "roles": [],
        },
    )

    sub_child_1.start()

    # ensure that subscription deletion happened on this timeline
    with sub_child_1.cursor() as scur_postgres:
        scur_postgres.execute("SELECT timeline_id from neon.drop_subscriptions_done")
        res = scur_postgres.fetchall()
        log.info(f"res = {res}")
        assert len(res) == 1
        assert str(sub_child_1_timeline_id) == res[0][0]

    # ensure that there are no subscriptions in the databases
    for i in range(NUMBER_OF_DBS):
        with sub_child_1.cursor(dbname=f"db{i}") as cursor:
            cursor.execute("SELECT * FROM pg_catalog.pg_subscription")
            res = cursor.fetchall()
            assert len(res) == 0

            # ensure that there are no unexpected rows in the tables
            cursor.execute(f"SELECT * FROM t{i}")
            rows = cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][0] == i

    for thread in threads:
        thread.join()

    # ensure that logical replication is still working in main endpoint
    # wait for it to catch up
    for i in range(NUMBER_OF_DBS):
        logical_replication_sync(
            endpoint,
            endpoint,
            f"mysub{i}",
            sub_dbname=f"db{i}",
            pub_dbname="publisher_db",
        )

    # verify that the data is the same in publisher and subscriber tables
    with endpoint.cursor(dbname="publisher_db") as publisher_cursor:
        for i in range(NUMBER_OF_DBS):
            with endpoint.cursor(dbname=f"db{i}") as cursor:
                publisher_cursor.execute(f"SELECT count(*) FROM t{i}")
                cursor.execute(f"SELECT count(*) FROM t{i}")
                pub_res = publisher_cursor.fetchone()
                sub_res = cursor.fetchone()
                log.info(f"for table t{i}: pub_res = {pub_res}, sub_res = {sub_res}")
                assert pub_res == sub_res
