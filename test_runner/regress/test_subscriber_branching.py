
from __future__ import annotations

import threading
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, logical_replication_sync
from fixtures.utils import query_scalar, wait_until


# This test checks that branching of timeline with logical subscriptions does not affect logical replication.
# Endpoint on a new branch will start with `disable_logical_replication_subscribers` set to True
# this will prevent it from receiving any changes until it is set to False
# Users can later set it to False explicitly, if they want to receive changes on a new branch.
def test_subscriber_branching(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.start()

    env.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    sub.start()

    n_records = 1000

    def check_that_changes_propagated():
        scur.execute("SELECT count(*) FROM t")
        res = scur.fetchall()
        assert res[0][0] == n_records

    def insert_data(pub, start):
        with pub.cursor() as pcur:
            for i in range(start, start + n_records):
                pcur.execute("INSERT into t values (%s,random()*100000)", (i,))

    with pub.cursor() as pcur:
        with sub.cursor() as scur:
            pcur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            pcur.execute("CREATE PUBLICATION pub FOR TABLE t")
            scur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            pub_conn = f"host=localhost port={pub.pg_port} dbname=postgres user=cloud_admin"
            query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
            scur.execute(query)
            time.sleep(2)  # let initial table sync complete

        insert_data(pub, 0)
        with sub.cursor() as scur:
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
        # it will not get changes from publisher, because disable_logical_replication_subscribers is set to True
        env.create_branch(
            "subscriber_child_1",
            ancestor_branch_name="subscriber",
            ancestor_start_lsn=last_insert_lsn,
        )
        sub_child_1 = env.endpoints.create_start(
            "subscriber_child_1", config_lines=["disable_logical_replication_subscribers=true"]
        )

        # ensure that subscriber_child_1 sees all the data
        with sub_child_1.cursor() as sc1cur:
            sc1cur.execute("SELECT count(*) FROM t")
            res = sc1cur.fetchall()
            assert res[0][0] == n_records

        old_n_records = n_records
        # insert more data on publisher
        insert_data(pub, n_records)
        n_records *= 2

        pcur.execute("SELECT count(*) FROM t")
        res = pcur.fetchall()
        assert res[0][0] == n_records

        # ensure that subscriber_child_1 doesn't see the new data
        with sub_child_1.cursor() as scur:
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == old_n_records

        # reenable logical replication on subscriber_child_1
        # using new publication
        # ensure that new publication works as expected
        with sub_child_1.cursor() as scur:
            scur.execute("alter subscription sub disable")
            scur.execute("alter subscription sub set (slot_name = NONE)")
            scur.execute("drop subscription sub")

            scur.execute("TRUNCATE t")

            disable_logical_replication_subscribers = query_scalar(scur, "SHOW disable_logical_replication_subscribers;")
            assert disable_logical_replication_subscribers == "on"

            scur.execute("alter system set disable_logical_replication_subscribers=false")
            scur.execute("select pg_reload_conf()")

            # create new subscription
            # with new pub name
            pcur.execute("CREATE PUBLICATION pub_new FOR TABLE t")
            query = f"CREATE SUBSCRIPTION sub_new CONNECTION '{pub_conn}' PUBLICATION pub_new"
            scur.execute(query)

            wait_until(check_that_changes_propagated)

            # time.sleep(5)  # let initial table sync complete
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == n_records

        # wake the sub and ensure that it catches up with the new data
        sub.start()
        with sub.cursor() as scur:
            logical_replication_sync(sub, pub)
            check_that_changes_propagated()
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] == n_records




# This test checks that branching of subscriber affects replication
def test_subscriber_branching_bad(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.start()

    env.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    sub.start()

    n_records = 1000

    def check_that_changes_propagated():
        scur.execute("SELECT count(*) FROM t")
        res = scur.fetchall()
        assert res[0][0] == n_records

    def insert_data(pub, start):
        with pub.cursor() as pcur:
            for i in range(start, start + n_records):
                pcur.execute("INSERT into t values (%s,random()*100000)", (i,))

    with pub.cursor() as pcur:
        with sub.cursor() as scur:
            pcur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            pcur.execute("CREATE PUBLICATION pub FOR TABLE t")
            scur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            # scur.execute("CREATE INDEX on t(sk)") # slowdown applying WAL at replica
            pub_conn = f"host=localhost port={pub.pg_port} dbname=postgres user=cloud_admin"
            query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
            scur.execute(query)
            time.sleep(2)  # let initial table sync complete

        insert_data(pub, 0)
        with sub.cursor() as scur:
            wait_until(check_that_changes_propagated)
            latest_end_lsn = query_scalar(
                scur, "select received_lsn from pg_catalog.pg_stat_subscription; "
            )
            last_insert_lsn = query_scalar(scur, "select pg_current_wal_insert_lsn();")

            log.info(f"latest_end_lsn = {latest_end_lsn}")
            log.info(f"last_insert_lsn = {last_insert_lsn}")

        # stop the parent subscriber so that it doesn't interfere with the test
        sub.stop()

        # bad scenario:
        # create subscriber_child_2
        # it will get changes from publisher
        # This is not desired behavior, because if sub were running, it would have created a race condition
        # with sub and sub_child_2 both moving the same replication slot forward.
        #
        # TODO: is there any way to test this race without flakiness?
        env.create_branch("subscriber_child_2", ancestor_branch_name="subscriber", ancestor_start_lsn=latest_end_lsn)
        sub_child_2 = env.endpoints.create_start("subscriber_child_2")

        #ensure that subscriber_child_2 sees all the data
        with sub_child_2.cursor() as scur:
            logical_replication_sync(sub_child_2, pub)
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] <= n_records

        # insert more data on publisher
        insert_data(pub, n_records)
        n_records *= 2

        pcur.execute("SELECT count(*) FROM t")
        res = pcur.fetchall()
        assert res[0][0] == n_records

        # check if subscriber_child_2 sees the new data
        # FIXME:
        with sub_child_2.cursor() as scur:
            logical_replication_sync(sub_child_2, pub)
            scur.execute("SELECT count(*) FROM t")
            res = scur.fetchall()
            assert res[0][0] < n_records
