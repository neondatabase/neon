from __future__ import annotations

import random
import threading
import time

import pytest
import requests
from fixtures.common_types import TenantId, TenantShardId, TimelineArchivalState, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    last_flush_lsn_upload,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import assert_prefix_empty, assert_prefix_not_empty, list_prefix
from fixtures.pg_version import PgVersion, run_only_on_default_postgres
from fixtures.remote_storage import s3_storage
from fixtures.utils import wait_until


@pytest.mark.parametrize("shard_count", [0, 4])
def test_timeline_archive(neon_env_builder: NeonEnvBuilder, shard_count: int):
    unsharded = shard_count == 0
    if unsharded:
        env = neon_env_builder.init_start()
        # If we run the unsharded version, talk to the pageserver directly
        ps_http = env.pageserver.http_client()
    else:
        neon_env_builder.num_pageservers = shard_count
        env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
        # If we run the unsharded version, talk to the storage controller
        ps_http = env.storage_controller.pageserver_api()

    # first try to archive a non existing timeline for an existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found") as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # for a non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match="NotFound: [tT]enant",
    ) as exc:
        ps_http.timeline_archival_config(
            invalid_tenant_id,
            invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # construct a pair of branches to validate that pageserver prohibits
    # archival of ancestor timelines when they have non-archived child branches
    parent_timeline_id = env.create_branch("test_ancestor_branch_archive_parent")

    leaf_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_branch1",
        ancestor_branch_name="test_ancestor_branch_archive_parent",
    )

    with pytest.raises(
        PageserverApiException,
        match="Cannot archive timeline which has non-archived child timelines",
    ) as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            parent_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 412

    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        timeline_id=leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is False

    # Test that archiving the leaf timeline and then the parent works
    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    # Test that the leaf can't be unarchived
    with pytest.raises(
        PageserverApiException,
        match="ancestor is archived",
    ) as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            leaf_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )

    # Unarchive works for the leaf if the parent gets unarchived first
    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )

    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )


@pytest.mark.parametrize("manual_offload", [False, True])
def test_timeline_offloading(neon_env_builder: NeonEnvBuilder, manual_offload: bool):
    if not manual_offload:
        # (automatic) timeline offloading defaults to false for now
        neon_env_builder.pageserver_config_override = "timeline_offloading = true"

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Turn off gc and compaction loops: we want to issue them manually for better reliability
    tenant_id, initial_timeline_id = env.create_tenant(
        conf={
            "gc_period": "0s",
            "compaction_period": "0s" if manual_offload else "1s",
        }
    )

    # Create three branches that depend on each other, starting with two
    grandparent_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_grandparent", tenant_id
    )
    parent_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_parent", tenant_id, "test_ancestor_branch_archive_grandparent"
    )

    # write some stuff to the parent
    with env.endpoints.create_start(
        "test_ancestor_branch_archive_parent", tenant_id=tenant_id
    ) as endpoint:
        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo(key serial primary key, t text default 'data_content')",
                "INSERT INTO foo SELECT FROM generate_series(1,1000)",
            ]
        )
        sum = endpoint.safe_psql("SELECT sum(key) from foo where key > 50")

    # create the third branch
    leaf_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_branch1", tenant_id, "test_ancestor_branch_archive_parent"
    )

    ps_http.timeline_archival_config(
        tenant_id,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        tenant_id,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        tenant_id,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    ps_http.timeline_archival_config(
        tenant_id,
        grandparent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    def timeline_offloaded_logged(timeline_id: TimelineId) -> bool:
        return (
            env.pageserver.log_contains(f".*{timeline_id}.* offloading archived timeline.*")
            is not None
        )

    if manual_offload:
        with pytest.raises(
            PageserverApiException,
            match="timeline has attached children",
        ):
            # This only tests the (made for testing only) http handler,
            # but still demonstrates the constraints we have.
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=parent_timeline_id)

    def parent_offloaded():
        if manual_offload:
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=parent_timeline_id)
        assert timeline_offloaded_logged(parent_timeline_id)

    def leaf_offloaded():
        if manual_offload:
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=leaf_timeline_id)
        assert timeline_offloaded_logged(leaf_timeline_id)

    wait_until(30, 1, leaf_offloaded)
    wait_until(30, 1, parent_offloaded)

    # Offloaded child timelines should still prevent deletion
    with pytest.raises(
        PageserverApiException,
        match=f".* timeline which has child timelines: \\[{leaf_timeline_id}\\]",
    ):
        ps_http.timeline_delete(tenant_id, parent_timeline_id)

    ps_http.timeline_archival_config(
        tenant_id,
        grandparent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
    ps_http.timeline_archival_config(
        tenant_id,
        parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
    parent_detail = ps_http.timeline_detail(
        tenant_id,
        parent_timeline_id,
    )
    assert parent_detail["is_archived"] is False

    with env.endpoints.create_start(
        "test_ancestor_branch_archive_parent", tenant_id=tenant_id
    ) as endpoint:
        sum_again = endpoint.safe_psql("SELECT sum(key) from foo where key > 50")
        assert sum == sum_again

    # Test that deletion of offloaded timelines works
    ps_http.timeline_delete(tenant_id, leaf_timeline_id)

    assert not timeline_offloaded_logged(initial_timeline_id)


@pytest.mark.parametrize("delete_timeline", [False, True])
def test_timeline_offload_persist(neon_env_builder: NeonEnvBuilder, delete_timeline: bool):
    """
    Test for persistence of timeline offload state
    """
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Turn off gc and compaction loops: we want to issue them manually for better reliability
    tenant_id, root_timeline_id = env.create_tenant(
        conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{1024 ** 2}",
        }
    )

    # Create a branch and archive it
    child_timeline_id = env.create_branch("test_archived_branch_persisted", tenant_id)

    with env.endpoints.create_start(
        "test_archived_branch_persisted", tenant_id=tenant_id
    ) as endpoint:
        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo(key serial primary key, t text default 'data_content')",
                "INSERT INTO foo SELECT FROM generate_series(1,2048)",
            ]
        )
        sum = endpoint.safe_psql("SELECT sum(key) from foo where key < 500")
        last_flush_lsn_upload(env, endpoint, tenant_id, child_timeline_id)

    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/",
    )
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/tenant-manifest",
    )

    ps_http.timeline_archival_config(
        tenant_id,
        child_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        tenant_id,
        child_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    def timeline_offloaded_api(timeline_id: TimelineId) -> bool:
        # TODO add a proper API to check if a timeline has been offloaded or not
        return not any(
            timeline["timeline_id"] == str(timeline_id)
            for timeline in ps_http.timeline_list(tenant_id=tenant_id)
        )

    def child_offloaded():
        ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=child_timeline_id)
        assert timeline_offloaded_api(child_timeline_id)

    wait_until(30, 1, child_offloaded)

    assert timeline_offloaded_api(child_timeline_id)
    assert not timeline_offloaded_api(root_timeline_id)

    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/tenant-manifest",
    )

    # Test persistence, is the timeline still offloaded?
    env.pageserver.stop()
    env.pageserver.start()

    assert timeline_offloaded_api(child_timeline_id)
    assert not timeline_offloaded_api(root_timeline_id)

    if delete_timeline:
        ps_http.timeline_delete(tenant_id, child_timeline_id)
        with pytest.raises(PageserverApiException, match="not found"):
            ps_http.timeline_detail(
                tenant_id,
                child_timeline_id,
            )
    else:
        ps_http.timeline_archival_config(
            tenant_id,
            child_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )
        child_detail = ps_http.timeline_detail(
            tenant_id,
            child_timeline_id,
        )
        assert child_detail["is_archived"] is False

        with env.endpoints.create_start(
            "test_archived_branch_persisted", tenant_id=tenant_id
        ) as endpoint:
            sum_again = endpoint.safe_psql("SELECT sum(key) from foo where key < 500")
            assert sum == sum_again

        assert_prefix_empty(
            neon_env_builder.pageserver_remote_storage,
            prefix=f"tenants/{str(env.initial_tenant)}/tenant-manifest",
        )

    assert not timeline_offloaded_api(root_timeline_id)

    ps_http.tenant_delete(tenant_id)

    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/",
    )


@run_only_on_default_postgres("this test isn't sensitive to the contents of timelines")
def test_timeline_archival_chaos(neon_env_builder: NeonEnvBuilder):
    """
    A general consistency check on archival/offload timeline state, and its intersection
    with tenant migrations and timeline deletions.
    """

    # Offloading is off by default a time of writing: remove this line when it's on by default
    neon_env_builder.pageserver_config_override = "timeline_offloading = true"
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    # We will exercise migrations, so need multiple pageservers
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "compaction_period": "1s",
        }
    )
    tenant_id = env.initial_tenant
    tenant_shard_id = TenantShardId(tenant_id, 0, 0)

    # Unavailable pageservers during timeline CRUD operations can be logged as errors on the storage controller
    env.storage_controller.allowed_errors.append(".*error sending request.*")

    for ps in env.pageservers:
        # We will do unclean restarts, which results in these messages when cleaning up files
        ps.allowed_errors.extend(
            [
                ".*removing local file.*because it has unexpected length.*",
                ".*__temp.*",
                # FIXME: there is a code path in timeline deletion that maps cancellation to 500
                ".*InternalServerError.*Cancelled.*",
                # FIXME: there is a code path in timeline deletion that maps not yet initialized to 500
                ".*InternalServerError.*Tenant not yet active.*",
            ]
        )

    class TimelineState:
        def __init__(self):
            self.timeline_id = TimelineId.generate()
            self.created = False
            self.archived = False
            self.offloaded = False
            self.deleted = False

    controller_ps_api = env.storage_controller.pageserver_api()

    shutdown = threading.Event()

    violations = []

    timelines_deleted = []

    def list_timelines(tenant_id) -> tuple[set[TimelineId], set[TimelineId]]:
        """Get the list of active and offloaded TimelineId"""
        listing = controller_ps_api.timeline_and_offloaded_list(tenant_id)
        active_ids = set([TimelineId(t["timeline_id"]) for t in listing.timelines])
        offloaded_ids = set([TimelineId(t["timeline_id"]) for t in listing.offloaded])

        return (active_ids, offloaded_ids)

    def timeline_objects(tenant_shard_id, timeline_id):
        response = list_prefix(
            env.pageserver_remote_storage,  # type: ignore
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_shard_id),
                    "timelines",
                    str(timeline_id),
                )
            )
            + "/",
        )

        log.info(f"Listing {timeline_id}: {response}")
        return [k["Key"] for k in response.get("Contents", [])]

    def worker():
        """
        Background thread which drives timeline lifecycle operations, and checks that between steps
        it obeys invariants. This should detect errors in pageserver persistence and in errors in
        concurrent operations on different timelines when it is run many times in parallel.
        """
        state = TimelineState()

        # Jitter worker startup, we're not interested in exercising lots of concurrent creations
        # as we know that's I/O bound.
        shutdown.wait(random.random() * 10)

        while not shutdown.is_set():
            # A little wait between actions to jitter out the API calls rather than having them
            # all queue up at once
            shutdown.wait(random.random())

            try:
                if not state.created:
                    log.info(f"Creating timeline {state.timeline_id}")
                    controller_ps_api.timeline_create(
                        PgVersion.NOT_SET, tenant_id=tenant_id, new_timeline_id=state.timeline_id
                    )
                    state.created = True

                    if (
                        timeline_objects(
                            tenant_shard_id=tenant_shard_id, timeline_id=state.timeline_id
                        )
                        == []
                    ):
                        msg = f"Timeline {state.timeline_id} unexpectedly not present in remote storage"
                        violations.append(msg)

                elif state.deleted:
                    # Try to confirm its deletion completed.
                    # Deleted timeline should not appear in listing API, either as offloaded or active
                    (active_ids, offloaded_ids) = list_timelines(tenant_id)
                    if state.timeline_id in active_ids or state.timeline_id in offloaded_ids:
                        msg = f"Timeline {state.timeline_id} appeared in listing after deletion was acked"
                        violations.append(msg)
                        raise RuntimeError(msg)

                    objects = timeline_objects(tenant_shard_id, state.timeline_id)
                    if len(objects) == 0:
                        log.info(f"Confirmed deletion of timeline {state.timeline_id}")
                        timelines_deleted.append(state.timeline_id)
                        state = TimelineState()  # A new timeline ID to create on next iteration
                    else:
                        # Deletion of objects doesn't have to be synchronous, we will keep polling
                        log.info(f"Timeline {state.timeline_id} objects still exist: {objects}")
                        shutdown.wait(random.random())
                else:
                    # The main lifetime of a timeline: proceed active->archived->offloaded->deleted
                    if not state.archived:
                        log.info(f"Archiving timeline {state.timeline_id}")
                        controller_ps_api.timeline_archival_config(
                            tenant_id, state.timeline_id, TimelineArchivalState.ARCHIVED
                        )
                        state.archived = True
                    elif state.archived and not state.offloaded:
                        log.info(f"Waiting for offload of timeline {state.timeline_id}")
                        # Wait for offload: this should happen fast because we configured a short compaction interval
                        while not shutdown.is_set():
                            (active_ids, offloaded_ids) = list_timelines(tenant_id)
                            if state.timeline_id in active_ids:
                                log.info(f"Timeline {state.timeline_id} is still active")
                                shutdown.wait(0.5)
                            elif state.timeline_id in offloaded_ids:
                                log.info(f"Timeline {state.timeline_id} is now offloaded")
                                state.offloaded = True
                                break
                            else:
                                # Timeline is neither offloaded nor active, this is unexpected: the pageserver
                                # should ensure that the timeline appears in either the offloaded list or main list
                                msg = f"Timeline {state.timeline_id} disappeared!"
                                violations.append(msg)
                                raise RuntimeError(msg)
                    elif state.offloaded:
                        # Once it's offloaded it should only be in offloaded or deleted state: check
                        # it didn't revert back to active.  This tests that the manfiest is doing its
                        # job to suppress loading of offloaded timelines as active.
                        (active_ids, offloaded_ids) = list_timelines(tenant_id)
                        if state.timeline_id in active_ids:
                            msg = f"Timeline {state.timeline_id} is active, should be offloaded or deleted"
                            violations.append(msg)
                            raise RuntimeError(msg)

                        log.info(f"Deleting timeline {state.timeline_id}")
                        controller_ps_api.timeline_delete(tenant_id, state.timeline_id)
                        state.deleted = True
                    else:
                        raise RuntimeError("State should be unreachable")
            except PageserverApiException as e:
                # This is expected: we are injecting chaos, API calls will sometimes fail.
                # TODO: can we narrow this to assert we are getting friendly 503s?
                log.info(f"Iteration error, will retry: {e}")
                shutdown.wait(random.random())
            except requests.exceptions.RetryError as e:
                # Retryable error repeated more times than `requests` is configured to tolerate, this
                # is expected when a pageserver remains unavailable for a couple seconds
                log.info(f"Iteration error, will retry: {e}")
                shutdown.wait(random.random())
            except Exception as e:
                log.warning(
                    f"Unexpected worker exception (current timeline {state.timeline_id}): {e}"
                )
            else:
                # In the non-error case, use a jitterd but small wait, we want to keep
                # a high rate of operations going
                shutdown.wait(random.random() * 0.1)

    n_workers = 4
    threads = []
    for _i in range(0, n_workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # Set delay failpoints so that deletions and migrations take some time, and have a good
    # chance to interact with other concurrent timeline mutations.
    env.storage_controller.configure_failpoints(
        [("reconciler-live-migrate-pre-await-lsn", "sleep(1)")]
    )
    for ps in env.pageservers:
        # TODO: make this stick across restarts
        ps.http_client().configure_failpoints([("in_progress_delete", "sleep(1)")])

    # Generate some chaos, while our workers are trying to complete their timeline operations
    rng = random.Random()
    try:
        chaos_rounds = 32
        for _i in range(0, chaos_rounds):
            action = rng.choice([0, 1])
            if action == 0:
                # Pick a random pageserver to gracefully restart
                pageserver = rng.choice(env.pageservers)

                # Whether to use a graceful shutdown or SIGKILL
                immediate = random.choice([True, False])
                log.info(f"Restarting pageserver {pageserver.id}, immediate={immediate}")

                t1 = time.time()
                pageserver.restart(immediate=immediate)
                restart_duration = time.time() - t1

                # Make sure we're up for as long as we spent restarting, to ensure operations can make progress
                log.info(f"Staying alive for {restart_duration}s")
                time.sleep(restart_duration)
            else:
                # Migrate our tenant between pageservers
                origin_ps = env.get_tenant_pageserver(tenant_shard_id)
                dest_ps = rng.choice([ps for ps in env.pageservers if ps.id != origin_ps.id])
                log.info(f"Migrating {tenant_shard_id} {origin_ps.id}->{dest_ps.id}")
                env.storage_controller.tenant_shard_migrate(
                    tenant_shard_id=tenant_shard_id, dest_ps_id=dest_ps.id
                )

            log.info(f"Full timeline lifecycles so far: {len(timelines_deleted)}")
    finally:
        shutdown.set()

        for thread in threads:
            thread.join()

    # Sanity check that during our run we did exercise some full timeline lifecycles, in case
    # one of our workers got stuck
    assert len(timelines_deleted) > 10

    # That no invariant-violations were reported by workers
    assert violations == []
