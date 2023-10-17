"""

Tests in this module exercise the pageserver's behavior around generation numbers,
as defined in docs/rfcs/025-generation-numbers.md.  Briefly, the behaviors we require
of the pageserver are:
- Do not start a tenant without a generation number if control_plane_api is set
- Remote objects must be suffixed with generation
- Deletions may only be executed after validating generation
- Updates to remote_consistent_lsn may only be made visible after validating generation
"""


import enum
import re
import time
from typing import Optional

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import list_prefix
from fixtures.remote_storage import (
    RemoteStorageKind,
)
from fixtures.types import TenantId, TimelineId
from fixtures.utils import print_gc_result, wait_until

# A tenant configuration that is convenient for generating uploads and deletions
# without a large amount of postgres traffic.
TENANT_CONF = {
    # small checkpointing and compaction targets to ensure we generate many upload operations
    "checkpoint_distance": f"{128 * 1024}",
    "compaction_threshold": "1",
    "compaction_target_size": f"{128 * 1024}",
    # no PITR horizon, we specify the horizon when we request on-demand GC
    "pitr_interval": "0s",
    # disable background compaction and GC. We invoke it manually when we want it to happen.
    "gc_period": "0s",
    "compaction_period": "0s",
    # create image layers eagerly, so that GC can remove some layers
    "image_creation_threshold": "1",
}


def generate_uploads_and_deletions(
    env: NeonEnv,
    *,
    init: bool = True,
    tenant_id: Optional[TenantId] = None,
    timeline_id: Optional[TimelineId] = None,
    data: Optional[str] = None,
):
    """
    Using the environment's default tenant + timeline, generate a load pattern
    that results in some uploads and some deletions to remote storage.
    """

    if tenant_id is None:
        tenant_id = env.initial_tenant
    assert tenant_id is not None

    if timeline_id is None:
        timeline_id = env.initial_timeline
    assert timeline_id is not None

    ps_http = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        if init:
            endpoint.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")
            last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

        def churn(data):
            endpoint.safe_psql_many(
                [
                    f"""
                INSERT INTO foo (id, val)
                SELECT g, '{data}'
                FROM generate_series(1, 200) g
                ON CONFLICT (id) DO UPDATE
                SET val = EXCLUDED.val
                """,
                    # to ensure that GC can actually remove some layers
                    "VACUUM foo",
                ]
            )
            assert tenant_id is not None
            assert timeline_id is not None
            wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
            ps_http.timeline_checkpoint(tenant_id, timeline_id)

        # Compaction should generate some GC-elegible layers
        for i in range(0, 2):
            churn(f"{i if data is None else data}")

        gc_result = ps_http.timeline_gc(tenant_id, timeline_id, 0)
        print_gc_result(gc_result)
        assert gc_result["layers_removed"] > 0


def read_all(
    env: NeonEnv, tenant_id: Optional[TenantId] = None, timeline_id: Optional[TimelineId] = None
):
    if tenant_id is None:
        tenant_id = env.initial_tenant
    assert tenant_id is not None

    if timeline_id is None:
        timeline_id = env.initial_timeline
    assert timeline_id is not None

    env.pageserver.http_client()
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        endpoint.safe_psql("SELECT SUM(LENGTH(val)) FROM foo;")


def get_metric_or_0(ps_http, metric: str) -> int:
    v = ps_http.get_metric_value(metric)
    return 0 if v is None else int(v)


def get_deletion_queue_executed(ps_http) -> int:
    return get_metric_or_0(ps_http, "pageserver_deletion_queue_executed_total")


def get_deletion_queue_submitted(ps_http) -> int:
    return get_metric_or_0(ps_http, "pageserver_deletion_queue_submitted_total")


def get_deletion_queue_validated(ps_http) -> int:
    return get_metric_or_0(ps_http, "pageserver_deletion_queue_validated_total")


def get_deletion_queue_dropped(ps_http) -> int:
    return get_metric_or_0(ps_http, "pageserver_deletion_queue_dropped_total")


def get_deletion_queue_unexpected_errors(ps_http) -> int:
    return get_metric_or_0(ps_http, "pageserver_deletion_queue_unexpected_errors_total")


def get_deletion_queue_dropped_lsn_updates(ps_http) -> int:
    return get_metric_or_0(ps_http, "pageserver_deletion_queue_dropped_lsn_updates_total")


def get_deletion_queue_depth(ps_http) -> int:
    """
    Queue depth if at least one deletion has been submitted, else None
    """
    submitted = get_deletion_queue_submitted(ps_http)
    executed = get_deletion_queue_executed(ps_http)
    dropped = get_deletion_queue_dropped(ps_http)
    depth = submitted - executed - dropped
    log.info(f"get_deletion_queue_depth: {depth} ({submitted} - {executed} - {dropped})")

    assert depth >= 0
    return int(depth)


def assert_deletion_queue(ps_http, size_fn) -> None:
    v = get_deletion_queue_depth(ps_http)
    assert v is not None
    assert size_fn(v) is True


def test_generations_upgrade(neon_env_builder: NeonEnvBuilder):
    """
    Validate behavior when a pageserver is run without generation support enabled,
    then started again after activating it:
    - Before upgrade, no objects should have generation suffixes
    - After upgrade, the bucket should contain a mixture.
    - In both cases, postgres I/O should work.
    """
    neon_env_builder.enable_generations = True
    neon_env_builder.enable_pageserver_remote_storage(
        RemoteStorageKind.MOCK_S3,
    )

    env = neon_env_builder.init_configs()
    env.broker.try_start()
    for sk in env.safekeepers:
        sk.start()
    assert env.attachment_service is not None
    env.attachment_service.start()

    env.pageserver.start(overrides=('--pageserver-config-override=control_plane_api=""',))

    env.neon_cli.create_tenant(
        tenant_id=env.initial_tenant, conf=TENANT_CONF, timeline_id=env.initial_timeline
    )
    generate_uploads_and_deletions(env)

    def parse_generation_suffix(key):
        m = re.match(".+-([0-9a-zA-Z]{8})$", key)
        if m is None:
            return None
        else:
            log.info(f"match: {m}")
            log.info(f"group: {m.group(1)}")
            return int(m.group(1), 16)

    pre_upgrade_keys = list(
        [o["Key"] for o in list_prefix(neon_env_builder, delimiter="")["Contents"]]
    )
    for key in pre_upgrade_keys:
        assert parse_generation_suffix(key) is None

    env.pageserver.stop()

    # Starting without the override that disabled control_plane_api
    env.pageserver.start()

    generate_uploads_and_deletions(env, init=False)

    legacy_objects: list[str] = []
    suffixed_objects = []
    post_upgrade_keys = list(
        [o["Key"] for o in list_prefix(neon_env_builder, delimiter="")["Contents"]]
    )
    for key in post_upgrade_keys:
        log.info(f"post-upgrade key: {key}")
        if parse_generation_suffix(key) is not None:
            suffixed_objects.append(key)
        else:
            legacy_objects.append(key)

    # Bucket now contains a mixture of suffixed and non-suffixed objects
    assert len(suffixed_objects) > 0
    assert len(legacy_objects) > 0

    assert get_deletion_queue_unexpected_errors(env.pageserver.http_client()) == 0


def test_deferred_deletion(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_generations = True
    neon_env_builder.enable_pageserver_remote_storage(
        RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    assert env.attachment_service is not None

    some_other_pageserver = 1234
    ps_http = env.pageserver.http_client()

    generate_uploads_and_deletions(env)

    # Flush: pending deletions should all complete
    assert_deletion_queue(ps_http, lambda n: n > 0)
    ps_http.deletion_queue_flush(execute=True)
    assert_deletion_queue(ps_http, lambda n: n == 0)
    assert get_deletion_queue_dropped(ps_http) == 0

    # Our visible remote_consistent_lsn should match projected
    timeline = ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)
    assert timeline["remote_consistent_lsn"] == timeline["remote_consistent_lsn_visible"]
    assert get_deletion_queue_dropped_lsn_updates(ps_http) == 0

    env.pageserver.allowed_errors.extend(
        [".*Dropped remote consistent LSN updates.*", ".*Dropping stale deletions.*"]
    )

    # Now advance the generation in the control plane: subsequent validations
    # from the running pageserver will fail.  No more deletions should happen.
    env.attachment_service.attach_hook(env.initial_tenant, some_other_pageserver)
    generate_uploads_and_deletions(env, init=False)

    assert_deletion_queue(ps_http, lambda n: n > 0)
    queue_depth_before = get_deletion_queue_depth(ps_http)
    executed_before = get_deletion_queue_executed(ps_http)
    ps_http.deletion_queue_flush(execute=True)

    # Queue drains to zero because we dropped deletions
    assert_deletion_queue(ps_http, lambda n: n == 0)
    # The executed counter has not incremented
    assert get_deletion_queue_executed(ps_http) == executed_before
    # The dropped counter has incremented to consume all of the deletions that were previously enqueued
    assert get_deletion_queue_dropped(ps_http) == queue_depth_before

    # Flush to S3 and see that remote_consistent_lsn does not advance: it cannot
    # because generation validation fails.
    timeline = ps_http.timeline_detail(env.initial_tenant, env.initial_timeline)
    assert timeline["remote_consistent_lsn"] != timeline["remote_consistent_lsn_visible"]
    assert get_deletion_queue_dropped_lsn_updates(ps_http) > 0

    # TODO: list bucket and confirm all objects have a generation suffix.

    assert get_deletion_queue_unexpected_errors(ps_http) == 0


class KeepAttachment(str, enum.Enum):
    KEEP = "keep"
    LOSE = "lose"


class ValidateBefore(str, enum.Enum):
    VALIDATE = "validate"
    NO_VALIDATE = "no-validate"


@pytest.mark.parametrize("keep_attachment", [KeepAttachment.KEEP, KeepAttachment.LOSE])
@pytest.mark.parametrize("validate_before", [ValidateBefore.VALIDATE, ValidateBefore.NO_VALIDATE])
def test_deletion_queue_recovery(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    keep_attachment: KeepAttachment,
    validate_before: ValidateBefore,
):
    """
    :param keep_attachment: whether to re-attach after restart.  Else, we act as if some other
    node took the attachment while we were restarting.
    :param validate_before: whether to wait for deletions to be validated before restart.  This
    makes them elegible to be executed after restart, if the same node keeps the attachment.
    """
    neon_env_builder.enable_generations = True
    neon_env_builder.enable_pageserver_remote_storage(
        RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)

    ps_http = env.pageserver.http_client()

    failpoints = [
        # Prevent deletion lists from being executed, to build up some backlog of deletions
        ("deletion-queue-before-execute", "return"),
    ]

    if validate_before == ValidateBefore.NO_VALIDATE:
        failpoints.append(
            # Prevent deletion lists from being validated, we will test that they are
            # dropped properly during recovery.  'pause' is okay here because we kill
            # the pageserver with immediate=true
            ("control-plane-client-validate", "pause")
        )

    ps_http.configure_failpoints(failpoints)

    generate_uploads_and_deletions(env)

    # There should be entries in the deletion queue
    assert_deletion_queue(ps_http, lambda n: n > 0)
    ps_http.deletion_queue_flush()
    before_restart_depth = get_deletion_queue_depth(ps_http)

    assert get_deletion_queue_unexpected_errors(ps_http) == 0
    assert get_deletion_queue_dropped_lsn_updates(ps_http) == 0

    if validate_before == ValidateBefore.VALIDATE:

        def assert_validation_complete():
            assert get_deletion_queue_submitted(ps_http) == get_deletion_queue_validated(ps_http)

        wait_until(20, 1, assert_validation_complete)

        # The validatated keys statistic advances before the header is written, so we
        # also wait to see the header hit the disk: this seems paranoid but the race
        # can really happen on a heavily overloaded test machine.
        def assert_header_written():
            assert (env.pageserver.workdir / "deletion" / "header-01").exists()

        wait_until(20, 1, assert_header_written)

    log.info(f"Restarting pageserver with {before_restart_depth} deletions enqueued")
    env.pageserver.stop(immediate=True)

    if keep_attachment == KeepAttachment.LOSE:
        some_other_pageserver = 101010
        assert env.attachment_service is not None
        env.attachment_service.attach_hook(env.initial_tenant, some_other_pageserver)

    env.pageserver.start()

    def assert_deletions_submitted(n: int):
        assert ps_http.get_metric_value("pageserver_deletion_queue_submitted_total") == n

    # After restart, issue a flush to kick the deletion frontend to do recovery.
    # It should recover all the operations we submitted before the restart.
    ps_http.deletion_queue_flush(execute=False)
    wait_until(20, 0.25, lambda: assert_deletions_submitted(before_restart_depth))

    # The queue should drain through completely if we flush it
    ps_http.deletion_queue_flush(execute=True)
    wait_until(10, 1, lambda: assert_deletion_queue(ps_http, lambda n: n == 0))

    if keep_attachment == KeepAttachment.KEEP or validate_before == ValidateBefore.VALIDATE:
        # - If we kept the attachment, then our pre-restart deletions should execute
        #   because on re-attach they were from the immediately preceding generation
        # - If we validated before restart, then the deletions should execute because the
        #   deletion queue header records a validated deletion list sequence number.
        assert get_deletion_queue_executed(ps_http) == before_restart_depth
    else:
        env.pageserver.allowed_errors.extend([".*Dropping stale deletions.*"])

        # If we lost the attachment, we should have dropped our pre-restart deletions.
        assert get_deletion_queue_dropped(ps_http) == before_restart_depth

    assert get_deletion_queue_unexpected_errors(ps_http) == 0
    assert get_deletion_queue_dropped_lsn_updates(ps_http) == 0

    # Restart again
    env.pageserver.stop(immediate=True)
    env.pageserver.start()

    # No deletion lists should be recovered: this demonstrates that deletion lists
    # were cleaned up after being executed or dropped in the previous process lifetime.
    time.sleep(1)
    assert_deletion_queue(ps_http, lambda n: n == 0)

    assert get_deletion_queue_unexpected_errors(ps_http) == 0
    assert get_deletion_queue_dropped_lsn_updates(ps_http) == 0


def test_emergency_mode(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    neon_env_builder.enable_generations = True
    neon_env_builder.enable_pageserver_remote_storage(
        RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)

    ps_http = env.pageserver.http_client()

    generate_uploads_and_deletions(env)

    env.pageserver.allowed_errors.extend(
        [
            # When the pageserver can't reach the control plane, it will complain
            ".*calling control plane generation validation API failed.*",
            # Emergency mode is a big deal, we log errors whenever it is used.
            ".*Emergency mode!.*",
        ]
    )

    # Simulate a major incident: the control plane goes offline
    assert env.attachment_service is not None
    env.attachment_service.stop()

    # Remember how many validations had happened before the control plane went offline
    validated = get_deletion_queue_validated(ps_http)

    generate_uploads_and_deletions(env, init=False)

    # The running pageserver should stop progressing deletions
    time.sleep(10)
    assert get_deletion_queue_validated(ps_http) == validated

    # Restart the pageserver: ordinarily we would _avoid_ doing this during such an
    # incident, but it might be unavoidable: if so, we want to be able to start up
    # and serve clients.
    env.pageserver.stop()  # Non-immediate: implicitly checking that shutdown doesn't hang waiting for CP
    env.pageserver.start(
        overrides=("--pageserver-config-override=control_plane_emergency_mode=true",)
    )

    # The pageserver should provide service to clients
    generate_uploads_and_deletions(env, init=False)

    # The pageserver should neither validate nor execute any deletions, it should have
    # loaded the DeletionLists from before though
    time.sleep(10)
    assert get_deletion_queue_depth(ps_http) > 0
    assert get_deletion_queue_validated(ps_http) == 0
    assert get_deletion_queue_executed(ps_http) == 0

    # When the control plane comes back up, normal service should resume
    env.attachment_service.start()

    ps_http.deletion_queue_flush(execute=True)
    assert get_deletion_queue_depth(ps_http) == 0
    assert get_deletion_queue_validated(ps_http) > 0
    assert get_deletion_queue_executed(ps_http) > 0

    # The pageserver should work fine when subsequently restarted in non-emergency mode
    env.pageserver.stop()  # Non-immediate: implicitly checking that shutdown doesn't hang waiting for CP
    env.pageserver.start()

    generate_uploads_and_deletions(env, init=False)
    ps_http.deletion_queue_flush(execute=True)
    assert get_deletion_queue_depth(ps_http) == 0
    assert get_deletion_queue_validated(ps_http) > 0
    assert get_deletion_queue_executed(ps_http) > 0


def evict_all_layers(env: NeonEnv, tenant_id: TenantId, timeline_id: TimelineId):
    timeline_path = env.pageserver.timeline_dir(tenant_id, timeline_id)
    initial_local_layers = sorted(
        list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    )
    client = env.pageserver.http_client()
    for layer in initial_local_layers:
        if "ephemeral" in layer.name or "temp" in layer.name:
            continue
        log.info(f"Evicting layer {tenant_id}/{timeline_id} {layer.name}")
        client.evict_layer(tenant_id=tenant_id, timeline_id=timeline_id, layer_name=layer.name)


def test_eviction_across_generations(neon_env_builder: NeonEnvBuilder):
    """
    Eviction and on-demand downloads exercise a particular code path where RemoteLayer is constructed
    and must be constructed using the proper generation for the layer, which may not be the same generation
    that the tenant is running in.
    """
    neon_env_builder.enable_generations = True
    neon_env_builder.enable_pageserver_remote_storage(
        RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    env.pageserver.http_client()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    generate_uploads_and_deletions(env)

    read_all(env, tenant_id, timeline_id)
    evict_all_layers(env, tenant_id, timeline_id)
    read_all(env, tenant_id, timeline_id)

    # This will cause the generation to increment
    env.pageserver.stop()
    env.pageserver.start()

    # Now we are running as generation 2, but must still correctly remember that the layers
    # we are evicting and downloading are from generation 1.
    read_all(env, tenant_id, timeline_id)
    evict_all_layers(env, tenant_id, timeline_id)
    read_all(env, tenant_id, timeline_id)
