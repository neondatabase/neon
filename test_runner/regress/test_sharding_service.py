import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    AttachmentServiceApiException,
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    TokenScope,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    enable_remote_storage_versioning,
    list_prefix,
    remote_storage_delete_key,
    tenant_delete_wait_completed,
    timeline_delete_wait_completed,
)
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import RemoteStorageKind, s3_storage
from fixtures.types import TenantId, TimelineId
from fixtures.utils import run_pg_bench_small, wait_until
from mypy_boto3_s3.type_defs import (
    ObjectTypeDef,
)
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


def get_node_shard_counts(env: NeonEnv, tenant_ids):
    counts: defaultdict[str, int] = defaultdict(int)
    for tid in tenant_ids:
        for shard in env.attachment_service.locate(tid):
            counts[shard["node_id"]] += 1
    return counts


def test_sharding_service_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basic lifecycle of a sharding service:
    - Restarting
    - Restarting a pageserver
    - Creating and deleting tenants and timelines
    - Marking a pageserver offline
    """

    neon_env_builder.num_pageservers = 3
    env = neon_env_builder.init_configs()

    for pageserver in env.pageservers:
        # This test detaches tenants during migration, which can race with deletion queue operations,
        # during detach we only do an advisory flush, we don't wait for it.
        pageserver.allowed_errors.extend([".*Dropped remote consistent LSN updates.*"])

    # Start services by hand so that we can skip a pageserver (this will start + register later)
    env.broker.try_start()
    env.attachment_service.start()
    env.pageservers[0].start()
    env.pageservers[1].start()
    for sk in env.safekeepers:
        sk.start()

    # The pageservers we started should have registered with the sharding service on startup
    nodes = env.attachment_service.node_list()
    assert len(nodes) == 2
    assert set(n["id"] for n in nodes) == {env.pageservers[0].id, env.pageservers[1].id}

    # Starting an additional pageserver should register successfully
    env.pageservers[2].start()
    nodes = env.attachment_service.node_list()
    assert len(nodes) == 3
    assert set(n["id"] for n in nodes) == {ps.id for ps in env.pageservers}

    # Use a multiple of pageservers to get nice even number of shards on each one
    tenant_shard_count = len(env.pageservers) * 4
    tenant_count = len(env.pageservers) * 2
    shards_per_tenant = tenant_shard_count // tenant_count
    tenant_ids = set(TenantId.generate() for i in range(0, tenant_count))

    # Creating several tenants should spread out across the pageservers
    for tid in tenant_ids:
        env.neon_cli.create_tenant(tid, shard_count=shards_per_tenant)

    for node_id, count in get_node_shard_counts(env, tenant_ids).items():
        # we used a multiple of pagservers for the total shard count,
        # so expect equal number on all pageservers
        assert count == tenant_shard_count / len(
            env.pageservers
        ), f"Node {node_id} has bad count {count}"

    # Creating and deleting timelines should work, using identical API to pageserver
    timeline_crud_tenant = next(iter(tenant_ids))
    timeline_id = TimelineId.generate()
    env.attachment_service.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=timeline_crud_tenant, new_timeline_id=timeline_id
    )
    timelines = env.attachment_service.pageserver_api().timeline_list(timeline_crud_tenant)
    assert len(timelines) == 2
    assert timeline_id in set(TimelineId(t["timeline_id"]) for t in timelines)
    #    virtual_ps_http.timeline_delete(tenant_id=timeline_crud_tenant, timeline_id=timeline_id)
    timeline_delete_wait_completed(
        env.attachment_service.pageserver_api(), timeline_crud_tenant, timeline_id
    )
    timelines = env.attachment_service.pageserver_api().timeline_list(timeline_crud_tenant)
    assert len(timelines) == 1
    assert timeline_id not in set(TimelineId(t["timeline_id"]) for t in timelines)

    # Marking a pageserver offline should migrate tenants away from it.
    env.attachment_service.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    def node_evacuated(node_id: int) -> None:
        counts = get_node_shard_counts(env, tenant_ids)
        assert counts[node_id] == 0

    wait_until(10, 1, lambda: node_evacuated(env.pageservers[0].id))

    # Marking pageserver active should not migrate anything to it
    # immediately
    env.attachment_service.node_configure(env.pageservers[0].id, {"availability": "Active"})
    time.sleep(1)
    assert get_node_shard_counts(env, tenant_ids)[env.pageservers[0].id] == 0

    # Restarting a pageserver should not detach any tenants (i.e. /re-attach works)
    before_restart = env.pageservers[1].http_client().tenant_list_locations()
    env.pageservers[1].stop()
    env.pageservers[1].start()
    after_restart = env.pageservers[1].http_client().tenant_list_locations()
    assert len(after_restart) == len(before_restart)

    # Locations should be the same before & after restart, apart from generations
    for _shard_id, tenant in after_restart["tenant_shards"]:
        del tenant["generation"]
    for _shard_id, tenant in before_restart["tenant_shards"]:
        del tenant["generation"]
    assert before_restart == after_restart

    # Delete all the tenants
    for tid in tenant_ids:
        tenant_delete_wait_completed(env.attachment_service.pageserver_api(), tid, 10)

    # Set a scheduling policy on one node, create all the tenants, observe
    # that the scheduling policy is respected.
    env.attachment_service.node_configure(env.pageservers[1].id, {"scheduling": "Draining"})

    # Create some fresh tenants
    tenant_ids = set(TenantId.generate() for i in range(0, tenant_count))
    for tid in tenant_ids:
        env.neon_cli.create_tenant(tid, shard_count=shards_per_tenant)

    counts = get_node_shard_counts(env, tenant_ids)
    # Nothing should have been scheduled on the node in Draining
    assert counts[env.pageservers[1].id] == 0
    assert counts[env.pageservers[0].id] == tenant_shard_count // 2
    assert counts[env.pageservers[2].id] == tenant_shard_count // 2

    env.attachment_service.consistency_check()


def test_node_status_after_restart(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # Initially we have two online pageservers
    nodes = env.attachment_service.node_list()
    assert len(nodes) == 2

    env.pageservers[1].stop()

    env.attachment_service.stop()
    env.attachment_service.start()

    def is_ready():
        assert env.attachment_service.ready() is True

    wait_until(30, 1, is_ready)

    # We loaded nodes from database on restart
    nodes = env.attachment_service.node_list()
    assert len(nodes) == 2

    # We should still be able to create a tenant, because the pageserver which is still online
    # should have had its availabilty state set to Active.
    env.attachment_service.tenant_create(TenantId.generate())

    env.attachment_service.consistency_check()


def test_sharding_service_passthrough(
    neon_env_builder: NeonEnvBuilder,
):
    """
    For simple timeline/tenant GET APIs that don't require coordination across
    shards, the sharding service implements a proxy to shard zero.  This test
    calls those APIs.
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # We will talk to attachment service as if it was a pageserver, using the pageserver
    # HTTP client
    client = PageserverHttpClient(env.attachment_service_port, lambda: True)
    timelines = client.timeline_list(tenant_id=env.initial_tenant)
    assert len(timelines) == 1

    status = client.tenant_status(env.initial_tenant)
    assert TenantId(status["id"]) == env.initial_tenant
    assert set(TimelineId(t) for t in status["timelines"]) == {
        env.initial_timeline,
    }
    assert status["state"]["slug"] == "Active"

    env.attachment_service.consistency_check()


def test_sharding_service_restart(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    tenant_a = env.initial_tenant
    tenant_b = TenantId.generate()
    env.attachment_service.tenant_create(tenant_b)
    env.pageserver.tenant_detach(tenant_a)

    # TODO: extend this test to use multiple pageservers, and check that locations don't move around
    # on restart.

    # Attachment service restart
    env.attachment_service.stop()
    env.attachment_service.start()

    observed = set(TenantId(tenant["id"]) for tenant in env.pageserver.http_client().tenant_list())

    # Tenant A should still be attached
    assert tenant_a not in observed

    # Tenant B should remain detached
    assert tenant_b in observed

    # Pageserver restart
    env.pageserver.stop()
    env.pageserver.start()

    # Same assertions as above: restarting either service should not perturb things
    observed = set(TenantId(tenant["id"]) for tenant in env.pageserver.http_client().tenant_list())
    assert tenant_a not in observed
    assert tenant_b in observed

    env.attachment_service.consistency_check()


def test_sharding_service_onboarding(
    neon_env_builder: NeonEnvBuilder,
):
    """
    We onboard tenants to the sharding service by treating it as a 'virtual pageserver'
    which provides the /location_config API.  This is similar to creating a tenant,
    but imports the generation number.
    """

    neon_env_builder.num_pageservers = 2

    # Start services by hand so that we can skip registration on one of the pageservers
    env = neon_env_builder.init_configs()
    env.broker.try_start()
    env.attachment_service.start()

    # This is the pageserver where we'll initially create the tenant.  Run it in emergency
    # mode so that it doesn't talk to storage controller, and do not register it.
    env.pageservers[0].allowed_errors.append(".*Emergency mode!.*")
    env.pageservers[0].start(
        overrides=("--pageserver-config-override=control_plane_emergency_mode=true",),
        register=False,
    )
    origin_ps = env.pageservers[0]

    # This is the pageserver managed by the sharding service, where the tenant
    # will be attached after onboarding
    env.pageservers[1].start(register=True)
    dest_ps = env.pageservers[1]
    virtual_ps_http = PageserverHttpClient(env.attachment_service_port, lambda: True)

    for sk in env.safekeepers:
        sk.start()

    # Create a tenant directly via pageserver HTTP API, skipping the attachment service
    tenant_id = TenantId.generate()
    generation = 123
    origin_ps.http_client().tenant_create(tenant_id, generation=generation)

    # As if doing a live migration, first configure origin into stale mode
    origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedStale",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )

    # Call into attachment service to onboard the tenant
    generation += 1
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedMulti",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )

    # As if doing a live migration, detach the original pageserver
    origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    # As if doing a live migration, call into the attachment service to
    # set it to AttachedSingle: this is a no-op, but we test it because the
    # cloud control plane may call this for symmetry with live migration to
    # an individual pageserver
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )

    # We should see the tenant is now attached to the pageserver managed
    # by the sharding service
    origin_tenants = origin_ps.http_client().tenant_list()
    assert len(origin_tenants) == 0
    dest_tenants = dest_ps.http_client().tenant_list()
    assert len(dest_tenants) == 1
    assert TenantId(dest_tenants[0]["id"]) == tenant_id

    # sharding service advances generation by 1 when it first attaches
    assert dest_tenants[0]["generation"] == generation + 1

    # The onboarded tenant should survive a restart of sharding service
    env.attachment_service.stop()
    env.attachment_service.start()

    # The onboarded tenant should surviev a restart of pageserver
    dest_ps.stop()
    dest_ps.start()

    env.attachment_service.consistency_check()


def test_sharding_service_compute_hook(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address,
):
    """
    Test that the sharding service calls out to the configured HTTP endpoint on attachment changes
    """

    # We will run two pageserver to migrate and check that the attachment service sends notifications
    # when migrating.
    neon_env_builder.num_pageservers = 2
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"

    # Set up fake HTTP notify endpoint
    notifications = []

    def handler(request: Request):
        log.info(f"Notify request: {request}")
        notifications.append(request.json)
        return Response(status=200)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    # Start running
    env = neon_env_builder.init_start()

    # We will to an unclean migration, which will result in deletion queue warnings
    env.pageservers[0].allowed_errors.append(".*Dropped remote consistent LSN updates for tenant.*")

    # Initial notification from tenant creation
    assert len(notifications) == 1
    expect: Dict[str, Union[List[Dict[str, int]], str, None, int]] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
    }
    assert notifications[0] == expect

    env.attachment_service.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    def node_evacuated(node_id: int) -> None:
        counts = get_node_shard_counts(env, [env.initial_tenant])
        assert counts[node_id] == 0

    wait_until(10, 1, lambda: node_evacuated(env.pageservers[0].id))

    # Additional notification from migration
    log.info(f"notifications: {notifications}")
    expect = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[1].id), "shard_number": 0}],
    }

    def received_migration_notification():
        assert len(notifications) == 2
        assert notifications[1] == expect

    wait_until(20, 0.25, received_migration_notification)

    # When we restart, we should re-emit notifications for all tenants
    env.attachment_service.stop()
    env.attachment_service.start()

    def received_restart_notification():
        assert len(notifications) == 3
        assert notifications[2] == expect

    wait_until(10, 1, received_restart_notification)

    # Splitting a tenant should cause its stripe size to become visible in the compute notification
    env.attachment_service.tenant_shard_split(env.initial_tenant, shard_count=2)
    expect = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": 32768,
        "shards": [
            {"node_id": int(env.pageservers[1].id), "shard_number": 0},
            {"node_id": int(env.pageservers[1].id), "shard_number": 1},
        ],
    }

    def received_split_notification():
        assert len(notifications) == 4
        assert notifications[3] == expect

    wait_until(10, 1, received_split_notification)

    env.attachment_service.consistency_check()


def test_sharding_service_debug_apis(neon_env_builder: NeonEnvBuilder):
    """
    Verify that occasional-use debug APIs work as expected.  This is a lightweight test
    that just hits the endpoints to check that they don't bitrot.
    """

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    tenant_id = TenantId.generate()
    env.attachment_service.tenant_create(tenant_id, shard_count=2, shard_stripe_size=8192)

    # Check that the consistency check passes on a freshly setup system
    env.attachment_service.consistency_check()

    # These APIs are intentionally not implemented as methods on NeonAttachmentService, as
    # they're just for use in unanticipated circumstances.

    # Initial tenant (1 shard) and the one we just created (2 shards) should be visible
    response = env.attachment_service.request(
        "GET",
        f"{env.attachment_service_api}/debug/v1/tenant",
        headers=env.attachment_service.headers(TokenScope.ADMIN),
    )
    assert len(response.json()) == 3

    # Scheduler should report the expected nodes and shard counts
    response = env.attachment_service.request(
        "GET", f"{env.attachment_service_api}/debug/v1/scheduler"
    )
    # Two nodes, in a dict of node_id->node
    assert len(response.json()["nodes"]) == 2
    assert sum(v["shard_count"] for v in response.json()["nodes"].values()) == 3
    assert all(v["may_schedule"] for v in response.json()["nodes"].values())

    response = env.attachment_service.request(
        "POST",
        f"{env.attachment_service_api}/debug/v1/node/{env.pageservers[1].id}/drop",
        headers=env.attachment_service.headers(TokenScope.ADMIN),
    )
    assert len(env.attachment_service.node_list()) == 1

    response = env.attachment_service.request(
        "POST",
        f"{env.attachment_service_api}/debug/v1/tenant/{tenant_id}/drop",
        headers=env.attachment_service.headers(TokenScope.ADMIN),
    )

    # Tenant drop should be reflected in dump output
    response = env.attachment_service.request(
        "GET",
        f"{env.attachment_service_api}/debug/v1/tenant",
        headers=env.attachment_service.headers(TokenScope.ADMIN),
    )
    assert len(response.json()) == 1

    # Check that the 'drop' APIs didn't leave things in a state that would fail a consistency check: they're
    # meant to be unclean wrt the pageserver state, but not leave a broken storage controller behind.
    env.attachment_service.consistency_check()


def test_sharding_service_s3_time_travel_recovery(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    """
    Test for S3 time travel
    """

    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # Mock S3 doesn't have versioning enabled by default, enable it
    # (also do it before there is any writes to the bucket)
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        remote_storage = neon_env_builder.pageserver_remote_storage
        assert remote_storage, "remote storage not configured"
        enable_remote_storage_versioning(remote_storage)

    neon_env_builder.num_pageservers = 1

    env = neon_env_builder.init_start()
    virtual_ps_http = PageserverHttpClient(env.attachment_service_port, lambda: True)

    tenant_id = TenantId.generate()
    env.attachment_service.tenant_create(
        tenant_id,
        shard_count=2,
        shard_stripe_size=8192,
        tenant_config=MANY_SMALL_LAYERS_TENANT_CONFIG,
    )

    # Check that the consistency check passes
    env.attachment_service.consistency_check()

    branch_name = "main"
    timeline_id = env.neon_cli.create_timeline(
        branch_name,
        tenant_id=tenant_id,
    )
    # Write some nontrivial amount of data into the endpoint and wait until it is uploaded
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        endpoint.safe_psql("CREATE TABLE created_foo(id integer);")
        # last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

    # Give the data time to be uploaded
    time.sleep(4)

    # Detach the tenant
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    time.sleep(4)
    ts_before_disaster = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    # Simulate a "disaster": delete some random files from remote storage for one of the shards
    assert env.pageserver_remote_storage
    shard_id_for_list = "0002"
    objects: List[ObjectTypeDef] = list_prefix(
        env.pageserver_remote_storage,
        f"tenants/{tenant_id}-{shard_id_for_list}/timelines/{timeline_id}/",
    ).get("Contents", [])
    assert len(objects) > 1
    log.info(f"Found {len(objects)} objects in remote storage")
    should_delete = False
    for obj in objects:
        obj_key = obj["Key"]
        should_delete = not should_delete
        if not should_delete:
            log.info(f"Keeping key on remote storage: {obj_key}")
            continue
        log.info(f"Deleting key from remote storage: {obj_key}")
        remote_storage_delete_key(env.pageserver_remote_storage, obj_key)
        pass

    time.sleep(4)
    ts_after_disaster = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    # Do time travel recovery
    virtual_ps_http.tenant_time_travel_remote_storage(
        tenant_id, ts_before_disaster, ts_after_disaster, shard_counts=[2]
    )
    time.sleep(4)

    # Attach the tenant again
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": 100,
        },
    )

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        endpoint.safe_psql("SELECT * FROM created_foo;")

    env.attachment_service.consistency_check()


def test_sharding_service_auth(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()
    svc = env.attachment_service
    api = env.attachment_service_api

    tenant_id = TenantId.generate()
    body: Dict[str, Any] = {"new_tenant_id": str(tenant_id)}

    # No token
    with pytest.raises(
        AttachmentServiceApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("POST", f"{env.attachment_service_api}/v1/tenant", json=body)

    # Token with incorrect scope
    with pytest.raises(
        AttachmentServiceApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request("POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.ADMIN))

    # Token with correct scope
    svc.request(
        "POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.PAGE_SERVER_API)
    )

    # No token
    with pytest.raises(
        AttachmentServiceApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("GET", f"{api}/debug/v1/tenant")

    # Token with incorrect scope
    with pytest.raises(
        AttachmentServiceApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "GET", f"{api}/debug/v1/tenant", headers=svc.headers(TokenScope.GENERATIONS_API)
        )

    # No token
    with pytest.raises(
        AttachmentServiceApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("POST", f"{api}/upcall/v1/re-attach")

    # Token with incorrect scope
    with pytest.raises(
        AttachmentServiceApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "POST", f"{api}/upcall/v1/re-attach", headers=svc.headers(TokenScope.PAGE_SERVER_API)
        )
