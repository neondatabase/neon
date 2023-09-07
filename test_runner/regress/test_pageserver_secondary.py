import random
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, NeonPageserver
from fixtures.pageserver.utils import assert_prefix_empty, tenant_delete_wait_completed
from fixtures.remote_storage import RemoteStorageKind
from fixtures.types import TenantId, TimelineId
from fixtures.utils import wait_until
from fixtures.workload import Workload

# A tenant configuration that is convenient for generating uploads and deletions
# without a large amount of postgres traffic.
TENANT_CONF = {
    # small checkpointing and compaction targets to ensure we generate many upload operations
    "checkpoint_distance": f"{128 * 1024}",
    "compaction_target_size": f"{128 * 1024}",
    "compaction_threshold": "1",
    # no PITR horizon, we specify the horizon when we request on-demand GC
    "pitr_interval": "0s",
    # disable background compaction and GC. We invoke it manually when we want it to happen.
    "gc_period": "0s",
    "compaction_period": "0s",
    # create image layers eagerly, so that GC can remove some layers
    "image_creation_threshold": "1",
}


def evict_random_layers(
    rng: random.Random, pageserver: NeonPageserver, tenant_id: TenantId, timeline_id: TimelineId
):
    """
    Evict 50% of the layers on a pageserver
    """
    timeline_path = pageserver.timeline_dir(tenant_id, timeline_id)
    initial_local_layers = sorted(
        list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    )
    client = pageserver.http_client()
    for layer in initial_local_layers:
        if "ephemeral" in layer.name:
            continue

        if rng.choice([True, False]):
            log.info(f"Evicting layer {tenant_id}/{timeline_id} {layer.name}")
            client.evict_layer(tenant_id=tenant_id, timeline_id=timeline_id, layer_name=layer.name)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_location_conf_churn(neon_env_builder: NeonEnvBuilder, seed: int):
    """
    Issue many location configuration changes, ensure that tenants
    remain readable & we don't get any unexpected errors.  We should
    have no ERROR in the log, and no 500s in the API.

    The location_config API is intentionally designed so that all destination
    states are valid, so that we may test it in this way: the API should always
    work as long as the tenant exists.
    """
    neon_env_builder.enable_generations = True
    neon_env_builder.num_pageservers = 3
    neon_env_builder.enable_pageserver_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    assert env.attachment_service is not None

    pageservers = env.pageservers
    list([p.http_client() for p in pageservers])
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # We will make no effort to avoid stale attachments
    for ps in env.pageservers:
        ps.allowed_errors.extend(
            [
                ".*Dropped remote consistent LSN updates.*",
                ".*Dropping stale deletions.*",
                # page_service_conn_main{peer_addr=[::1]:41176}: query handler for 'pagestream 3b19aec5038c796f64b430b30a555121 d07776761d44050b8aab511df1657d83' failed: Tenant 3b19aec5038c796f64b430b30a555121 not found
                ".*query handler.*Tenant.*not found.*",
                # page_service_conn_main{peer_addr=[::1]:45552}: query handler for 'pagestream 414ede7ad50f775a8e7d9ba0e43b9efc a43884be16f44b3626482b6981b2c745' failed: Tenant 414ede7ad50f775a8e7d9ba0e43b9efc is not active
                ".*query handler.*Tenant.*not active.*",
            ]
        )

        # these can happen, if we shutdown at a good time. to be fixed as part of #5172.
        message = ".*duplicated L1 layer layer=.*"
        ps.allowed_errors.append(message)

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageservers[0].id)
    workload.write_rows(256, env.pageservers[0].id)

    # We use a fixed seed to make the test reproducible: we want a randomly
    # chosen order, but not to change the order every time we run the test.
    rng = random.Random(seed)

    initial_generation = 1
    last_state = {
        env.pageservers[0].id: ("AttachedSingle", initial_generation),
        env.pageservers[1].id: ("Detached", None),
        env.pageservers[2].id: ("Detached", None),
    }

    latest_attached = env.pageservers[0].id

    for _i in range(0, 64):
        # Pick a pageserver
        pageserver = rng.choice(env.pageservers)

        # Pick a pseudorandom state
        modes = [
            "AttachedSingle",
            "AttachedMulti",
            "AttachedStale",
            "Secondary",
            "Detached",
            "_Evictions",
            "_Restart",
        ]

        mode = rng.choice(modes)

        last_state_ps = last_state[pageserver.id]
        if mode == "_Evictions":
            if last_state_ps[0].startswith("Attached"):
                log.info(f"Action: evictions on pageserver {pageserver.id}")
                evict_random_layers(rng, pageserver, tenant_id, timeline_id)
            else:
                log.info(
                    f"Action: skipping evictions on pageserver {pageserver.id}, is not attached"
                )
        elif mode == "_Restart":
            log.info(f"Action: restarting pageserver {pageserver.id}")
            pageserver.stop()
            pageserver.start()
            if last_state_ps[0].startswith("Attached") and latest_attached == pageserver.id:
                log.info("Entering postgres...")
                workload.churn_rows(rng.randint(128, 256), pageserver.id)
                workload.validate(pageserver.id)
            elif last_state_ps[0].startswith("Attached"):
                # The `attachment_service` will only re-attach on startup when a pageserver was the
                # holder of the latest generation: otherwise the pageserver will revert to detached
                # state if it was running attached with a stale generation
                last_state[pageserver.id] = ("Detached", None)
        else:
            secondary_conf: Optional[Dict[str, Any]] = None
            if mode == "Secondary":
                secondary_conf = {"warm": rng.choice([True, False])}

            location_conf: Dict[str, Any] = {
                "mode": mode,
                "secondary_conf": secondary_conf,
                "tenant_conf": {},
            }

            log.info(f"Action: Configuring pageserver {pageserver.id} to {location_conf}")

            # Select a generation number
            if mode.startswith("Attached"):
                if last_state_ps[1] is not None:
                    if rng.choice([True, False]):
                        # Move between attached states, staying in the same generation
                        generation = last_state_ps[1]
                    else:
                        # Switch generations, while also jumping between attached states
                        generation = env.attachment_service.attach_hook(tenant_id, pageserver.id)
                        latest_attached = pageserver.id
                else:
                    generation = env.attachment_service.attach_hook(tenant_id, pageserver.id)
                    latest_attached = pageserver.id
            else:
                generation = None

            location_conf["generation"] = generation

            pageserver.tenant_location_configure(tenant_id, location_conf)
            last_state[pageserver.id] = (mode, generation)

            if mode.startswith("Attached"):
                # TODO: a variant of this test that runs background endpoint workloads, as well as
                # the inter-step workloads.

                workload.churn_rows(
                    rng.randint(128, 256), pageserver.id, upload=mode != "AttachedStale"
                )
                workload.validate(pageserver.id)

    # Attach all pageservers
    for ps in env.pageservers:
        location_conf = {"mode": "AttachedMulti", "secondary_conf": None, "tenant_conf": {}}
        ps.tenant_location_configure(tenant_id, location_conf)

    # Confirm that all are readable
    for ps in env.pageservers:
        workload.validate(ps.id)

    # Detach all pageservers
    for ps in env.pageservers:
        location_conf = {"mode": "Detached", "secondary_conf": None, "tenant_conf": {}}
        ps.tenant_location_configure(tenant_id, location_conf)

    # Confirm that all local disk state was removed on detach
    # TODO


def test_live_migration(neon_env_builder: NeonEnvBuilder):
    """
    Test the sequence of location states that are used in a live migration.
    """
    neon_env_builder.enable_generations = True
    neon_env_builder.num_pageservers = 2
    neon_env_builder.enable_pageserver_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    assert env.attachment_service is not None

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    pageserver_a = env.pageservers[0]
    pageserver_b = env.pageservers[1]

    initial_generation = 1

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageservers[0].id)
    workload.write_rows(256, env.pageservers[0].id)

    # Make the destination a secondary location
    pageserver_b.tenant_location_configure(
        tenant_id,
        {
            "mode": "Secondary",
            "secondary_conf": {"warm": True},
            "tenant_conf": {},
        },
    )

    workload.churn_rows(64, pageserver_a.id, upload=False)

    # Set origin attachment to stale
    log.info("Setting origin to AttachedStale")
    pageserver_a.tenant_location_configure(
        tenant_id,
        {
            "mode": "AttachedStale",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": initial_generation,
        },
        flush_ms=5000,
    )

    migrated_generation = env.attachment_service.attach_hook(tenant_id, pageserver_b.id)
    log.info(f"Acquired generation {migrated_generation} for destination pageserver")
    assert migrated_generation == initial_generation + 1

    # Writes and reads still work in AttachedStale.
    workload.validate(pageserver_a.id)

    # Ensure that secondary location's timeline directory is populated: we will then
    # do some more writes on top of that to ensure that the newly attached pageserver
    # properly makes use of the downloaded layers as well as ingesting WAL to catch up.
    pageserver_a.http_client().secondary_tenant_upload(tenant_id)
    pageserver_b.http_client().secondary_tenant_download(tenant_id)

    # Generate some more dirty writes: we expect the origin to ingest WAL in
    # in AttachedStale
    workload.churn_rows(64, pageserver_a.id, upload=False)
    workload.validate(pageserver_a.id)

    # Attach the destination
    log.info("Setting destination to AttachedMulti")
    pageserver_b.tenant_location_configure(
        tenant_id,
        {
            "mode": "AttachedMulti",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": migrated_generation,
        },
    )

    # Wait for destination LSN to catch up with origin
    origin_lsn = pageserver_a.http_client().timeline_detail(tenant_id, timeline_id)[
        "last_record_lsn"
    ]

    def caught_up():
        destination_lsn = pageserver_b.http_client().timeline_detail(tenant_id, timeline_id)[
            "last_record_lsn"
        ]
        log.info(
            f"Waiting for LSN to catch up: origin {origin_lsn} vs destination {destination_lsn}"
        )
        assert destination_lsn >= origin_lsn

    wait_until(100, 0.1, caught_up)

    # The destination should accept writes
    workload.churn_rows(64, pageserver_b.id)

    # Dual attached: both are readable.
    workload.validate(pageserver_a.id)
    workload.validate(pageserver_b.id)

    # Revert the origin to secondary
    log.info("Setting origin to Secondary")
    pageserver_a.tenant_location_configure(
        tenant_id,
        {
            "mode": "Secondary",
            "secondary_conf": {"warm": True},
            "tenant_conf": {},
        },
    )

    workload.churn_rows(64, pageserver_b.id)

    # Put the destination into final state
    pageserver_b.tenant_location_configure(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": migrated_generation,
        },
    )

    workload.churn_rows(64, pageserver_b.id)
    workload.validate(pageserver_b.id)


def list_layers(pageserver, tenant_id: TenantId, timeline_id: TimelineId) -> list[Path]:
    """
    Inspect local storage on a pageserver to discover which layer files are present.

    :return: list of relative paths to layers, from the timeline root.
    """
    timeline_path = pageserver.timeline_dir(tenant_id, timeline_id)

    def relative(p: Path) -> Path:
        return p.relative_to(timeline_path)

    return sorted(
        list(
            map(
                relative,
                filter(
                    lambda path: path.name != "metadata"
                    and "ephemeral" not in path.name
                    and "temp" not in path.name,
                    timeline_path.glob("*"),
                ),
            )
        )
    )


def test_secondary_downloads(neon_env_builder: NeonEnvBuilder):
    """
    Test the overall data flow in secondary mode:
     - Heatmap uploads from the attached location
     - Heatmap & layer downloads from the secondary location
     - Eviction of layers on the attached location results in deletion
       on the secondary location as well.
    """
    neon_env_builder.enable_generations = True
    neon_env_builder.num_pageservers = 2
    neon_env_builder.enable_pageserver_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    assert env.attachment_service is not None

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    ps_attached = env.pageservers[0]
    ps_secondary = env.pageservers[1]

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageservers[0].id)
    workload.write_rows(256, ps_attached.id)

    # Configure a secondary location
    log.info("Setting up secondary location...")
    ps_secondary.tenant_location_configure(
        tenant_id,
        {
            "mode": "Secondary",
            "secondary_conf": {"warm": True},
            "tenant_conf": {},
        },
    )
    readback_conf = ps_secondary.read_tenant_location_conf(tenant_id)
    log.info(f"Read back conf: {readback_conf}")

    # Explicit upload/download cycle
    # ==============================
    log.info("Synchronizing after initial write...")
    ps_attached.http_client().secondary_tenant_upload(tenant_id)

    ps_secondary.http_client().secondary_tenant_download(tenant_id)

    assert list_layers(ps_attached, tenant_id, timeline_id) == list_layers(
        ps_secondary, tenant_id, timeline_id
    )

    # Make changes on attached pageserver, check secondary downloads them
    # ===================================================================
    log.info("Synchronizing after subsequent write...")
    workload.churn_rows(128, ps_attached.id)

    ps_attached.http_client().secondary_tenant_upload(tenant_id)
    ps_secondary.http_client().secondary_tenant_download(tenant_id)

    assert list_layers(ps_attached, tenant_id, timeline_id) == list_layers(
        ps_secondary, tenant_id, timeline_id
    )

    # FIXME: this sleep is needed to avoid on-demand promotion of the layers we evict, while
    # walreceiver is still doing something.
    import time

    time.sleep(5)

    # Do evictions on attached pageserver, check secondary follows along
    # ==================================================================
    log.info("Evicting a layer...")
    layer_to_evict = list_layers(ps_attached, tenant_id, timeline_id)[0]
    ps_attached.http_client().evict_layer(tenant_id, timeline_id, layer_name=layer_to_evict.name)

    log.info("Synchronizing after eviction...")
    ps_attached.http_client().secondary_tenant_upload(tenant_id)
    ps_secondary.http_client().secondary_tenant_download(tenant_id)

    assert layer_to_evict not in list_layers(ps_attached, tenant_id, timeline_id)
    assert list_layers(ps_attached, tenant_id, timeline_id) == list_layers(
        ps_secondary, tenant_id, timeline_id
    )

    # Scrub the remote storage
    # ========================
    # This confirms that the scrubber isn't upset by the presence of the heatmap
    # TODO: depends on `jcsp/scrubber-index-part` branch.

    # Detach secondary and delete tenant
    # ===================================
    # This confirms that the heatmap gets cleaned up as well as other normal content.
    log.info("Detaching secondary location...")
    ps_secondary.tenant_location_configure(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
        },
    )

    log.info("Deleting tenant...")
    tenant_delete_wait_completed(ps_attached.http_client(), tenant_id, 10)

    assert_prefix_empty(
        neon_env_builder,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )
