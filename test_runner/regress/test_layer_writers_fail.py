from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnv, NeonPageserver
from fixtures.pageserver.http import PageserverApiException


@pytest.mark.skip("See https://github.com/neondatabase/neon/issues/2703")
def test_image_layer_writer_fail_before_finish(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.create_tenant(
        conf={
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{1024 ** 2}",
            # set the target size to be large to allow the image layer to cover the whole key space
            "compaction_target_size": f"{1024 ** 3}",
            # tweak the default settings to allow quickly create image layers and L1 layers
            "compaction_period": "1 s",
            "compaction_threshold": "2",
            "image_creation_threshold": "1",
        }
    )

    pg = env.endpoints.create_start("main", tenant_id=tenant_id)
    pg.safe_psql_many(
        [
            "CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)",
            """INSERT INTO foo
        SELECT 'long string to consume some space' || g
        FROM generate_series(1, 100000) g""",
        ]
    )

    pageserver_http.configure_failpoints(("image-layer-writer-fail-before-finish", "return"))
    with pytest.raises(Exception, match="image-layer-writer-fail-before-finish"):
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    new_temp_layer_files = list(
        filter(
            lambda file: str(file).endswith(NeonPageserver.TEMP_FILE_SUFFIX),
            [path for path in env.pageserver.timeline_dir(tenant_id, timeline_id).iterdir()],
        )
    )

    assert (
        len(new_temp_layer_files) == 0
    ), "pageserver should clean its temporary new image layer files on failure"


@pytest.mark.skip("See https://github.com/neondatabase/neon/issues/2703")
def test_delta_layer_writer_fail_before_finish(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.create_tenant(
        conf={
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{1024 ** 2}",
            # set the target size to be large to allow the image layer to cover the whole key space
            "compaction_target_size": f"{1024 ** 3}",
            # tweak the default settings to allow quickly create image layers and L1 layers
            "compaction_period": "1 s",
            "compaction_threshold": "2",
            "image_creation_threshold": "1",
        }
    )

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)",
            """INSERT INTO foo
        SELECT 'long string to consume some space' || g
        FROM generate_series(1, 100000) g""",
        ]
    )

    pageserver_http.configure_failpoints(("delta-layer-writer-fail-before-finish", "return"))
    # Note: we cannot test whether the exception is exactly 'delta-layer-writer-fail-before-finish'
    # since our code does it in loop, we cannot get this exact error for our request.
    with pytest.raises(PageserverApiException):
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    new_temp_layer_files = list(
        filter(
            lambda file: str(file).endswith(NeonPageserver.TEMP_FILE_SUFFIX),
            [path for path in env.pageserver.timeline_dir(tenant_id, timeline_id).iterdir()],
        )
    )

    assert (
        len(new_temp_layer_files) == 0
    ), "pageserver should clean its temporary new delta layer files on failure"
