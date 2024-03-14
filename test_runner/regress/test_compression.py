import os
import time
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


#
# Test branching, when a transaction is in prepared state
#
def test_compression(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env

    def calculate_layers_size(tenant, timeline):
        timeline_path = "{}/tenants/{}/timelines/{}/".format(
            env.pageserver.workdir, tenant, timeline
        )
        delta_total_size = 0
        image_total_size = 0
        for filename in os.listdir(timeline_path):
             if filename.startswith("00000") and not filename.endswith(".___temp"):
                size = os.path.getsize(timeline_path + filename)
                pos = filename.find("__")
                if pos >= 0:
                    pos = filename.find("-", pos)
                    if pos >= 0:
                        delta_total_size += size
                    else:
                        image_total_size += size
        log.info(f"Image layers size: {image_total_size}, delta layers size: {delta_total_size}")
        return image_total_size

    tenant, timeline = env.neon_cli.create_tenant(
        conf={
            # Use aggressive compaction and checkpoint settings
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            "compaction_period": "1 s",
            "compaction_threshold": "1",
            "image_layer_compression": "\"LZ4\"",
        }
    )
    endpoint = env.endpoints.create_start("main", tenant_id=tenant)
    connstr = endpoint.connstr()

    log.info(f"Start a pgbench workload on pg {connstr}")
    pg_bin.run_capture(["pgbench", "-i", f"-s50", connstr])
    pg_bin.run_capture(["pgbench", "-c10", f"-T25", "-Mprepared", connstr])

    time.sleep(5) # wait sometime to let background tasks completed at PS
    compressed_image_size = calculate_layers_size(tenant,timeline)

    tenant, timeline = env.neon_cli.create_tenant(
        conf={
            # Use aggressive compaction and checkpoint settings
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            "compaction_period": "1 s",
            "compaction_threshold": "1",
            "image_layer_compression": "\"NoCompression\"",
        }
    )
    endpoint = env.endpoints.create_start("main", tenant_id=tenant)
    connstr = endpoint.connstr()

    log.info(f"Start a pgbench workload on pg {connstr}")
    pg_bin.run_capture(["pgbench", "-i", f"-s50", connstr])
    pg_bin.run_capture(["pgbench", "-c10", f"-T25", "-Mprepared", connstr])

    time.sleep(5) # wait sometime to let background tasks completed at PS
    raw_image_size = calculate_layers_size(tenant,timeline)
    log.info(f"Compression ratio: {raw_image_size/compressed_image_size}")
