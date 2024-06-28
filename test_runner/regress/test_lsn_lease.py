# from pathlib import Path

# from fixtures.common_types import TenantId, TimelineId
# from fixtures.log_helper import log
# from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, wait_for_last_flush_lsn


# # def test_lsn_lease_size_zero_gc(neon_env_builder: NeonEnvBuilder, test_output_dir: Path):
# #     conf = {"pitr_interval": "0s"}
# #     env = neon_env_builder.init_start(initial_tenant_conf=conf)
# #     lease_res = insert_and_acquire_lease(env, env.initial_tenant, env.initial_timeline, test_output_dir)

# #     tenant, timeline = env.neon_cli.create_tenant(conf=conf)
# #     ro_branch_res = insert_and_create_ro_branch(
# #         env,
# #         tenant,
# #         timeline,
# #     )

# #     for (l, r) in zip(lease_res, ro_branch_res):


# def test_lsn_lease_api_zero_cost(neon_env_builder: NeonEnvBuilder, test_output_dir: Path):
#     env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "3600s"})

#     client = env.pageserver.http_client()
#     with env.endpoints.create_start("main") as ep:
#         initial_size = client.tenant_size(env.initial_tenant)
#         log.info(f"initial size: {initial_size}")

#         with ep.cursor() as cur:
#             cur.execute(
#                 "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
#             )
#         last_flush_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
#         res = client.timeline_lsn_lease(env.initial_tenant, env.initial_timeline, last_flush_lsn)
#         log.info(f"result from lsn_lease api: {res}")

#         with ep.cursor() as cur:
#             cur.execute(
#                 "CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
#             )
#         wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
#         size_after_lease_and_insert = client.tenant_size(env.initial_tenant)
#         log.info(f"size_after_lease_and_insert: {size_after_lease_and_insert}")

#         size_debug_file = open(test_output_dir / "size_debug.html", "w")
#         size_debug = client.tenant_size_debug(env.initial_tenant)
#         size_debug_file.write(size_debug)


# def test_lsn_lease_api_zero_cost_compare(neon_env_builder: NeonEnvBuilder, test_output_dir: Path):
#     env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "3600s"})

#     client = env.pageserver.http_client()
#     with env.endpoints.create_start("main") as ep:
#         initial_size = client.tenant_size(env.initial_tenant)
#         log.info(f"initial size: {initial_size}")

#         with ep.cursor() as cur:
#             cur.execute(
#                 "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
#             )
#         last_flush_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
#         static_branch = env.neon_cli.create_branch("static")
#         # res = client.timeline_lsn_lease(env.initial_tenant, env.initial_timeline, last_flush_lsn)
#         log.info(f"{static_branch=}")

#         with ep.cursor() as cur:
#             cur.execute(
#                 "CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
#             )
#         wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
#         size_after_lease_and_insert = client.tenant_size(env.initial_tenant)
#         log.info(f"size_after_lease_and_insert: {size_after_lease_and_insert}")

#         size_debug_file = open(test_output_dir / "size_debug.html", "w")
#         size_debug = client.tenant_size_debug(env.initial_tenant)
#         size_debug_file.write(size_debug)
