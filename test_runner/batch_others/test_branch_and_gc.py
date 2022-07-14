import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import lsn_from_hex


# Test the GC implementation when running with branching.
# This test reproduces the issue https://github.com/neondatabase/neon/issues/707.
#
# Consider two LSNs `lsn1` and `lsn2` with some delta files as follows:
# ...
# p   -> has an image layer xx_p with p < lsn1
# ...
# lsn1
# ...
# q   -> has an image layer yy_q with lsn1 < q < lsn2
# ...
# lsn2
#
# Consider running a GC iteration such that the GC horizon is between p and lsn1
# ...
# p       -> has an image layer xx_p with p < lsn1
# D_start -> is a delta layer D's start (e.g D = '...-...-D_start-D_end')
# ...
# GC_h    -> is a gc horizon such that p < GC_h < lsn1
# ...
# lsn1
# ...
# D_end   -> is a delta layer D's end
# ...
# q       -> has an image layer yy_q with lsn1 < q < lsn2
# ...
# lsn2
#
# As described in the issue #707, the image layer xx_p will be deleted as
# its range is below the GC horizon and there exists a newer image layer yy_q (q > p).
# However, removing xx_p will corrupt any delta layers that depend on xx_p that
# are not deleted by GC. For example, the delta layer D is corrupted in the
# above example because D depends on the image layer xx_p for value reconstruction.
#
# Because the delta layer D covering lsn1 is corrupted, creating a branch
# starting from lsn1 should return an error as follows:
#     could not find data for key ... at LSN ..., for request at LSN ...
# @pytest.mark.skip("")
def test_branch_and_gc(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant, _ = env.neon_cli.create_tenant(
        conf={
            # disable background GC
            'gc_period': '10 m',
            'gc_horizon': f'{10 * 1024 ** 3}',

            # small checkpoint distance to create more delta layer files
            'checkpoint_distance': f'{1024 ** 2}',

            # set the target size to be large to allow the image layer to cover the whole key space
            'compaction_target_size': f'{1024 ** 3}',

            # tweak the default settings to allow quickly create image layers and L1 layers
            'compaction_period': '10 s',
            'compaction_threshold': '2',
            'image_creation_threshold': '2',

            # set PITR interval to be small, so we can do GC
            'pitr_interval': '1 s'
        })

    timeline_main = env.neon_cli.create_timeline(f'test_main', tenant_id=tenant)
    pg_main = env.postgres.create_start('test_main', tenant_id=tenant)

    main_cur = pg_main.connect().cursor()

    main_cur.execute(
        "CREATE TABLE foo(key serial primary key, t text default 'foooooooooooooooooooooooooooooooooooooooooooooooooooo')"
    )
    main_cur.execute('INSERT INTO foo SELECT FROM generate_series(1, 100000)')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn1 = main_cur.fetchone()[0]
    log.info(f'LSN1: {lsn1}')

    # trigger a manual compaction
    env.pageserver.safe_psql(f'''compact {tenant.hex} {timeline_main.hex}''')

    main_cur.execute('INSERT INTO foo SELECT FROM generate_series(1, 100000)')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn2 = main_cur.fetchone()[0]
    log.info(f'LSN2: {lsn2}')

    # Set the GC horizon so that lsn1 is inside the horizon, which means
    # we can create a new branch starting from lsn1.
    env.pageserver.safe_psql(
        f'''do_gc {tenant.hex} {timeline_main.hex} {lsn_from_hex(lsn2) - lsn_from_hex(lsn1) + 1024}'''
    )

    env.neon_cli.create_branch('test_branch',
                               'test_main',
                               tenant_id=tenant,
                               ancestor_start_lsn=lsn1)
    pg_branch = env.postgres.create_start('test_branch', tenant_id=tenant)

    branch_cur = pg_branch.connect().cursor()
    branch_cur.execute('INSERT INTO foo SELECT FROM generate_series(1, 100000)')

    branch_cur.execute('SELECT count(*) FROM foo')
    assert branch_cur.fetchone() == (200000, )
