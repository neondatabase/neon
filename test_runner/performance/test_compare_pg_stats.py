import pytest
from fixtures.compare_fixtures import VanillaCompare, NeonCompare

from performance.test_perf_pgbench import (get_durations_matrix, get_scales_matrix)


@pytest.mark.parametrize("scale", get_scales_matrix())
def test_compare_pg_stats_with_pgbench(neon_compare: NeonCompare, vanilla_compare: VanillaCompare, scale: int):
    ...
