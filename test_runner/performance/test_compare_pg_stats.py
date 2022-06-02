import pytest
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare, zenith_compare

from performance.test_perf_pgbench import (get_durations_matrix, get_scales_matrix)


@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
def test_pgbench(zenith_compare: ZenithCompare, vanilla_compare: VanillaCompare, scale: int, duration: int):
    ...
