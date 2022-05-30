import pytest
import os
import time

from fixtures.zenith_fixtures import ZenithEnvBuilder


@pytest.mark.parametrize('iteration', list(range(100)))
def test_timeout(zenith_env_builder: ZenithEnvBuilder, test_output_dir: str, iteration: int):
    env = zenith_env_builder.init_start()

    # Sleep long enough with no output so CI step times out
    time.sleep(1000)
