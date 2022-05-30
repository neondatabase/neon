import pytest
import os
import time

from fixtures.zenith_fixtures import ZenithEnvBuilder

def test_timeout(zenith_env_builder: ZenithEnvBuilder, test_output_dir: str):
    env = zenith_env_builder.init_start()

    # Sleep long enough with no output so CI step times out
    time.sleep(1000)
