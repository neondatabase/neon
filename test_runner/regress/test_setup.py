"""Tests for the code in test fixtures"""

from __future__ import annotations

from fixtures.neon_fixtures import NeonEnvBuilder


# Test that pageserver and safekeeper can restart quickly.
# This is a regression test, see https://github.com/neondatabase/neon/issues/2247
def test_fixture_restart(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    for _ in range(3):
        env.pageserver.stop()
        env.pageserver.start()

    for _ in range(3):
        env.safekeepers[0].stop()
        env.safekeepers[0].start()
