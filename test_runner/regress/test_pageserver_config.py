import pytest
from fixtures.neon_fixtures import NeonEnv


@pytest.mark.parametrize("what", ["default", "top_level", "nested"])
def test_unknown_config_items_handling(neon_simple_env: NeonEnv, what: str):
    env = neon_simple_env

    def edit_fn(config):
        if what == "default":
            pass
        elif what == "top_level":
            config["unknown_top_level_config_item"] = 23
        elif what == "nested":
            config["remote_storage"]["unknown_config_item"] = 23
        else:
            raise ValueError(f"Unknown what: {what}")

    env.pageserver.edit_config_toml(edit_fn)

    # in any way, unknown config items should not fail pageserver to start
    # TODO: check that we warn about unkonwn config items (we currently don't)
    # => https://github.com/neondatabase/neon/issues/11322
    # TODO: extend this test with the config validator mode once we introduce it
    # => https://github.com/neondatabase/cloud/issues/24349
    env.pageserver.restart()

