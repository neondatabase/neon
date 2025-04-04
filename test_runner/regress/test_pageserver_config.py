import pytest
from fixtures.neon_fixtures import NeonEnv


@pytest.mark.parametrize("what", ["default", "top_level", "nested"])
def test_unknown_config_items_handling(neon_simple_env: NeonEnv, what: str):
    """
    Ensure we log unknown config fields.
    There are more unit tests in the Rust code for other TOML items.
    """
    env = neon_simple_env

    def edit_fn(config) -> str | None:
        if what == "default":
            return None
        elif what == "top_level":
            config["unknown_top_level_config_item"] = 23
            return r".*unknown_top_level_config_item.*"
        elif what == "nested":
            config["remote_storage"]["unknown_config_item"] = 23
            return r".*remote_storage\.unknown_config_item.*"
        else:
            raise ValueError(f"Unknown what: {what}")

    expect_warn_re = env.pageserver.edit_config_toml(edit_fn)

    if expect_warn_re is not None:
        env.pageserver.allowed_errors.append(expect_warn_re)

    if expect_warn_re is not None:
        assert not env.pageserver.log_contains(expect_warn_re)

    # in any way, unknown config items should not fail pageserver to start
    # TODO: extend this test with the config validator mode once we introduce it
    # https://github.com/neondatabase/cloud/issues/24349
    env.pageserver.restart()

    if expect_warn_re is not None:
        assert env.pageserver.log_contains(expect_warn_re)
