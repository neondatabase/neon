import re

import pytest
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import run_only_on_default_postgres


@pytest.mark.parametrize("what", ["default", "top_level", "nested"])
@run_only_on_default_postgres(reason="does not use postgres")
def test_unknown_config_items_handling(neon_simple_env: NeonEnv, what: str):
    """
    Ensure we log unknown config fields and expose a metric for alerting.
    There are more unit tests in the Rust code for other TOML items.
    """
    env = neon_simple_env

    def edit_fn(config) -> str | None:
        if what == "default":
            return None
        elif what == "top_level":
            config["unknown_top_level_config_item"] = 23
            return r"unknown_top_level_config_item"
        elif what == "nested":
            config["remote_storage"]["unknown_config_item"] = 23
            return r"remote_storage.unknown_config_item"
        else:
            raise ValueError(f"Unknown what: {what}")

    def get_metric():
        metrics = env.pageserver.http_client().get_metrics()
        samples = metrics.query_all("pageserver_config_ignored_items")
        by_item = {sample.labels["item"]: sample.value for sample in samples}
        assert by_item[""] == 0, "must always contain the empty item with value 0"
        del by_item[""]
        return by_item

    expected_ignored_item = env.pageserver.edit_config_toml(edit_fn)

    if expected_ignored_item is not None:
        expected_ignored_item_log_line_re = r".*ignoring unknown configuration item.*" + re.escape(
            expected_ignored_item
        )
        env.pageserver.allowed_errors.append(expected_ignored_item_log_line_re)

    if expected_ignored_item is not None:
        assert not env.pageserver.log_contains(expected_ignored_item_log_line_re)
        assert get_metric() == {}

    # in any way, unknown config items should not fail pageserver to start
    # TODO: extend this test with the config validator mode once we introduce it
    # https://github.com/neondatabase/cloud/issues/24349
    env.pageserver.restart()

    if expected_ignored_item is not None:
        assert env.pageserver.log_contains(expected_ignored_item_log_line_re)
        assert get_metric() == {expected_ignored_item: 1}
