from __future__ import annotations

from collections.abc import MutableMapping
from typing import TYPE_CHECKING, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import MutableMapping
    from typing import Any

    from _pytest.config import Config


def pytest_collection_modifyitems(config: Config, items: list[pytest.Item]):
    # pytest-rerunfailures is not compatible with pytest-timeout (timeout is not set for reruns),
    #   we can workaround it by setting `timeout_func_only` to True[1].
    # Unfortunately, setting `timeout_func_only = True` globally in pytest.ini is broken[2],
    #   but we still can do it using pytest marker.
    #
    # - [1] https://github.com/pytest-dev/pytest-rerunfailures/issues/99
    # - [2] https://github.com/pytest-dev/pytest-timeout/issues/142

    if not config.getoption("--reruns"):
        return

    for item in items:
        timeout_marker = item.get_closest_marker("timeout")
        if timeout_marker is not None:
            kwargs = cast("MutableMapping[str, Any]", timeout_marker.kwargs)
            kwargs["func_only"] = True
