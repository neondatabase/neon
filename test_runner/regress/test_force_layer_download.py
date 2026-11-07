from __future__ import annotations

import asyncio
from io import StringIO
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from scripts.force_layer_download import (
    Client,
    ClientException,
    _get_timeline_ids_or_raise,
    main_impl,
)

if TYPE_CHECKING:
    from typing import Any


@pytest.mark.parametrize("result", [[], ["timeline-1", "timeline-2"]])
def test_get_timeline_ids_or_raise_returns_lists(result: list[str]):
    assert _get_timeline_ids_or_raise("tenant-1", result) == result


@pytest.mark.parametrize(
    "result",
    [ClientException("request failed"), RuntimeError("request timed out")],
)
def test_get_timeline_ids_or_raise_adds_tenant_context(result: Exception):
    with pytest.raises(ClientException, match="tenant tenant-1") as exc_info:
        _get_timeline_ids_or_raise("tenant-1", result)

    assert exc_info.value.__cause__ is result


@pytest.mark.parametrize("result", [None, {"timeline_id": "timeline-1"}])
def test_get_timeline_ids_or_raise_rejects_invalid_response(result: Any):
    with pytest.raises(ClientException, match="expecting a list.*tenant tenant-1"):
        _get_timeline_ids_or_raise("tenant-1", result)


def test_get_timeline_ids_or_raise_preserves_cancellation():
    cancellation = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError) as exc_info:
        _get_timeline_ids_or_raise("tenant-1", cancellation)

    assert exc_info.value is cancellation


@pytest.mark.asyncio
async def test_main_impl_names_tenant_when_listing_timelines_fails():
    class FakeClient(Client):
        def __init__(self):
            pass

        async def get_tenant_ids(self) -> list[str]:
            return ["tenant-1", "tenant-2"]

        async def get_timeline_ids(self, tenant_id: str) -> list[str]:
            if tenant_id == "tenant-2":
                raise ClientException("request failed")
            return ["timeline-1"]

    args = SimpleNamespace(what=["ALL"], concurrent_tasks=1)

    with pytest.raises(ClientException, match="tenant tenant-2: request failed"):
        await main_impl(args, StringIO(), FakeClient())
