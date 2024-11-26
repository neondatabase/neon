"""
https://python-hyper.org/projects/hyper-h2/en/stable/asyncio-example.html

auth-broker -> local-proxy needs a h2 connection, so we need a h2 server :)
"""

from __future__ import annotations

import asyncio
import collections
import io
import json
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, final

import pytest_asyncio
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.errors import ErrorCodes
from h2.events import (
    ConnectionTerminated,
    DataReceived,
    RemoteSettingsChanged,
    RequestReceived,
    StreamEnded,
    StreamReset,
    WindowUpdated,
)
from h2.exceptions import ProtocolError, StreamClosedError
from h2.settings import SettingCodes
from typing_extensions import override

if TYPE_CHECKING:
    from typing import Any


RequestData = collections.namedtuple("RequestData", ["headers", "data"])


@final
class H2Server:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port


@final
class H2Protocol(asyncio.Protocol):
    def __init__(self):
        config = H2Configuration(client_side=False, header_encoding="utf-8")
        self.conn = H2Connection(config=config)
        self.transport: asyncio.Transport | None = None
        self.stream_data: dict[int, RequestData] = {}
        self.flow_control_futures: dict[int, asyncio.Future[Any]] = {}

    @override
    def connection_made(self, transport: asyncio.BaseTransport):
        assert isinstance(transport, asyncio.Transport)
        self.transport = transport
        self.conn.initiate_connection()
        self.transport.write(self.conn.data_to_send())

    @override
    def connection_lost(self, exc: Exception | None):
        for future in self.flow_control_futures.values():
            future.cancel()
        self.flow_control_futures = {}

    @override
    def data_received(self, data: bytes):
        assert self.transport is not None
        try:
            events = self.conn.receive_data(data)
        except ProtocolError:
            self.transport.write(self.conn.data_to_send())
            self.transport.close()
        else:
            self.transport.write(self.conn.data_to_send())
            for event in events:
                if isinstance(event, RequestReceived):
                    self.request_received(event.headers, event.stream_id)
                elif isinstance(event, DataReceived):
                    self.receive_data(event.data, event.stream_id)
                elif isinstance(event, StreamEnded):
                    self.stream_complete(event.stream_id)
                elif isinstance(event, ConnectionTerminated):
                    self.transport.close()
                elif isinstance(event, StreamReset):
                    self.stream_reset(event.stream_id)
                elif isinstance(event, WindowUpdated):
                    self.window_updated(event.stream_id, event.delta)
                elif isinstance(event, RemoteSettingsChanged):
                    if SettingCodes.INITIAL_WINDOW_SIZE in event.changed_settings:
                        self.window_updated(0, 0)

                self.transport.write(self.conn.data_to_send())

    def request_received(self, headers: list[tuple[str, str]], stream_id: int):
        headers_map = collections.OrderedDict(headers)

        # Store off the request data.
        request_data = RequestData(headers_map, io.BytesIO())
        self.stream_data[stream_id] = request_data

    def stream_complete(self, stream_id: int):
        """
        When a stream is complete, we can send our response.
        """
        try:
            request_data = self.stream_data[stream_id]
        except KeyError:
            # Just return, we probably 405'd this already
            return

        headers = request_data.headers
        body = request_data.data.getvalue().decode("utf-8")

        data = json.dumps({"headers": headers, "body": body}, indent=4).encode("utf8")

        response_headers = (
            (":status", "200"),
            ("content-type", "application/json"),
            ("content-length", str(len(data))),
        )
        self.conn.send_headers(stream_id, response_headers)
        asyncio.ensure_future(self.send_data(data, stream_id))

    def receive_data(self, data: bytes, stream_id: int):
        """
        We've received some data on a stream. If that stream is one we're
        expecting data on, save it off. Otherwise, reset the stream.
        """
        try:
            stream_data = self.stream_data[stream_id]
        except KeyError:
            self.conn.reset_stream(stream_id, error_code=ErrorCodes.PROTOCOL_ERROR)
        else:
            stream_data.data.write(data)

    def stream_reset(self, stream_id: int):
        """
        A stream reset was sent. Stop sending data.
        """
        if stream_id in self.flow_control_futures:
            future = self.flow_control_futures.pop(stream_id)
            future.cancel()

    async def send_data(self, data: bytes, stream_id: int):
        """
        Send data according to the flow control rules.
        """
        while data:
            while self.conn.local_flow_control_window(stream_id) < 1:
                try:
                    await self.wait_for_flow_control(stream_id)
                except asyncio.CancelledError:
                    return

            chunk_size = min(
                self.conn.local_flow_control_window(stream_id),
                len(data),
                self.conn.max_outbound_frame_size,
            )

            try:
                self.conn.send_data(
                    stream_id, data[:chunk_size], end_stream=(chunk_size == len(data))
                )
            except (StreamClosedError, ProtocolError):
                # The stream got closed and we didn't get told. We're done
                # here.
                break

            assert self.transport is not None
            self.transport.write(self.conn.data_to_send())
            data = data[chunk_size:]

    async def wait_for_flow_control(self, stream_id: int):
        """
        Waits for a Future that fires when the flow control window is opened.
        """
        f: asyncio.Future[None] = asyncio.Future()
        self.flow_control_futures[stream_id] = f
        await f

    def window_updated(self, stream_id: int, delta):
        """
        A window update frame was received. Unblock some number of flow control
        Futures.
        """
        if stream_id and stream_id in self.flow_control_futures:
            f = self.flow_control_futures.pop(stream_id)
            f.set_result(delta)
        elif not stream_id:
            for f in self.flow_control_futures.values():
                f.set_result(delta)

            self.flow_control_futures = {}


@pytest_asyncio.fixture(scope="function")
async def http2_echoserver() -> AsyncIterable[H2Server]:
    loop = asyncio.get_event_loop()
    serve = await loop.create_server(H2Protocol, "127.0.0.1", 0)
    (host, port) = serve.sockets[0].getsockname()

    asyncio.create_task(serve.wait_closed())

    server = H2Server(host, port)
    yield server

    serve.close()
