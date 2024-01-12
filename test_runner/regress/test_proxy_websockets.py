import asyncio
import ssl

import pytest
from fixtures.neon_fixtures import NeonProxy
import websockets


@pytest.mark.asyncio
async def test_websockets(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user ws_auth with password 'ws' superuser")

    user = "ws_auth"
    password = "ws"

    version = b"\x00\x03\x00\x00"
    params = {
        "user": user,
        "database": "postgres",
        "client_encoding": "UTF8",
    }

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_verify_locations(str(static_proxy.test_output_dir / "proxy.crt"))

    async with websockets.connect(
        f"wss://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
        ssl=ssl_context,
    ) as websocket:
        startup_message = bytearray(version)
        for key, value in params.items():
            startup_message.extend(key.encode("ascii"))
            startup_message.extend(b"\0")
            startup_message.extend(value.encode("ascii"))
            startup_message.extend(b"\0")
        startup_message.extend(b"\0")
        length = (4 + len(startup_message)).to_bytes(4, byteorder="big")

        await websocket.send([length, startup_message])

        startup_response = await websocket.recv()
        assert startup_response[0:1] == b"R", "should be authentication message"
        assert startup_response[1:5] == b"\x00\x00\x00\x08", "should be 8 bytes long message"
        assert startup_response[5:9] == b"\x00\x00\x00\x03", "should be cleartext"

        auth_message = password.encode("utf-8") + b"\0"
        length = (4 + len(auth_message)).to_bytes(4, byteorder="big")
        await websocket.send([b"p", length, auth_message])

        auth_response = await websocket.recv()
        assert auth_response[0:1] == b"R", "should be authentication message"
        assert auth_response[1:5] == b"\x00\x00\x00\x08", "should be 8 bytes long message"
        assert auth_response[5:9] == b"\x00\x00\x00\x00", "should be authenticated"

        query_message = "SELECT 1".encode("utf-8") + b"\0"
        length = (4 + len(query_message)).to_bytes(4, byteorder="big")
        await websocket.send([b"Q", length, query_message])

        _query_response = await websocket.recv()

        # close
        await websocket.send(b"X\x00\x00\x00\x04")
        await websocket.wait_closed()
