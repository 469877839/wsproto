from __future__ import annotations

from typing import Union

import h11
import pytest

from wsproto import WSConnection
from wsproto.connection import CLIENT, SERVER
from wsproto.events import AcceptConnection, Request, TextMessage
from wsproto.extensions import Extension, ExtensionOffer, PerMessageDeflate
from wsproto.frame_protocol import RsvBits
from wsproto.handshake import client_extensions_handshake, server_extensions_handshake
from wsproto.utilities import RemoteProtocolError, generate_accept_token


def _raw_request(extensions: bytes | None = None) -> bytes:
    h11_client = h11.Connection(h11.CLIENT)
    headers = [
        (b"Host", b"localhost"),
        (b"Connection", b"Upgrade"),
        (b"Upgrade", b"websocket"),
        (b"Sec-WebSocket-Version", b"13"),
        (b"Sec-WebSocket-Key", b"dGhlIHNhbXBsZSBub25jZQ=="),
    ]
    if extensions is not None:
        headers.append((b"Sec-WebSocket-Extensions", extensions))
    return h11_client.send(h11.Request(method="GET", target="/", headers=headers))


def _response_headers(data: bytes) -> dict[bytes, bytes]:
    # Feed a fake upgrade request first so h11 accepts the 101 response.
    h11_client = h11.Connection(h11.CLIENT)
    h11_client.send(
        h11.Request(
            method="GET",
            target="/",
            headers=[
                (b"Host", b"localhost"),
                (b"Connection", b"Upgrade"),
                (b"Upgrade", b"websocket"),
                (b"Sec-WebSocket-Version", b"13"),
                (b"Sec-WebSocket-Key", b"dGhlIHNhbXBsZSBub25jZQ=="),
            ],
        ),
    )
    h11_client.receive_data(data)
    response = h11_client.next_event()
    assert isinstance(response, h11.InformationalResponse)
    return dict(response.headers)


class Rsv1Extension(Extension):
    name = "rsv1-extension"

    def offer(self) -> Union[bool, str]:
        return True

    def accept(self, offer: str) -> Union[bool, str]:
        return True

    def reserved_bits(self) -> RsvBits:
        return RsvBits(True, False, False)


class LegacyExtension(Extension):
    name = "legacy"

    def __init__(self) -> None:
        self.accepted_offer = None
        self.offered = None
        self.finalized = None

    def offer(self) -> Union[bool, str]:
        return "legacy; foo=bar"

    def accept(self, offer: str) -> Union[bool, str]:
        self.offered = offer
        return "foo=bar"

    def finalize(self, offer: str) -> None:
        self.finalized = offer


# ---------------------------------------------------------------------------
# Server: multiple proposals
# ---------------------------------------------------------------------------


def test_server_selects_first_acceptable_offer() -> None:
    extension = PerMessageDeflate()
    response = server_extensions_handshake(
        [
            "permessage-deflate; server_max_window_bits=99",
            "permessage-deflate; server_max_window_bits=10",
            "permessage-deflate; server_max_window_bits=12",
        ],
        [extension],
    )
    assert response == b"permessage-deflate; server_max_window_bits=10"
    # The failed candidates must not have left window bits behind.
    assert extension.server_max_window_bits == 10
    assert extension.client_max_window_bits == 15
    assert extension.enabled()


def test_server_skips_failed_candidates_without_state() -> None:
    extension = PerMessageDeflate()
    response = server_extensions_handshake(
        [
            "permessage-deflate; client_max_window_bits=9; server_max_window_bits=99",
            "permessage-deflate; bogus_parameter",
            "permessage-deflate; server_max_window_bits=10",
        ],
        [extension],
    )
    assert response == b"permessage-deflate; server_max_window_bits=10"
    assert extension.server_max_window_bits == 10
    assert extension.client_max_window_bits == 15


def test_server_declines_all_offers() -> None:
    extension = PerMessageDeflate()
    response = server_extensions_handshake(
        ["permessage-deflate; server_max_window_bits=99"],
        [extension],
    )
    assert response is None
    assert not extension.enabled()
    assert extension.server_max_window_bits == 15
    assert extension.client_max_window_bits == 15


def test_server_accepts_only_one_offer_per_name() -> None:
    extension = PerMessageDeflate()
    response = server_extensions_handshake(
        [
            "permessage-deflate; server_max_window_bits=10",
            "permessage-deflate; server_max_window_bits=12",
        ],
        [extension],
    )
    assert response == b"permessage-deflate; server_max_window_bits=10"


# ---------------------------------------------------------------------------
# Legacy extension compatibility path
# ---------------------------------------------------------------------------


def test_legacy_extension_server_path() -> None:
    extension = LegacyExtension()
    response = server_extensions_handshake(["legacy; foo=bar"], [extension])
    assert response == b"legacy; foo=bar"
    assert extension.offered == "legacy; foo=bar"


def test_legacy_extension_client_path() -> None:
    extension = LegacyExtension()
    result = client_extensions_handshake(["legacy; foo=bar"], [extension])
    assert result == [extension]
    assert extension.finalized == "legacy; foo=bar"


# ---------------------------------------------------------------------------
# RSV bit contention
# ---------------------------------------------------------------------------


def test_server_rejects_conflicting_rsv_bits() -> None:
    pmd = PerMessageDeflate()
    other = Rsv1Extension()
    # PMD is offered first and accepted, then the conflicting extension is
    # dropped from the response.
    response = server_extensions_handshake(
        ["permessage-deflate", "rsv1-extension"],
        [pmd, other],
    )
    assert response == b"permessage-deflate"


def test_server_rejects_conflicting_rsv_bits_first_wins() -> None:
    pmd = PerMessageDeflate()
    other = Rsv1Extension()
    response = server_extensions_handshake(
        ["rsv1-extension", "permessage-deflate"],
        [pmd, other],
    )
    assert response == b"rsv1-extension"


def test_client_rejects_conflicting_rsv_bits() -> None:
    pmd = PerMessageDeflate()
    other = Rsv1Extension()
    with pytest.raises(RemoteProtocolError):
        client_extensions_handshake(
            ["permessage-deflate", "rsv1-extension"],
            [pmd, other],
        )


# ---------------------------------------------------------------------------
# Client validation of server responses
# ---------------------------------------------------------------------------


def test_client_rejects_unoffered_client_window_bits() -> None:
    extension = PerMessageDeflate()
    offered = extension.parse_offer(
        ExtensionOffer(extension.name, "permessage-deflate; server_max_window_bits=15"),
    )
    assert offered is not None
    with pytest.raises(RemoteProtocolError):
        client_extensions_handshake(
            ["permessage-deflate; client_max_window_bits=10"],
            [extension],
            {extension.name: offered},
        )


def test_client_rejects_invalid_server_response() -> None:
    extension = PerMessageDeflate()
    with pytest.raises(RemoteProtocolError):
        client_extensions_handshake(
            ["permessage-deflate; server_max_window_bits=99"],
            [extension],
        )


def test_client_rejects_duplicate_extension_responses() -> None:
    extension = PerMessageDeflate()
    with pytest.raises(RemoteProtocolError):
        client_extensions_handshake(
            ["permessage-deflate", "permessage-deflate"],
            [extension],
        )


class NoClientWindowBitsDeflate(PerMessageDeflate):
    def offer(self) -> str:
        return "server_max_window_bits=15"


def test_wire_client_rejects_unoffered_parameters() -> None:
    # A client offer that does not include client_max_window_bits must fail
    # if the server nevertheless returns that parameter.
    config = NoClientWindowBitsDeflate()
    client = WSConnection(CLIENT)
    request_bytes = client.send(
        Request(host="localhost", target="/", extensions=[config]),
    )
    h11_server = h11.Connection(h11.SERVER)
    h11_server.receive_data(request_bytes)
    request = h11_server.next_event()
    key = dict(request.headers)[b"sec-websocket-key"]
    assert dict(request.headers)[b"sec-websocket-extensions"] == (
        b"permessage-deflate; server_max_window_bits=15"
    )
    with pytest.raises(RemoteProtocolError):
        client.receive_data(
            _client_response(
                b"permessage-deflate; client_max_window_bits=10",
                key,
            ),
        )
    assert not config.enabled()


# ---------------------------------------------------------------------------
# End-to-end: two connections from the same configuration
# ---------------------------------------------------------------------------


def _server_handshake(config: Extension, offer: bytes) -> tuple[WSConnection, dict[bytes, bytes]]:
    server = WSConnection(SERVER)
    server.receive_data(_raw_request(offer))
    next(server.events())
    response = server.send(AcceptConnection(extensions=[config]))
    return server, _response_headers(response)


def _client_response(extensions: bytes, key: bytes) -> bytes:
    h11_server = h11.Connection(h11.SERVER)
    h11_server.receive_data(_raw_request(extensions))
    assert isinstance(h11_server.next_event(), h11.Request)
    response = h11.InformationalResponse(
        status_code=101,
        headers=[
            (b"Connection", b"Upgrade"),
            (b"Upgrade", b"websocket"),
            (b"Sec-WebSocket-Accept", generate_accept_token(key)),
            (b"Sec-WebSocket-Extensions", extensions),
        ],
    )
    return h11_server.send(response) or b""


def test_failed_negotiation_does_not_break_shared_config() -> None:
    config = PerMessageDeflate()

    # First connection: the only offer is invalid for this configuration.
    server1, headers1 = _server_handshake(
        config, b"permessage-deflate; server_max_window_bits=99"
    )
    assert b"sec-websocket-extensions" not in headers1
    assert not config.enabled()
    assert config.server_max_window_bits == 15
    assert config.client_max_window_bits == 15
    assert server1.connection is not None
    assert server1.connection._proto.extensions == []

    # Second connection with the same configuration object: it must still
    # negotiate successfully and leave the failed candidate's bits behind.
    good_offer = b"permessage-deflate; client_max_window_bits=12; server_max_window_bits=10"
    server2, headers2 = _server_handshake(config, good_offer)
    assert headers2[b"sec-websocket-extensions"] == (
        b"permessage-deflate; client_max_window_bits=12; server_max_window_bits=10"
    )
    assert config.enabled()
    assert config.client_max_window_bits == 12
    assert config.server_max_window_bits == 10

    runtime = server2.connection._proto.extensions
    assert len(runtime) == 1
    assert isinstance(runtime[0], PerMessageDeflate)
    assert runtime[0] is not config
    assert runtime[0].client_max_window_bits == 12
    assert runtime[0].server_max_window_bits == 10

    # A client using the same configuration object negotiates matching
    # parameters and exchanges compressed frames with the server.
    client = WSConnection(CLIENT)
    client_offer = client.send(
        Request(host="localhost", target="/", extensions=[config]),
    )
    h11_server = h11.Connection(h11.SERVER)
    h11_server.receive_data(client_offer)
    request = h11_server.next_event()
    key = dict(request.headers)[b"sec-websocket-key"]
    assert dict(request.headers)[b"sec-websocket-extensions"] == (
        b"permessage-deflate; client_max_window_bits=12; server_max_window_bits=10"
    )

    client.receive_data(
        _client_response(
            b"permessage-deflate; client_max_window_bits=12; server_max_window_bits=10",
            key,
        ),
    )
    events = list(client.events())
    assert isinstance(events[0], AcceptConnection)
    client_extension = events[0].extensions[0]
    assert isinstance(client_extension, PerMessageDeflate)
    assert client_extension.client_max_window_bits == 12
    assert client_extension.server_max_window_bits == 10

    outbound = client.send(TextMessage(data="hello hello hello hello"))
    assert outbound[0] & 0x40  # RSV1 set: the payload is compressed
    server2.receive_data(outbound)
    assert list(server2.events()) == [
        TextMessage(data="hello hello hello hello", frame_finished=True, message_finished=True)
    ]

    inbound = server2.send(TextMessage(data="world world world world"))
    assert inbound[0] & 0x40
    client.receive_data(inbound)
    assert list(client.events()) == [
        TextMessage(data="world world world world", frame_finished=True, message_finished=True)
    ]

    # Neither endpoint's runtime extension is the shared configuration object.
    assert server2.connection._proto.extensions[0] is not config
    assert client.connection._proto.extensions[0] is not config
