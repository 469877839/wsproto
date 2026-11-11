"""
Tests for the optional wsproto.h2 RFC 8441 adapter.

These tests wire two :class:`wsproto.h2.H2Connection` instances together
in memory (no real network sockets) and exercise two concurrent WebSocket
streams, including interleaved pings, fragmented messages, a stream
reset, HTTP/2 flow control and resource cleanup.
"""
from __future__ import annotations

import pytest

h2 = pytest.importorskip("h2")
import h2.config  # noqa: E402
import h2.connection  # noqa: E402
import h2.errors  # noqa: E402
import h2.events  # noqa: E402

from wsproto.connection import ConnectionState, ConnectionType  # noqa: E402
from wsproto.events import (  # noqa: E402
    AcceptConnection,
    BytesMessage,
    CloseConnection,
    Ping,
    Pong,
    RejectConnection,
    Request,
    TextMessage,
)
from wsproto.extensions import PerMessageDeflate  # noqa: E402
from wsproto.frame_protocol import CloseReason, Opcode  # noqa: E402
from wsproto.h2 import (  # noqa: E402
    ConnectionTerminated,
    H2Connection,
    StreamClosed,
    StreamEvent,
)
from wsproto.utilities import LocalProtocolError  # noqa: E402


class MemoryLink:
    """Glue two H2Connection endpoints together without real I/O."""

    def __init__(
        self,
        client: H2Connection,
        server: H2Connection,
        chunk_size: int | None = None,
    ) -> None:
        self.client = client
        self.server = server
        self.chunk_size = chunk_size

    def _pump(self, data: bytes, receiver: H2Connection) -> None:
        if self.chunk_size is None:
            receiver.receive_data(data)
            return
        for index in range(0, len(data), self.chunk_size):
            receiver.receive_data(data[index : index + self.chunk_size])

    def pump(self) -> None:
        """Drain both directions until no more bytes are pending."""
        while True:
            c2s = self.client.data_to_send()
            s2c = self.server.data_to_send()
            if not c2s and not s2c:
                break
            if c2s:
                self._pump(c2s, self.server)
            if s2c:
                self._pump(s2c, self.client)


@pytest.fixture
def link() -> MemoryLink:
    client = H2Connection(ConnectionType.CLIENT)
    server = H2Connection(ConnectionType.SERVER)
    memory_link = MemoryLink(client, server)
    memory_link.pump()  # exchange HTTP/2 prefaces + SETTINGS
    return memory_link


def open_stream(
    link: MemoryLink,
    target: str,
    *,
    extensions: list[PerMessageDeflate] | None = None,
    subprotocols: list[str] | None = None,
) -> int:
    stream_id = link.client.send_request(
        Request(
            host="example.com",
            target=target,
            extensions=extensions or [],
            subprotocols=subprotocols or [],
        ),
    )
    link.pump()
    requests = [event for event in link.server.events()]
    assert len(requests) == 1
    request = requests[0]
    assert isinstance(request, Request)
    assert request.target == target
    link.server.send(
        AcceptConnection(extensions=extensions or []),
        stream_id=stream_id,
    )
    link.pump()
    accepts = [event for event in link.client.events()]
    assert len(accepts) == 1
    assert isinstance(accepts[0], AcceptConnection)
    assert link.client.stream_states[stream_id] is ConnectionState.OPEN
    assert link.server.stream_states[stream_id] is ConnectionState.OPEN
    return stream_id


def test_extended_connect_headers() -> None:
    # Inspect the raw HEADERS the client emits for RFC 8441 compliance.
    client = H2Connection(ConnectionType.CLIENT)
    preface = client.data_to_send()
    stream_id = client.send_request(
        Request(
            host="example.com",
            target="/ws",
            extensions=[PerMessageDeflate()],
            subprotocols=["chat"],
        ),
    )
    raw = client.data_to_send()
    # Feed it through a bare h2 connection and check the header block.
    probe = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=False, header_encoding=False),
    )
    probe.initiate_connection()
    probe.receive_data(preface)
    events = probe.receive_data(raw)
    request = events[0]
    headers = dict(request.headers)
    assert headers[b":method"] == b"CONNECT"
    assert headers[b":protocol"] == b"websocket"
    assert headers[b":scheme"] == b"https"
    assert headers[b":authority"] == b"example.com"
    assert headers[b":path"] == b"/ws"
    assert headers[b"sec-websocket-version"] == b"13"
    assert headers[b"sec-websocket-protocol"] == b"chat"
    assert b"permessage-deflate" in headers[b"sec-websocket-extensions"]
    # hop-by-hop headers must not appear
    assert b"upgrade" not in headers
    assert b"connection" not in headers
    assert stream_id % 2 == 1


def drain_events(conn: H2Connection) -> list[object]:
    return list(conn.events())


def ws_events(events: list[object]) -> list[StreamEvent]:
    return [event for event in events if isinstance(event, StreamEvent)]


def test_two_concurrent_streams_with_ping_fragments_and_reset(link: MemoryLink) -> None:
    stream_a = open_stream(link, "/a")
    stream_b = open_stream(link, "/b")
    assert stream_a != stream_b
    assert set(link.client.stream_states) == {stream_a, stream_b}
    assert set(link.server.stream_states) == {stream_a, stream_b}

    # Interleave a fragmented text message on A with a binary message and a
    # ping on B.
    link.client.send(TextMessage(data="frag", message_finished=False), stream_id=stream_a)
    link.client.send(BytesMessage(data=b"\x00\x01\x02"), stream_id=stream_b)
    link.client.send(Ping(payload=b"knock"), stream_id=stream_b)
    link.client.send(TextMessage(data="ment", message_finished=True), stream_id=stream_a)
    link.pump()

    server_events = drain_events(link.server)
    by_stream = {stream_a: [], stream_b: []}
    for event in ws_events(server_events):
        by_stream[event.stream_id].append(event.event)

    message_a = by_stream[stream_a]
    assert isinstance(message_a[0], TextMessage)
    assert message_a[0].data == "frag"
    assert not message_a[0].message_finished
    assert isinstance(message_a[1], TextMessage)
    assert message_a[1].data == "ment"
    assert message_a[1].message_finished

    message_b = by_stream[stream_b]
    assert isinstance(message_b[0], BytesMessage)
    assert message_b[0].data == b"\x00\x01\x02"
    assert isinstance(message_b[1], Ping)
    assert message_b[1].payload == b"knock"

    # Application-level pong, echoed back on the same stream.
    link.server.send(Pong(payload=b"knock"), stream_id=stream_b)
    link.pump()
    client_events = drain_events(link.client)
    pong = [event for event in ws_events(client_events) if isinstance(event.event, Pong)]
    assert len(pong) == 1
    assert pong[0].stream_id == stream_b
    assert pong[0].event.payload == b"knock"

    # Reset stream B: A must keep working.
    link.client.reset_stream(stream_b)
    link.pump()
    closed_events = [event for event in drain_events(link.server) if isinstance(event, StreamClosed)]
    assert len(closed_events) == 1
    assert closed_events[0].stream_id == stream_b
    assert closed_events[0].error_code == int(h2.errors.ErrorCodes.CANCEL)
    assert stream_b not in link.server.stream_states
    assert stream_b not in link.client.stream_states

    # Stream A is fully usable after the reset.
    link.client.send(TextMessage(data="still alive"), stream_id=stream_a)
    link.pump()
    surviving = ws_events(drain_events(link.server))
    assert len(surviving) == 1
    assert surviving[0].stream_id == stream_a
    assert surviving[0].event.data == "still alive"


def test_data_frames_split_ws_frames(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/split")
    # Chunk every HTTP/2 payload into 1-byte DATA frames so WebSocket
    # frames are split at arbitrary positions.
    link.chunk_size = 1
    link.client.send(BytesMessage(data=b"split me up across frames"), stream_id=stream_id)
    link.pump()
    events = ws_events(drain_events(link.server))
    assert len(events) == 1
    assert events[0].stream_id == stream_id
    assert events[0].event.data == b"split me up across frames"


def test_websocket_close_handshake_ends_stream(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/close")
    link.client.send(
        CloseConnection(code=CloseReason.NORMAL_CLOSURE, reason="bye"),
        stream_id=stream_id,
    )
    link.pump()
    close_events = [
        event for event in ws_events(drain_events(link.server))
        if isinstance(event.event, CloseConnection)
    ]
    assert len(close_events) == 1
    assert close_events[0].event.code == CloseReason.NORMAL_CLOSURE
    assert close_events[0].event.reason == "bye"
    # Respond with the mirrored close frame; both sides reach CLOSED and
    # the HTTP/2 stream is fully released.
    link.server.send(close_events[0].event.response(), stream_id=stream_id)
    link.pump()
    response = [
        event for event in ws_events(drain_events(link.client))
        if isinstance(event.event, CloseConnection)
    ]
    assert len(response) == 1
    assert stream_id not in link.client.stream_states
    assert stream_id not in link.server.stream_states
    # No StreamClosed/ConnectionTerminated events for an orderly close.
    assert not [
        event for event in drain_events(link.client)
        if isinstance(event, (StreamClosed, ConnectionTerminated))
    ]


def test_half_close_without_ws_close_is_abnormal(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/half")
    # Tear down the HTTP/2 stream directly, bypassing the WebSocket close.
    link.client._h2.end_stream(stream_id)  # noqa: SLF001
    link.pump()
    events = drain_events(link.server)
    close = [event for event in ws_events(events) if isinstance(event.event, CloseConnection)]
    assert len(close) == 1
    assert close[0].event.code == CloseReason.ABNORMAL_CLOSURE


def test_reject_handshake(link: MemoryLink) -> None:
    stream_id = link.client.send_request(Request(host="example.com", target="/nope"))
    link.pump()
    request = drain_events(link.server)[0]
    assert isinstance(request, Request)
    link.server.send(RejectConnection(status_code=403), stream_id=stream_id)
    link.pump()
    events = drain_events(link.client)
    assert isinstance(events[0], RejectConnection)
    assert events[0].status_code == 403
    assert stream_id not in link.client.stream_states
    assert stream_id not in link.server.stream_states


def test_bad_extended_connect_is_rejected() -> None:
    # A raw client omitting Sec-WebSocket-Version must be answered
    # with a non-200 status, per RFC 8441 section 5.
    server = H2Connection(ConnectionType.SERVER)
    raw = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=True, header_encoding=False),
    )
    raw.initiate_connection()
    server.receive_data(raw.data_to_send())
    raw.receive_data(server.data_to_send())
    raw.send_headers(
        1,
        [
            (b":method", b"CONNECT"),
            (b":protocol", b"websocket"),
            (b":scheme", b"https"),
            (b":authority", b"example.com"),
            (b":path", b"/bad"),
        ],
    )
    server.receive_data(raw.data_to_send())
    assert drain_events(server) == []
    responses = raw.receive_data(server.data_to_send())
    response_headers = [
        event for event in responses if isinstance(event, h2.events.ResponseReceived)
    ]
    assert len(response_headers) == 1
    assert response_headers[0].headers[0] == (b":status", b"400")


def test_flow_control_blocks_until_window_returns(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/flow")
    # Default connection window is 65535 bytes; send more than that.
    payload = b"x" * 70000
    link.client.send(BytesMessage(data=payload), stream_id=stream_id)
    link.pump()
    server_events = ws_events(drain_events(link.server))
    # The client was blocked by the HTTP/2 window: bytes remain queued
    # and the server has not seen the complete message yet.
    assert len(link.client._streams[stream_id].send_buffer) > 0  # noqa: SLF001
    assert not any(event.event.message_finished for event in server_events)

    # Consuming everything returns flow control credit and unblocks the
    # rest of the message.
    link.pump()
    server_events += ws_events(drain_events(link.server))
    reassembled = b"".join(
        event.event.data for event in server_events
    )
    assert reassembled == payload
    assert link.client._streams[stream_id].send_buffer == bytearray()  # noqa: SLF001
    # Window updates must have made it back to the client.
    assert link.client._h2.local_flow_control_window(stream_id) > 0  # noqa: SLF001


def test_receive_credit_returned_only_after_consume(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/credit")
    link.client.send(BytesMessage(data=b"abcdef"), stream_id=stream_id)
    link.pump()
    stream = link.server._streams[stream_id]  # noqa: SLF001
    # Data fully parsed into a WebSocket event means wsproto consumed it,
    # so all flow control credit must have been acknowledged to h2.
    window_manager = link.server._h2._inbound_flow_control_window_manager  # noqa: SLF001
    processed_before = window_manager._bytes_processed  # noqa: SLF001
    drain_events(link.server)
    assert stream.pending_ack == 0
    assert window_manager._bytes_processed > processed_before  # noqa: SLF001

    # A partial WebSocket frame stays buffered: no credit is given back
    # for the unconsumed bytes.
    # Build a properly masked client->server frame, then deliver it in
    # two DATA chunks so it stays partially buffered.
    raw_frame = bytes(
        link.client._streams[stream_id].ws._proto._serialize_frame(  # noqa: SLF001
            Opcode.BINARY,
            b"hold",
        ),
    )
    link.client._h2.send_data(stream_id, raw_frame[:3])  # noqa: SLF001
    link.server.receive_data(link.client.data_to_send())
    held = link.server._streams[stream_id]  # noqa: SLF001
    assert held.buffered_inbound() == 3
    assert held.pending_ack == 3
    processed_while_held = window_manager._bytes_processed  # noqa: SLF001
    link.server._return_flow_control_credit()  # noqa: SLF001
    assert held.pending_ack == 3  # still fully unconsumed
    assert window_manager._bytes_processed == processed_while_held  # noqa: SLF001

    # Deliver the rest of the frame: wsproto can now consume everything
    # and the credit is handed back.
    link.client._h2.send_data(stream_id, raw_frame[3:])  # noqa: SLF001
    link.server.receive_data(link.client.data_to_send())
    drain_events(link.server)
    assert held.pending_ack == 0
    assert link.server.data_to_send() == b""  # h2 coalesces below threshold


def test_send_queue_is_bounded() -> None:
    bounded = H2Connection(
        ConnectionType.CLIENT, max_pending_bytes=100,
    )
    server = H2Connection(ConnectionType.SERVER)
    bounded_link = MemoryLink(bounded, server)
    bounded_link.pump()
    stream_id = bounded.send_request(Request(host="example.com", target="/b"))
    bounded_link.pump()
    request = drain_events(server)[0]
    assert isinstance(request, Request)
    server.send(AcceptConnection(), stream_id=stream_id)
    bounded_link.pump()
    drain_events(bounded)
    with pytest.raises(LocalProtocolError, match="Pending outbound data"):
        bounded.send(BytesMessage(data=b"q" * 200), stream_id=stream_id)


def test_inbound_buffer_cap_resets_flooding_stream(link: MemoryLink) -> None:
    server = H2Connection(
        ConnectionType.SERVER, max_buffered_bytes=16,
    )
    client = H2Connection(ConnectionType.CLIENT)
    memory_link = MemoryLink(client, server)
    memory_link.pump()
    stream_id = client.send_request(Request(host="example.com", target="/flood"))
    memory_link.pump()
    request = drain_events(server)[0]
    server.send(AcceptConnection(), stream_id=stream_id)
    memory_link.pump()
    drain_events(client)

    # A single WebSocket frame larger than the buffer cap, delivered in
    # DATA chunks, must trigger an ENHANCE_YOUR_CALM reset of that stream.
    raw_frame = bytes(
        client._streams[stream_id].ws._proto._serialize_frame(  # noqa: SLF001
            Opcode.BINARY,
            b"f" * 128,
        ),
    )
    client._h2.send_data(stream_id, raw_frame)  # noqa: SLF001
    server.receive_data(client.data_to_send())
    resets = [
        event
        for event in client._h2.receive_data(server.data_to_send())  # noqa: SLF001
        if isinstance(event, h2.events.StreamReset)
    ]
    assert len(resets) == 1
    assert resets[0].error_code == h2.errors.ErrorCodes.ENHANCE_YOUR_CALM
    assert stream_id not in server.stream_states


def test_goaway_fails_open_streams_and_clears_resources(link: MemoryLink) -> None:
    stream_a = open_stream(link, "/ga1")
    stream_b = open_stream(link, "/ga2")
    link.client.close_connection()
    link.pump()
    events = drain_events(link.server)
    closed = [event for event in events if isinstance(event, StreamClosed)]
    terminated = [event for event in events if isinstance(event, ConnectionTerminated)]
    assert {event.stream_id for event in closed} == {stream_a, stream_b}
    assert len(terminated) == 1
    assert link.server.terminated
    assert link.server._streams == {}  # noqa: SLF001
    # Sending after termination is rejected locally.
    with pytest.raises(LocalProtocolError):
        link.server.send(TextMessage(data="x"), stream_id=stream_a)


def test_permessage_deflate_negotiated_over_h2(link: MemoryLink) -> None:
    extension = PerMessageDeflate()
    stream_id = open_stream(
        link, "/deflate", extensions=[extension],
    )
    assert link.client._streams[stream_id].ws._proto.extensions  # noqa: SLF001
    assert link.server._streams[stream_id].ws._proto.extensions  # noqa: SLF001
    link.client.send(TextMessage(data="deflate me " * 16), stream_id=stream_id)
    link.pump()
    events = ws_events(drain_events(link.server))
    assert len(events) == 1
    assert events[0].event.data == "deflate me " * 16


def test_reset_twice_is_rejected(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/once")
    link.client.reset_stream(stream_id)
    link.pump()
    drain_events(link.server)
    with pytest.raises(LocalProtocolError):
        link.client.reset_stream(stream_id)


def test_events_are_stream_tagged(link: MemoryLink) -> None:
    stream_a = open_stream(link, "/tag1")
    stream_b = open_stream(link, "/tag2")
    link.client.send(TextMessage(data="one"), stream_id=stream_a)
    link.client.send(TextMessage(data="two"), stream_id=stream_b)
    link.pump()
    events = ws_events(drain_events(link.server))
    assert {(event.stream_id, event.event.data) for event in events} == {
        (stream_a, "one"),
        (stream_b, "two"),
    }


def test_reset_discards_buffered_outbound(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/discard")
    link.client.send(TextMessage(data="pending bytes"), stream_id=stream_id)
    link.client.reset_stream(stream_id)
    link.pump()
    closed = [event for event in drain_events(link.server) if isinstance(event, StreamClosed)]
    assert len(closed) == 1
    assert closed[0].stream_id == stream_id
    assert stream_id not in link.client._streams  # noqa: SLF001
    assert stream_id not in link.server._streams  # noqa: SLF001


def test_ws_protocol_error_is_scoped_to_one_stream(link: MemoryLink) -> None:
    stream_a = open_stream(link, "/ok")
    stream_b = open_stream(link, "/bad")
    # Inject an invalid WebSocket frame directly at the HTTP/2 layer.
    link.client._h2.send_data(stream_b, b"\xff\xff garbage")  # noqa: SLF001
    link.pump()
    bad = [
        event
        for event in ws_events(drain_events(link.server))
        if event.stream_id == stream_b
    ]
    assert len(bad) == 1
    assert isinstance(bad[0].event, CloseConnection)
    assert bad[0].event.code == CloseReason.PROTOCOL_ERROR
    # The HTTP/2 connection and the other stream are untouched.
    assert not link.server.terminated
    assert not link.client.terminated
    link.client.send(TextMessage(data="fine"), stream_id=stream_a)
    link.pump()
    good = [
        event
        for event in ws_events(drain_events(link.server))
        if event.stream_id == stream_a
    ]
    assert len(good) == 1
    assert good[0].event.data == "fine"


def test_request_forbidden_header_rejected_locally(link: MemoryLink) -> None:
    with pytest.raises(LocalProtocolError):
        link.client.send_request(
            Request(
                host="example.com",
                target="/h",
                extra_headers=[(b"Upgrade", b"websocket")],
            ),
        )


def test_server_cannot_open_stream(link: MemoryLink) -> None:
    with pytest.raises(LocalProtocolError):
        link.server.send_request(Request(host="example.com", target="/x"))


def test_goaway_emits_once(link: MemoryLink) -> None:
    open_stream(link, "/g1")
    open_stream(link, "/g2")
    link.client.close_connection()
    link.pump()
    first = drain_events(link.server)
    assert any(isinstance(event, ConnectionTerminated) for event in first)
    assert link.server.terminated
    # ConnectionTerminated is delivered exactly once.
    assert not any(
        isinstance(event, ConnectionTerminated) for event in drain_events(link.server)
    )


def test_flow_control_credit_incremental_across_chunks(link: MemoryLink) -> None:
    stream_id = open_stream(link, "/incremental")
    frame = bytes(
        link.client._streams[stream_id].ws._proto._serialize_frame(  # noqa: SLF001
            Opcode.BINARY,
            b"0123456789",
        ),
    )
    third = len(frame) // 3
    stream = link.server._streams[stream_id]  # noqa: SLF001
    # Deliver the frame in three DATA chunks; the first two leave wsproto
    # holding partial data, so their credit must stay outstanding.
    link.client._h2.send_data(stream_id, frame[:third])  # noqa: SLF001
    link.server.receive_data(link.client.data_to_send())
    first = stream.pending_ack
    assert first == third
    link.server.events()
    assert stream.pending_ack == first  # nothing consumed yet

    link.client._h2.send_data(stream_id, frame[third : 2 * third])  # noqa: SLF001
    link.server.receive_data(link.client.data_to_send())
    link.server.events()
    assert stream.pending_ack == 2 * third

    # The final chunk completes the WebSocket frame: all credit returns.
    link.client._h2.send_data(stream_id, frame[2 * third :])  # noqa: SLF001
    link.server.receive_data(link.client.data_to_send())
    messages = [
        event
        for event in link.server.events()
        if isinstance(event, StreamEvent)
    ]
    assert len(messages) == 1
    assert messages[0].event.data == b"0123456789"
    assert stream.pending_ack == 0


@pytest.mark.parametrize("seed", [1, 7, 42, 1337])
def test_random_chunked_delivery(seed: int) -> None:
    import random

    rng = random.Random(seed)
    client = H2Connection(ConnectionType.CLIENT)
    server = H2Connection(ConnectionType.SERVER)
    link = MemoryLink(client, server)
    link.pump()
    stream_id = link.client.send_request(Request(host="example.com", target="/r"))
    link.pump()
    request = drain_events(server)[0]
    server.send(AcceptConnection(), stream_id=stream_id)
    link.pump()
    drain_events(client)

    payload = bytes(rng.randrange(256) for _ in range(2000))
    client.send(BytesMessage(data=payload), stream_id=stream_id)
    # Deliver the HTTP/2 byte stream in random-sized chunks.
    wire = client.data_to_send()
    while wire:
        step = rng.randrange(1, 37)
        server.receive_data(wire[:step])
        wire = wire[step:]
    events = ws_events(drain_events(server))
    assert len(events) == 1
    assert events[0].event.data == payload
