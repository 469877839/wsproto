"""Contract tests for state advancement while ``events()`` is iterated.

The contract under test:

* ``receive_data()`` only buffers parsed frames; state transitions caused by
  received frames are applied when their events are *consumed* from
  :meth:`Connection.events`, never when the bytes are handed over.
* A remote close frame therefore moves the connection to
  ``REMOTE_CLOSING`` / ``CLOSED`` only while its ``CloseConnection`` event is
  yielded. Bytes already received are not the same as an application-consumed
  close.
* Everything here runs in memory: a second in-process :class:`Connection` is
  used purely to serialize frames, no sockets or I/O.
"""
from __future__ import annotations

import pytest

from wsproto.connection import CLIENT, SERVER, Connection, ConnectionState
from wsproto.events import (
    BytesMessage,
    CloseConnection,
    Event,
    Ping,
    Pong,
    TextMessage,
)
from wsproto.frame_protocol import CloseReason
from wsproto.utilities import LocalProtocolError


def _event_names(events: list[Event]) -> list[str]:
    return [type(event).__name__ for event in events]


def _send_allowed(connection: Connection, event: Event) -> bool:
    try:
        connection.send(event)
    except LocalProtocolError:
        return False
    return True


def _wire(peer: Connection, event: Event) -> bytes:
    return peer.send(event)


def test_receive_data_does_not_advance_state_without_consuming() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)

    server.receive_data(_wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))

    assert server.state is ConnectionState.OPEN

    event = next(server.events())
    assert isinstance(event, CloseConnection)
    assert server.state is ConnectionState.REMOTE_CLOSING


def test_data_frame_followed_by_close_partial_consumption() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(
        _wire(client, BytesMessage(b"hello"))
        + _wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)),
    )

    assert server.state is ConnectionState.OPEN
    first = next(server.events())
    assert isinstance(first, BytesMessage)
    # The close frame is already parsed and buffered, but its state
    # transition must not have happened yet.
    assert server.state is ConnectionState.OPEN

    # An application that closes locally before consuming the remote close
    # initiates its own handshake; state stays LOCAL_CLOSING until the remote
    # close event is consumed.
    server.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    assert server.state is ConnectionState.LOCAL_CLOSING

    remaining = list(server.events())
    assert _event_names(remaining) == ["CloseConnection"]
    assert server.state is ConnectionState.CLOSED


def test_simultaneous_close() -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)

    client_close = client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    server_close = server.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    assert client.state is ConnectionState.LOCAL_CLOSING
    assert server.state is ConnectionState.LOCAL_CLOSING

    client.receive_data(server_close)
    server.receive_data(client_close)
    # Bytes received, events not consumed: the local state is unchanged.
    assert client.state is ConnectionState.LOCAL_CLOSING
    assert server.state is ConnectionState.LOCAL_CLOSING

    client_events = list(client.events())
    server_events = list(server.events())
    assert _event_names(client_events) == ["CloseConnection"]
    assert _event_names(server_events) == ["CloseConnection"]
    assert client.state is ConnectionState.CLOSED
    assert server.state is ConnectionState.CLOSED

    for connection in (client, server):
        assert not _send_allowed(connection, BytesMessage(b"x"))
        assert not _send_allowed(connection, Ping())
        assert not _send_allowed(connection, CloseConnection(code=1000))
        with pytest.raises(LocalProtocolError):
            connection.receive_data(b"more bytes")


def test_close_then_eof_after_consuming_close() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(_wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))

    consumed = list(server.events())
    assert _event_names(consumed) == ["CloseConnection"]
    assert server.state is ConnectionState.REMOTE_CLOSING

    # The application has not replied yet when the transport signals EOF.
    server.receive_data(None)
    assert server.state is ConnectionState.CLOSED
    events = list(server.events())
    assert _event_names(events) == ["CloseConnection"]
    assert events[0].code is CloseReason.ABNORMAL_CLOSURE
    assert server.state is ConnectionState.CLOSED


def test_close_then_eof_without_consuming_close() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(_wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))
    assert server.state is ConnectionState.OPEN

    server.receive_data(None)
    assert server.state is ConnectionState.CLOSED

    events = list(server.events())
    # EOF is surfaced first; the already-buffered close frame must still be
    # delivered and must not drag the state out of CLOSED again.
    assert _event_names(events) == ["CloseConnection", "CloseConnection"]
    assert events[0].code is CloseReason.ABNORMAL_CLOSURE
    assert events[1].code is CloseReason.NORMAL_CLOSURE
    assert server.state is ConnectionState.CLOSED


def test_partial_consumption_then_eof_keeps_closed_state() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(
        _wire(client, BytesMessage(b"hello"))
        + _wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)),
    )

    generator = server.events()
    first = next(generator)
    assert isinstance(first, BytesMessage)
    assert server.state is ConnectionState.OPEN

    server.receive_data(None)
    assert server.state is ConnectionState.CLOSED
    remaining = list(generator)
    assert _event_names(remaining) == ["CloseConnection"]
    assert server.state is ConnectionState.CLOSED


def test_eof_without_close_frame() -> None:
    server = Connection(SERVER)
    server.receive_data(None)

    assert server.state is ConnectionState.CLOSED
    events = list(server.events())
    assert _event_names(events) == ["CloseConnection"]
    assert events[0].code is CloseReason.ABNORMAL_CLOSURE


def test_data_then_eof_without_close_frame() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(_wire(client, TextMessage(data="hi")))
    assert _event_names(list(server.events())) == ["TextMessage"]
    assert server.state is ConnectionState.OPEN

    server.receive_data(None)
    assert server.state is ConnectionState.CLOSED
    events = list(server.events())
    assert _event_names(events) == ["CloseConnection"]
    assert events[0].code is CloseReason.ABNORMAL_CLOSURE


def test_unconsumed_ping_keeps_connection_open() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(_wire(client, Ping(payload=b"pl")))

    assert server.state is ConnectionState.OPEN
    # A pending ping does not restrict what the application may send.
    assert _send_allowed(server, BytesMessage(b"x"))
    assert _send_allowed(server, Ping())
    assert _send_allowed(server, Pong())
    assert _send_allowed(server, CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    assert server.state is ConnectionState.LOCAL_CLOSING

    events = list(server.events())
    assert _event_names(events) == ["Ping"]
    # Control events other than close never change the connection state.
    assert server.state is ConnectionState.LOCAL_CLOSING


def test_unconsumed_events_survive_further_receive_data() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(_wire(client, Ping(payload=b"1")))

    generator = server.events()
    first = next(generator)
    assert isinstance(first, Ping)

    server.receive_data(_wire(client, BytesMessage(b"d")))
    server.receive_data(_wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))
    assert server.state is ConnectionState.OPEN

    assert _event_names(list(generator)) == ["BytesMessage", "CloseConnection"]
    assert server.state is ConnectionState.REMOTE_CLOSING
    # A second generator has nothing left and does not alter the state.
    assert list(server.events()) == []
    assert server.state is ConnectionState.REMOTE_CLOSING


def test_remote_close_unconsumed_allows_local_close_first() -> None:
    server = Connection(SERVER)
    client = Connection(CLIENT)
    server.receive_data(_wire(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))
    assert server.state is ConnectionState.OPEN

    server.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    assert server.state is ConnectionState.LOCAL_CLOSING

    events = list(server.events())
    assert _event_names(events) == ["CloseConnection"]
    assert server.state is ConnectionState.CLOSED


def test_local_closing_peer_data_before_peer_close() -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)
    client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))

    assert not _send_allowed(client, BytesMessage(b"x"))
    assert not _send_allowed(client, Ping())
    assert not _send_allowed(client, Pong())
    assert not _send_allowed(client, CloseConnection(code=CloseReason.NORMAL_CLOSURE))

    client.receive_data(_wire(server, BytesMessage(b"late1")))
    client.receive_data(_wire(server, BytesMessage(b"late2")))
    client.receive_data(_wire(server, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))
    assert client.state is ConnectionState.LOCAL_CLOSING

    events = list(client.events())
    assert _event_names(events) == [
        "BytesMessage",
        "BytesMessage",
        "CloseConnection",
    ]
    assert client.state is ConnectionState.CLOSED
    with pytest.raises(LocalProtocolError):
        client.receive_data(b"after closed")


def test_local_closing_then_eof_with_unconsumed_remote_close() -> None:
    client = Connection(CLIENT)
    server = Connection(SERVER)
    client.send(CloseConnection(code=CloseReason.NORMAL_CLOSURE))
    client.receive_data(_wire(server, CloseConnection(code=CloseReason.NORMAL_CLOSURE)))
    assert client.state is ConnectionState.LOCAL_CLOSING

    client.receive_data(None)
    assert client.state is ConnectionState.CLOSED
    events = list(client.events())
    assert _event_names(events) == ["CloseConnection", "CloseConnection"]
    assert events[0].code is CloseReason.ABNORMAL_CLOSURE
    assert events[1].code is CloseReason.NORMAL_CLOSURE
    assert client.state is ConnectionState.CLOSED
