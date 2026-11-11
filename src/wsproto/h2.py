"""
wsproto/h2
~~~~~~~~~~

An optional Sans-I/O adapter that carries WebSocket connections over
HTTP/2 extended CONNECT streams, as defined by RFC 8441.

This module wraps a single :class:`h2.connection.H2Connection` (from the
``hyper-h2`` package) and multiplexes any number of WebSocket streams over
it. It performs no network I/O itself: feed it bytes received from the
network with :meth:`H2Connection.receive_data`, collect events with
:meth:`H2Connection.events`, and write :meth:`H2Connection.data_to_send`
to the network.

The WebSocket framing, message reassembly and extension handling are
provided by :class:`wsproto.connection.Connection`, so a WebSocket stream
behaves exactly like an HTTP/1.1 upgraded connection once the handshake
completes. HTTP/2 DATA frames may split WebSocket frames at arbitrary
byte boundaries; wsproto reassembles them transparently.

This module is only importable if the ``h2`` package is installed (the
``wsproto[h2]`` extra). Plain HTTP/1.1 usage of wsproto does not require
it.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from .connection import Connection, ConnectionState, ConnectionType
from .events import (
    AcceptConnection,
    Event,
    RejectConnection,
    Request,
)
from .handshake import (
    WEBSOCKET_VERSION,
    client_extensions_handshake,
    server_extensions_handshake,
)
from .utilities import (
    LocalProtocolError,
    RemoteProtocolError,
    split_comma_header,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from .extensions import Extension
    from .typing import Headers

try:
    import h2.config
    import h2.connection
    import h2.errors
    import h2.events
    import h2.exceptions
    import h2.settings
except ImportError as exc:  # pragma: no cover
    msg = (
        "wsproto.h2 requires the 'h2' package; "
        "install wsproto with the 'h2' extra (pip install wsproto[h2])"
    )
    raise ImportError(msg) from exc

__all__ = (
    "ConnectionTerminated",
    "H2Connection",
    "StreamClosed",
    "StreamEvent",
)

# RFC 8441, Section 4 - The value of the :protocol pseudo-header.
WEBSOCKET_PROTOCOL = b"websocket"

# RFC 9113, Section 8.2.2 - CONNECT requests MUST NOT send these headers.
_FORBIDDEN_REQUEST_HEADERS = frozenset(
    [
        b"connection",
        b"keep-alive",
        b"proxy-connection",
        b"transfer-encoding",
        b"upgrade",
    ]
)

#: Default cap, in bytes, on WebSocket data buffered per stream while the
#: HTTP/2 flow control window is exhausted.
DEFAULT_MAX_PENDING_BYTES = 2**20

#: Default cap, in bytes, on inbound WebSocket data per stream that has
#: not yet been consumed by wsproto. Inbound HTTP/2 flow control credit
#: is only returned to the peer once the corresponding data is consumed.
DEFAULT_MAX_BUFFERED_BYTES = 2**20


@dataclass(frozen=True)
class StreamClosed(Event):
    """
    A WebSocket stream ended without completing the WebSocket closing
    handshake.

    This is fired when the HTTP/2 stream carrying a WebSocket connection
    is ended (END_STREAM) or reset (RST_STREAM) by the peer, or when the
    whole HTTP/2 connection is torn down (GOAWAY or a connection error),
    while the WebSocket connection on that stream was still open. Other
    streams on the same HTTP/2 connection are unaffected and keep working.

    Fields:

    .. attribute:: stream_id

       (Required) The HTTP/2 stream identifier.

    .. attribute:: error_code

       The HTTP/2 error code if the stream was reset, ``None`` if the
       stream was ended cleanly at the HTTP/2 layer.
    """

    stream_id: int
    error_code: int | None = None


@dataclass(frozen=True)
class StreamEvent(Event):
    """
    A WebSocket event tagged with the HTTP/2 stream it belongs to.

    The multiplexing layer wraps every WebSocket-level event (messages,
    pings, pongs and close frames) in a :class:`StreamEvent` so the
    application can correlate activity on the concurrently open streams.

    Fields:

    .. attribute:: stream_id

       (Required) The HTTP/2 stream identifier.

    .. attribute:: event

       (Required) The wrapped wsproto :class:`~wsproto.events.Event`.
    """

    stream_id: int
    event: Event


@dataclass(frozen=True)
class ConnectionTerminated(Event):
    """
    The whole HTTP/2 connection was terminated.

    This is fired when the peer sends a GOAWAY frame, when the HTTP/2
    connection fails, or after :meth:`H2Connection.close_connection` was
    called. All WebSocket streams that were still open have been failed
    with :class:`StreamClosed` events (emitted before this event); no
    further events are generated and no further data may be sent.

    Fields:

    .. attribute:: error_code

       The HTTP/2 error code cited by the peer, if any.

    .. attribute:: last_stream_id

       The last stream the peer might have processed, as advertised in
       the GOAWAY frame. Streams with a higher identifier were never
       processed by the peer.
    """

    error_code: int | None = None
    last_stream_id: int | None = None


class _StreamStatus(Enum):
    """The HTTP/2-level lifecycle of one WebSocket stream."""

    CONNECTING = 0
    OPEN = 1
    REMOTE_HALF_CLOSED = 2
    LOCAL_HALF_CLOSED = 3
    CLOSED = 4


class _H2Stream:
    """
    A single WebSocket stream multiplexed over an HTTP/2 connection.

    Holds the wsproto :class:`~wsproto.connection.Connection` once the
    handshake has completed, the outbound bytes that could not yet be
    sent because the flow control window was exhausted, and the
    bookkeeping needed to hand inbound flow control credit back to the
    peer only after wsproto has consumed the data.
    """

    def __init__(
        self,
        stream_id: int,
        max_pending_bytes: int,
        max_buffered_bytes: int,
    ) -> None:
        self.stream_id = stream_id
        self.status = _StreamStatus.CONNECTING
        #: wsproto connection once the extended CONNECT handshake completes.
        self.ws: Connection | None = None
        #: Bytes produced by wsproto that wait for HTTP/2 send window.
        self.send_buffer = bytearray()
        #: Cap on ``send_buffer``.
        self.max_pending_bytes = max_pending_bytes
        #: Cap on wsproto's inbound buffering for this stream.
        self.max_buffered_bytes = max_buffered_bytes
        #: flow_controlled_length of inbound DATA not yet handed back via
        #: ``acknowledge_received_data``.
        self.pending_ack = 0
        #: Extension instances offered (client) or accepted (server),
        #: retained until the handshake completes.
        self.offered_extensions: list[Extension] = []
        #: Raw Sec-WebSocket-Extensions offers received with the request.
        self.request_extensions: list[str] = []
        self.subprotocol: str | None = None
        #: Set once the stream has been failed or released.
        self.failed = False
        #: Whether an END_STREAM frame has been emitted on this stream.
        self.end_stream_sent = False
        #: Pending events queued up for the application.
        self.events: deque[Event] = deque()

    @property
    def closed(self) -> bool:
        return self.status is _StreamStatus.CLOSED

    def buffered_inbound(self) -> int:
        if self.ws is None:
            return 0
        return len(self.ws._proto._frame_decoder.buffer)  # noqa: SLF001


class H2Connection:
    """
    A Sans-I/O HTTP/2 connection carrying one or more WebSocket streams.

    This object owns one :class:`h2.connection.H2Connection` and a
    :class:`_H2Stream` for every active HTTP/2 stream. It translates
    between wsproto events and HTTP/2 frames, implementing the extended
    CONNECT handshake from RFC 8441 and reusing wsproto's WebSocket
    frame and extension machinery for each stream.

    Typical client usage::

        conn = H2Connection(ConnectionType.CLIENT)
        network.write(conn.data_to_send())  # preface + SETTINGS
        stream_id = conn.send_request(Request(host="example.com",
                                              target="/chat"))
        network.write(conn.data_to_send())

        conn.receive_data(network.read())
        for event in conn.events():
            ...

    Typical server usage mirrors the client: instantiate with
    :data:`~wsproto.connection.ConnectionType.SERVER`, feed received
    bytes, and respond to :class:`~wsproto.events.Request` events with
    :class:`~wsproto.events.AcceptConnection` or
    :class:`~wsproto.events.RejectConnection`.

    Outbound data is subject to the HTTP/2 flow control windows and the
    peer's maximum frame size. Bytes that do not fit are buffered per
    stream (up to ``max_pending_bytes``) and flushed when the peer
    returns window. Inbound flow control credit is only returned to the
    peer after wsproto has consumed the corresponding data, and inbound
    buffering is bounded by ``max_buffered_bytes`` per stream.
    """

    def __init__(
        self,
        connection_type: ConnectionType,
        *,
        max_pending_bytes: int = DEFAULT_MAX_PENDING_BYTES,
        max_buffered_bytes: int = DEFAULT_MAX_BUFFERED_BYTES,
    ) -> None:
        """
        :param connection_type: Whether this end acts as client or server.
        :param max_pending_bytes: Per-stream cap on outbound WebSocket bytes
            queued while the HTTP/2 send window is exhausted. Sending beyond
            this raises :class:`~wsproto.utilities.LocalProtocolError`.
        :param max_buffered_bytes: Per-stream cap on inbound WebSocket data
            not yet consumed by wsproto. Receiving beyond this resets the
            stream with ENHANCE_YOUR_CALM.
        """
        self.client = connection_type is ConnectionType.CLIENT
        self._max_pending_bytes = max_pending_bytes
        self._max_buffered_bytes = max_buffered_bytes
        self._h2 = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=self.client,
                header_encoding=False,
            ),
        )
        self._h2.initiate_connection()
        # RFC 8441 section 3: advertise support for extended CONNECT.
        self._h2.update_settings(
            {h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL: 1},
        )
        self._streams: dict[int, _H2Stream] = {}
        self._events: deque[Event] = deque()
        self._terminated: ConnectionTerminated | None = None
        self._terminated_emitted = False

    @property
    def terminated(self) -> bool:
        """Whether the HTTP/2 connection has been torn down."""
        return self._terminated is not None

    @property
    def stream_states(self) -> dict[int, ConnectionState]:
        """
        The currently active WebSocket streams and their states.

        Maps each HTTP/2 stream identifier to the
        :class:`~wsproto.connection.ConnectionState` of the WebSocket
        connection on that stream.
        """
        states = {}
        for stream_id, stream in self._streams.items():
            if stream.ws is None:
                states[stream_id] = ConnectionState.CONNECTING
            else:
                states[stream_id] = stream.ws.state
        return states

    # -- Sending ---------------------------------------------------------

    def send(self, event: Event, stream_id: int | None = None) -> None:
        """
        Queue a wsproto event for transmission.

        :class:`~wsproto.events.Request` (client only) opens a new
        extended CONNECT stream; the allocated HTTP/2 stream identifier
        is available from :meth:`send_request`. Handshake responses
        (:class:`~wsproto.events.AcceptConnection`,
        :class:`~wsproto.events.RejectConnection`) and WebSocket events
        (:class:`~wsproto.events.Message`, :class:`~wsproto.events.Ping`,
        :class:`~wsproto.events.Pong`,
        :class:`~wsproto.events.CloseConnection`) require the
        ``stream_id`` of the stream they apply to.

        WebSocket frames become HTTP/2 DATA frames honouring the peer's
        flow control window and maximum frame size; excess is buffered
        per stream and sent once the window reopens.
        """
        if self.terminated:
            msg = "The HTTP/2 connection is terminated."
            raise LocalProtocolError(msg)
        if isinstance(event, Request):
            self._send_request(event)
        elif isinstance(event, AcceptConnection):
            self._send_accept(self._get_stream(stream_id), event)
        elif isinstance(event, RejectConnection):
            self._send_reject(self._get_stream(stream_id), event)
        else:
            self._send_ws_event(self._get_stream(stream_id), event)

    def send_request(self, request: Request) -> int:
        """
        Open a new WebSocket stream as a client.

        :returns: The allocated HTTP/2 stream identifier, which is needed
            to correlate later events.
        """
        return self._send_request(request)

    def reset_stream(self, stream_id: int, error_code: int = 0x8) -> None:
        """
        Abruptly terminate a single WebSocket stream (CANCEL by default).

        Sends an HTTP/2 RST_STREAM frame. The peer receives a
        :class:`StreamClosed` event; other streams are unaffected and
        any data still queued for the stream is discarded.
        """
        stream = self._get_stream(stream_id)
        if stream.end_stream_sent or stream.failed:
            msg = f"Stream {stream_id} is already closed."
            raise LocalProtocolError(msg)
        self._h2.reset_stream(stream_id, error_code=error_code)
        self._release_stream(stream, error_code=error_code)

    def close_connection(self, error_code: int = 0x0) -> None:
        """
        Terminate the whole HTTP/2 connection by sending a GOAWAY frame.

        Every open WebSocket stream is failed; subsequent calls to
        :meth:`events` yield a :class:`StreamClosed` event per open
        stream followed by a :class:`ConnectionTerminated` event.
        """
        if self.terminated:
            msg = "The HTTP/2 connection is already terminated."
            raise LocalProtocolError(msg)
        self._h2.close_connection(error_code=error_code)
        self._fail_all_streams(error_code=error_code)

    def data_to_send(self, amount: int | None = None) -> bytes:
        """
        Return bytes to write to the network.

        This includes the HTTP/2 preface and SETTINGS, queued
        HEADERS/DATA frames, and WINDOW_UPDATE frames generated when
        inbound data was consumed. Like ``h2``'s
        :meth:`~h2.connection.H2Connection.data_to_send`, this never
        blocks.
        """
        self._flush_outbound()
        return self._h2.data_to_send(amount)

    # -- Handshake: sending ---------------------------------------------

    def _send_request(self, request: Request) -> int:
        if not self.client:
            msg = "Cannot initiate an extended CONNECT request as a server"
            raise LocalProtocolError(msg)
        stream_id = self._h2.get_next_available_stream_id()
        stream = _H2Stream(
            stream_id, self._max_pending_bytes, self._max_buffered_bytes,
        )
        self._streams[stream_id] = stream

        headers: Headers = [
            (b":method", b"CONNECT"),
            (b":protocol", WEBSOCKET_PROTOCOL),
            (b":scheme", b"https"),
            (b":authority", request.host.encode("idna")),
            (b":path", request.target.encode("ascii")),
            (b"sec-websocket-version", WEBSOCKET_VERSION),
        ]
        extension_headers: list[bytes] = []
        for offered in request.extensions:
            if isinstance(offered, str):
                extension_headers.append(offered.encode("ascii"))
                continue
            stream.offered_extensions.append(offered)
            params = offered.offer()
            name = offered.name.encode("ascii")
            if isinstance(params, bool):
                if params:
                    extension_headers.append(name)
            else:
                extension_headers.append(
                    b"%s; %s" % (name, params.encode("ascii")),
                )
        if extension_headers:
            headers.append(
                (b"sec-websocket-extensions", b", ".join(extension_headers)),
            )
        if request.subprotocols:
            headers.append(
                (b"sec-websocket-protocol",
                 ", ".join(request.subprotocols).encode("ascii")),
            )
        for name, value in request.extra_headers:
            if name.lower() in _FORBIDDEN_REQUEST_HEADERS:
                msg = f"Header {name!r} is forbidden on extended CONNECT requests"
                raise LocalProtocolError(msg)
            headers.append((name, value))

        self._h2.send_headers(stream_id, headers)
        return stream_id

    def _send_accept(self, stream: _H2Stream, event: AcceptConnection) -> None:
        if stream.status is not _StreamStatus.CONNECTING or self.client:
            msg = "Cannot accept a WebSocket stream in this state"
            raise LocalProtocolError(msg)
        headers: Headers = [(b":status", b"200")]
        if event.subprotocol is not None:
            if stream.subprotocol not in (None, event.subprotocol):
                msg = f"unexpected subprotocol {event.subprotocol}"
                raise LocalProtocolError(msg)
            headers.append(
                (b"sec-websocket-protocol", event.subprotocol.encode("ascii")),
            )
            stream.subprotocol = event.subprotocol
        accepts = server_extensions_handshake(
            stream.request_extensions, event.extensions,
        )
        if accepts is not None:
            headers.append((b"sec-websocket-extensions", accepts))
        headers.extend(event.extra_headers)
        self._h2.send_headers(stream.stream_id, headers)
        self._establish(stream, event.extensions)

    def _send_reject(self, stream: _H2Stream, event: RejectConnection) -> None:
        if stream.status is not _StreamStatus.CONNECTING or self.client:
            msg = "Cannot reject a WebSocket stream in this state"
            raise LocalProtocolError(msg)
        if event.status_code == 200:
            msg = "RejectConnection must not use status 200"
            raise LocalProtocolError(msg)
        self._h2.send_headers(
            stream.stream_id,
            [(b":status", str(event.status_code).encode("ascii")), *event.headers],
            end_stream=True,
        )
        self._release_stream(stream)

    # -- WebSocket events: sending ---------------------------------------

    def _send_ws_event(self, stream: _H2Stream, event: Event) -> None:
        if stream.ws is None:
            msg = f"Event {event} cannot be sent before the stream is established"
            raise LocalProtocolError(msg)
        data = stream.ws.send(event)
        self._queue_data(stream, data)
        if stream.ws.state is ConnectionState.CLOSED:
            # The WebSocket closing handshake completed: half-close the
            # HTTP/2 stream so the peer learns the WebSocket is gone.
            self._end_stream(stream)

    def _queue_data(self, stream: _H2Stream, data: bytes) -> None:
        if stream.failed or stream.end_stream_sent:
            return
        if len(stream.send_buffer) + len(data) > stream.max_pending_bytes:
            msg = (
                f"Pending outbound data for stream {stream.stream_id} exceeds "
                f"the {stream.max_pending_bytes} byte limit"
            )
            raise LocalProtocolError(msg)
        stream.send_buffer += data

    def _end_stream(self, stream: _H2Stream) -> None:
        if stream.end_stream_sent or stream.failed:
            return
        if stream.status is _StreamStatus.REMOTE_HALF_CLOSED:
            stream.status = _StreamStatus.CLOSED
        else:
            stream.status = _StreamStatus.LOCAL_HALF_CLOSED

    def _flush_outbound(self) -> None:
        """Send buffered data, honouring flow control and frame size."""
        for stream in list(self._streams.values()):
            if stream.end_stream_sent or stream.failed:
                continue
            while stream.send_buffer:
                try:
                    window = self._h2.local_flow_control_window(stream.stream_id)
                except h2.exceptions.StreamClosedError:
                    stream.failed = True
                    self._release_stream(stream)
                    break
                if window <= 0:
                    break
                chunk = stream.send_buffer[
                    : min(window, self._h2.max_outbound_frame_size)
                ]
                del stream.send_buffer[: len(chunk)]
                end_stream = (
                    not stream.send_buffer
                    and stream.status in (
                        _StreamStatus.LOCAL_HALF_CLOSED,
                        _StreamStatus.CLOSED,
                    )
                )
                self._h2.send_data(
                    stream.stream_id, bytes(chunk), end_stream=end_stream,
                )
                if end_stream:
                    stream.end_stream_sent = True
                    break
            else:
                # Buffer drained; still owe the peer an END_STREAM frame.
                if stream.status in (
                    _StreamStatus.LOCAL_HALF_CLOSED,
                    _StreamStatus.CLOSED,
                ):
                    try:
                        self._h2.send_data(
                            stream.stream_id, b"", end_stream=True,
                        )
                        stream.end_stream_sent = True
                    except h2.exceptions.StreamClosedError:
                        stream.failed = True
                        self._release_stream(stream)
            if stream.end_stream_sent and stream.closed:
                # Both halves are done and the last byte has been flushed:
                # the stream's resources can be released silently.
                self._release_stream(stream, emit=False)

    # -- Receiving --------------------------------------------------------

    def receive_data(self, data: bytes) -> None:
        """
        Feed bytes received from the network into the HTTP/2 connection.

        HTTP/2 DATA frames are reassembled by wsproto and turned into
        WebSocket events; collect them with :meth:`events`. A failure on
        one stream (protocol error, reset, half-close) never affects the
        other streams.
        """
        if not data or self.terminated:
            return
        try:
            h2_events = self._h2.receive_data(data)
        except h2.exceptions.ProtocolError as exc:
            # h2 has already queued a GOAWAY frame; the whole connection
            # is unrecoverable.
            self._fail_all_streams(
                error_code=getattr(exc, "error_code", None),
            )
            return
        for h2_event in h2_events:
            self._handle_h2_event(h2_event)
        self._return_flow_control_credit()

    def _handle_h2_event(self, event: h2.events.Event) -> None:
        if isinstance(event, h2.events.RequestReceived):
            self._handle_request(event)
        elif isinstance(event, h2.events.ResponseReceived):
            self._handle_response(event)
        elif isinstance(event, h2.events.DataReceived):
            self._handle_data(event)
            if event.stream_ended is not None:
                self._handle_stream_ended(event.stream_ended)
        elif isinstance(event, h2.events.StreamEnded):
            self._handle_stream_ended(event)
        elif isinstance(event, h2.events.StreamReset):
            self._handle_stream_reset(event)
        elif isinstance(event, h2.events.ConnectionTerminated):
            self._handle_terminated(event)
        # SettingsAcknowledged, RemoteSettingsChanged, WindowUpdated,
        # PingReceived/Ack, PriorityUpdated, trailers and unknown frames
        # do not affect the WebSocket streams.

    def _handle_request(
        self, event: h2.events.RequestReceived,
    ) -> None:
        if self.client:
            self._h2.reset_stream(event.stream_id)
            return
        headers = self._norm_headers(event.headers)
        protocol_error: str | None = None
        if not self._h2.local_settings.enable_connect_protocol:
            protocol_error = (
                "Extended CONNECT is not enabled on this connection"
            )
        elif headers.get(b":method") != b"CONNECT":
            protocol_error = "Request method must be CONNECT"
        elif headers.get(b":protocol") != WEBSOCKET_PROTOCOL:
            protocol_error = ":protocol must be 'websocket'"
        elif b":scheme" not in headers or b":path" not in headers:
            protocol_error = "Extended CONNECT requires :scheme and :path"
        elif headers.get(b"sec-websocket-version") != WEBSOCKET_VERSION:
            protocol_error = "Missing or bad 'Sec-WebSocket-Version'"
        elif any(
            name in _FORBIDDEN_REQUEST_HEADERS
            for name in headers
            if not name.startswith(b":")
        ):
            protocol_error = "Forbidden hop-by-hop header in extended CONNECT"

        if protocol_error is not None:
            # RFC 8441 section 5: reject with a non-200 status instead of
            # corrupting the stream.
            self._h2.send_headers(
                event.stream_id,
                [(b":status", b"400")],
                end_stream=True,
            )
            return

        stream = _H2Stream(
            event.stream_id,
            self._max_pending_bytes,
            self._max_buffered_bytes,
        )
        self._streams[event.stream_id] = stream
        stream.request_extensions = self._split_header(
            headers.get(b"sec-websocket-extensions", b""),
        )
        subprotocols = self._split_header(
            headers.get(b"sec-websocket-protocol", b""),
        )
        extra_headers = [
            (name, value)
            for name, value in event.headers
            if not name.startswith(b":")
            and name
            not in (
                b"sec-websocket-version",
                b"sec-websocket-extensions",
                b"sec-websocket-protocol",
            )
        ]
        stream.events.append(
            Request(
                host=headers[b":authority"].decode("idna"),
                target=headers[b":path"].decode("ascii"),
                extensions=stream.request_extensions,
                extra_headers=extra_headers,
                subprotocols=subprotocols,
            ),
        )

    def _handle_response(
        self, event: h2.events.ResponseReceived,
    ) -> None:
        if not self.client:
            return
        stream = self._streams.get(event.stream_id)
        if stream is None or stream.ws is not None:
            return
        headers = self._norm_headers(event.headers)
        status = int(headers[b":status"])
        if status == 200:
            accepted = self._split_header(
                headers.get(b"sec-websocket-extensions", b""),
            )
            try:
                extensions = client_extensions_handshake(
                    accepted, stream.offered_extensions,
                )
            except RemoteProtocolError:
                # The server accepted an extension that was never offered:
                # fail just this stream.
                self._h2.reset_stream(
                    event.stream_id,
                    error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
                )
                self._release_stream(
                    stream,
                    error_code=int(h2.errors.ErrorCodes.PROTOCOL_ERROR),
                )
                return
            subprotocol = headers.get(b"sec-websocket-protocol")
            if subprotocol is not None:
                stream.subprotocol = subprotocol.decode("ascii")
            extra_headers = [
                (name, value)
                for name, value in event.headers
                if not name.startswith(b":")
                and name not in (
                    b"sec-websocket-extensions",
                    b"sec-websocket-protocol",
                )
            ]
            stream.accepted_extensions = extensions
            self._establish(stream, extensions)
            stream.events.append(
                AcceptConnection(
                    subprotocol=stream.subprotocol,
                    extensions=extensions,
                    extra_headers=extra_headers,
                ),
            )
        else:
            stream.events.append(
                RejectConnection(
                    status_code=status,
                    headers=[
                        (name, value)
                        for name, value in event.headers
                        if not name.startswith(b":")
                    ],
                ),
            )
            # The handshake failed; release the stream without a
            # StreamClosed event, the rejection says it all.
            self._release_stream(stream, emit=False)

    def _handle_data(self, event: h2.events.DataReceived) -> None:
        stream = self._streams.get(event.stream_id)
        if stream is None or stream.failed:
            return
        if stream.ws is None:
            # DATA on a stream whose handshake has not completed, or whose
            # response was rejected: protocol violation on that stream.
            self._h2.reset_stream(
                event.stream_id,
                error_code=h2.errors.ErrorCodes.PROTOCOL_ERROR,
            )
            self._release_stream(
                stream,
                error_code=int(h2.errors.ErrorCodes.PROTOCOL_ERROR),
            )
            return
        # Inbound flow control credit is only returned once wsproto has
        # consumed the data; until then it counts against the stream's
        # buffered byte budget.
        if stream.buffered_inbound() + event.flow_controlled_length > (
            stream.max_buffered_bytes
        ):
            # The peer is flooding us faster than we consume: reset just
            # this stream rather than endangering the whole connection.
            self._h2.reset_stream(
                event.stream_id,
                error_code=h2.errors.ErrorCodes.ENHANCE_YOUR_CALM,
            )
            self._release_stream(
                stream,
                error_code=int(h2.errors.ErrorCodes.ENHANCE_YOUR_CALM),
            )
            return
        stream.pending_ack += event.flow_controlled_length
        if event.data:
            stream.ws.receive_data(event.data)

    def _handle_stream_ended(self, event: h2.events.StreamEnded) -> None:
        stream = self._streams.get(event.stream_id)
        if stream is None:
            return
        self._drain_ws_events(stream)
        if stream.ws is None:
            # The peer ended the stream before the WebSocket handshake
            # completed: nothing can keep this stream alive.
            self._release_stream(stream)
            return
        if stream.ws.state not in (ConnectionState.CLOSED,):
            # The peer ended the HTTP/2 stream without a WebSocket close
            # frame: abnormal closure (RFC 6455 section 7.1.5).
            stream.ws.receive_data(None)
            self._drain_ws_events(stream)
        if stream.end_stream_sent:
            # Both directions are done: release silently, the closing
            # handshake events already told the application everything.
            self._release_stream(stream, emit=False)
            return
        if stream.ws.state is ConnectionState.CLOSED:
            # The WebSocket closing handshake finished: half-close our side
            # so the stream is fully torn down after the data flushes.
            self._end_stream(stream)
        if stream.status is _StreamStatus.LOCAL_HALF_CLOSED:
            stream.status = _StreamStatus.CLOSED
        elif stream.status is not _StreamStatus.CLOSED:
            stream.status = _StreamStatus.REMOTE_HALF_CLOSED
        # A half-close is not itself an error; keep the stream around so
        # buffered WebSocket events can still be drained by events().

    def _handle_stream_reset(self, event: h2.events.StreamReset) -> None:
        stream = self._streams.get(event.stream_id)
        if stream is None:
            return
        if not event.remote_reset:
            # We reset this stream ourselves; cleanup already happened.
            return
        self._drain_ws_events(stream)
        if stream.ws is not None and stream.ws.state not in (
            ConnectionState.CLOSED,
        ):
            stream.ws.receive_data(None)
            self._drain_ws_events(stream)
        self._release_stream(stream, error_code=int(event.error_code))

    def _handle_terminated(
        self, event: h2.events.ConnectionTerminated,
    ) -> None:
        self._fail_all_streams(
            error_code=int(event.error_code)
            if event.error_code is not None
            else None,
            last_stream_id=event.last_stream_id,
        )

    # -- Stream lifecycle -------------------------------------------------

    def _establish(
        self, stream: _H2Stream, extensions: list[Extension],
    ) -> None:
        stream.ws = Connection(
            ConnectionType.CLIENT if self.client else ConnectionType.SERVER,
            extensions,
        )
        stream.status = _StreamStatus.OPEN

    def _release_stream(
        self,
        stream: _H2Stream,
        error_code: int | None = None,
        emit: bool = True,
    ) -> None:
        """Release all resources held by a stream."""
        if stream.failed and stream.stream_id not in self._streams:
            return
        stream.failed = True
        stream.send_buffer.clear()
        stream.pending_ack = 0
        self._streams.pop(stream.stream_id, None)
        # Preserve events already queued for the application (e.g. a
        # RejectConnection attached to the stream being torn down).
        self._events.extend(stream.events)
        stream.events.clear()
        if emit:
            self._events.append(
                StreamClosed(stream_id=stream.stream_id, error_code=error_code),
            )

    def _fail_all_streams(
        self,
        error_code: int | None = None,
        last_stream_id: int | None = None,
    ) -> None:
        if self._terminated is not None:
            return
        for stream in list(self._streams.values()):
            self._drain_ws_events(stream)
            if stream.ws is not None and stream.ws.state not in (
                ConnectionState.CLOSED,
            ):
                stream.ws.receive_data(None)
                self._drain_ws_events(stream)
            self._release_stream(stream, error_code=error_code)
        self._terminated = ConnectionTerminated(
            error_code=error_code, last_stream_id=last_stream_id,
        )
        self._terminated_emitted = False

    def _drain_ws_events(self, stream: _H2Stream) -> None:
        """Move any ready wsproto events onto the stream's event queue."""
        if stream.ws is None:
            return
        for ws_event in stream.ws.events():
            stream.events.append(
                StreamEvent(stream_id=stream.stream_id, event=ws_event),
            )

    def _return_flow_control_credit(self) -> None:
        """
        Hand inbound flow control credit back for consumed data.

        Unacknowledged flow-controlled bytes are either still held in
        wsproto's buffer or have already been parsed into events; only
        the parsed fraction is acknowledged, so the peer's window stays
        shrunk while we hold unprocessed data. h2 coalesces the
        resulting WINDOW_UPDATE frames itself.
        """
        for stream in list(self._streams.values()):
            if stream.failed or stream.pending_ack <= 0:
                continue
            consumed = stream.pending_ack - stream.buffered_inbound()
            if consumed <= 0:
                continue
            stream.pending_ack -= consumed
            try:
                self._h2.acknowledge_received_data(consumed, stream.stream_id)
            except h2.exceptions.StreamClosedError:
                stream.pending_ack = 0

    # -- Events -----------------------------------------------------------

    def events(self) -> Generator[Event, None, None]:
        """
        Yield pending events for all streams.

        Handshake events (:class:`~wsproto.events.Request` /
        :class:`~wsproto.events.AcceptConnection` /
        :class:`~wsproto.events.RejectConnection`) are yielded directly;
        WebSocket-level events are wrapped in :class:`StreamEvent`. A
        :class:`StreamClosed` marks the death of one stream while other
        streams keep working, and a final :class:`ConnectionTerminated`
        marks the death of the whole HTTP/2 connection.
        """
        for stream in list(self._streams.values()):
            self._drain_ws_events(stream)
            while stream.events:
                yield stream.events.popleft()
        while self._events:
            yield self._events.popleft()
        # Events have been consumed by the application: hand the
        # corresponding inbound flow control credit back to the peer.
        self._return_flow_control_credit()
        if self._terminated is not None and not self._terminated_emitted:
            self._terminated_emitted = True
            yield self._terminated

    # -- Helpers ----------------------------------------------------------

    def _get_stream(self, stream_id: int | None) -> _H2Stream:
        if stream_id is None:
            msg = "A stream_id is required"
            raise LocalProtocolError(msg)
        stream = self._streams.get(stream_id)
        if stream is None:
            msg = f"Unknown or closed stream {stream_id}"
            raise LocalProtocolError(msg)
        return stream

    @staticmethod
    def _norm_headers(
        headers: list[tuple[bytes, bytes]],
    ) -> dict[bytes, bytes]:
        return {name: value for name, value in headers}

    @staticmethod
    def _split_header(value: bytes) -> list[str]:
        if not value:
            return []
        return split_comma_header(value)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(client={self.client}, "
            f"streams={len(self._streams)}, terminated={self.terminated})"
        )
