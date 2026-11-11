"""
wsproto/extensions
~~~~~~~~~~~~~~~~~~

WebSocket extensions.
"""
from __future__ import annotations

import zlib
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import NamedTuple, Optional

from .frame_protocol import CloseReason, FrameDecoder, FrameProtocol, Opcode, RsvBits


class MessageMetadata(NamedTuple):
    """
    Metadata describing an outbound message, as known before its first
    frame is sent.

    This is passed to a permessage-deflate compression strategy so that
    the strategy can decide whether to compress the message without
    having to buffer the complete message first.

    .. attribute:: opcode

       The opcode of the message's first frame, either
       :attr:`Opcode.TEXT <wsproto.frame_protocol.Opcode.TEXT>` or
       :attr:`Opcode.BINARY <wsproto.frame_protocol.Opcode.BINARY>`.

    .. attribute:: payload_length

       The length in bytes of the message's first frame payload. This
       is the length of the first chunk handed to
       :meth:`wsproto.Connection.send`, not necessarily the length of
       the whole message.

    .. attribute:: message_finished

       True if the first frame is also the final frame of the message.
    """

    opcode: Opcode
    payload_length: int
    message_finished: bool


#: A callable deciding, per message, whether permessage-deflate should
#: compress it. It is called with the message's :class:`MessageMetadata`
#: before the first frame of the message is sent.
CompressionStrategy = Callable[[MessageMetadata], bool]


class Extension(ABC):
    name: str

    def enabled(self) -> bool:
        return False

    @abstractmethod
    def offer(self) -> bool | str:
        pass

    def accept(self, offer: str) -> bool | str | None:
        pass

    def finalize(self, offer: str) -> None:
        pass

    def frame_inbound_header(
        self,
        proto: FrameDecoder | FrameProtocol,
        opcode: Opcode,
        rsv: RsvBits,
        payload_length: int,
    ) -> CloseReason | RsvBits:
        return RsvBits(False, False, False)

    def frame_inbound_payload_data(
        self, proto: FrameDecoder | FrameProtocol, data: bytes,
    ) -> bytes | CloseReason:
        return data

    def frame_inbound_complete(
        self, proto: FrameDecoder | FrameProtocol, fin: bool,
    ) -> bytes | CloseReason | None:
        pass

    def frame_outbound(
        self,
        proto: FrameDecoder | FrameProtocol,
        opcode: Opcode,
        rsv: RsvBits,
        data: bytes,
        fin: bool,
        compress: bool | None = None,
    ) -> tuple[RsvBits, bytes]:
        return (rsv, data)


class PerMessageDeflate(Extension):
    name = "permessage-deflate"

    DEFAULT_CLIENT_MAX_WINDOW_BITS = 15
    DEFAULT_SERVER_MAX_WINDOW_BITS = 15

    def __init__(
        self,
        client_no_context_takeover: bool = False,
        client_max_window_bits: int | None = None,
        server_no_context_takeover: bool = False,
        server_max_window_bits: int | None = None,
        compress: bool | CompressionStrategy | None = None,
    ) -> None:
        self.client_no_context_takeover = client_no_context_takeover
        self.server_no_context_takeover = server_no_context_takeover
        self._client_max_window_bits = self.DEFAULT_CLIENT_MAX_WINDOW_BITS
        self._server_max_window_bits = self.DEFAULT_SERVER_MAX_WINDOW_BITS
        if client_max_window_bits is not None:
            self.client_max_window_bits = client_max_window_bits
        if server_max_window_bits is not None:
            self.server_max_window_bits = server_max_window_bits

        # The default compression policy for outbound messages that do not
        # carry an explicit per-message compress hint.
        self._compress: bool | CompressionStrategy | None = None
        self.compress = compress

        self._compressor: Optional[zlib._Compress] = None  # noqa
        self._decompressor: Optional[zlib._Decompress] = None  # noqa
        # This refers to the current frame
        self._inbound_is_compressible: bool | None = None
        # This refers to the ongoing message (which might span multiple
        # frames). Only the first frame in a fragmented message is flagged for
        # compression, so this carries that bit forward.
        self._inbound_compressed: bool | None = None
        # This refers to the ongoing outbound message (which might span
        # multiple frames). The decision is taken once, before the first
        # frame of the message is sent, and carries over to every
        # continuation frame.
        self._outbound_compressed: bool | None = None

        self._enabled = False

    @property
    def compress(self) -> bool | CompressionStrategy | None:
        """
        The default policy for outbound messages.

        ``True`` compresses every data message, ``False`` sends every
        data message uncompressed, and a callable is invoked with the
        :class:`MessageMetadata` of each message before its first frame
        is sent and returns whether that message should be compressed.
        ``None`` keeps the historical behavior of compressing every data
        message. Per-message hints on an outbound event take precedence
        over this policy.
        """
        return self._compress

    @compress.setter
    def compress(self, value: bool | CompressionStrategy | None) -> None:
        if value is not None and not isinstance(value, (bool, Callable)):
            msg = "compress must be a bool, a callable or None"
            raise TypeError(msg)
        self._compress = value

    @property
    def client_max_window_bits(self) -> int:
        return self._client_max_window_bits

    @client_max_window_bits.setter
    def client_max_window_bits(self, value: int) -> None:
        if value < 9 or value > 15:
            msg = "Window size must be between 9 and 15 inclusive"
            raise ValueError(msg)
        self._client_max_window_bits = value

    @property
    def server_max_window_bits(self) -> int:
        return self._server_max_window_bits

    @server_max_window_bits.setter
    def server_max_window_bits(self, value: int) -> None:
        if value < 9 or value > 15:
            msg = "Window size must be between 9 and 15 inclusive"
            raise ValueError(msg)
        self._server_max_window_bits = value

    def _compressible_opcode(self, opcode: Opcode) -> bool:
        return opcode in (Opcode.TEXT, Opcode.BINARY, Opcode.CONTINUATION)

    def enabled(self) -> bool:
        return self._enabled

    def offer(self) -> bool | str:
        parameters = [
            f"client_max_window_bits={self.client_max_window_bits}",
            f"server_max_window_bits={self.server_max_window_bits}",
        ]

        if self.client_no_context_takeover:
            parameters.append("client_no_context_takeover")
        if self.server_no_context_takeover:
            parameters.append("server_no_context_takeover")

        return "; ".join(parameters)

    def finalize(self, offer: str) -> None:
        bits = [b.strip() for b in offer.split(";")]
        for bit in bits[1:]:
            if bit.startswith("client_no_context_takeover"):
                self.client_no_context_takeover = True
            elif bit.startswith("server_no_context_takeover"):
                self.server_no_context_takeover = True
            elif bit.startswith("client_max_window_bits"):
                self.client_max_window_bits = int(bit.split("=", 1)[1].strip())
            elif bit.startswith("server_max_window_bits"):
                self.server_max_window_bits = int(bit.split("=", 1)[1].strip())

        self._enabled = True

    def _parse_params(self, params: str) -> tuple[int | None, int | None]:
        client_max_window_bits = None
        server_max_window_bits = None

        bits = [b.strip() for b in params.split(";")]
        for bit in bits[1:]:
            if bit.startswith("client_no_context_takeover"):
                self.client_no_context_takeover = True
            elif bit.startswith("server_no_context_takeover"):
                self.server_no_context_takeover = True
            elif bit.startswith("client_max_window_bits"):
                if "=" in bit:
                    client_max_window_bits = int(bit.split("=", 1)[1].strip())
                else:
                    client_max_window_bits = self.client_max_window_bits
            elif bit.startswith("server_max_window_bits"):
                if "=" in bit:
                    server_max_window_bits = int(bit.split("=", 1)[1].strip())
                else:
                    server_max_window_bits = self.server_max_window_bits

        return client_max_window_bits, server_max_window_bits

    def accept(self, offer: str) -> bool | None | str:
        client_max_window_bits, server_max_window_bits = self._parse_params(offer)

        parameters = []

        if self.client_no_context_takeover:
            parameters.append("client_no_context_takeover")
        if self.server_no_context_takeover:
            parameters.append("server_no_context_takeover")
        try:
            if client_max_window_bits is not None:
                parameters.append(f"client_max_window_bits={client_max_window_bits}")
                self.client_max_window_bits = client_max_window_bits
            if server_max_window_bits is not None:
                parameters.append(f"server_max_window_bits={server_max_window_bits}")
                self.server_max_window_bits = server_max_window_bits
        except ValueError:
            return None
        else:
            self._enabled = True
            return "; ".join(parameters)

    def frame_inbound_header(
        self,
        proto: FrameDecoder | FrameProtocol,
        opcode: Opcode,
        rsv: RsvBits,
        payload_length: int,
    ) -> CloseReason | RsvBits:
        if rsv.rsv1 and opcode.iscontrol():
            return CloseReason.PROTOCOL_ERROR
        if rsv.rsv1 and opcode is Opcode.CONTINUATION:
            return CloseReason.PROTOCOL_ERROR

        self._inbound_is_compressible = self._compressible_opcode(opcode)

        if self._inbound_is_compressible and self._inbound_compressed is None:
            self._inbound_compressed = rsv.rsv1
            if self._inbound_compressed:
                assert self._inbound_is_compressible
                if proto.client:
                    bits = self.server_max_window_bits
                else:
                    bits = self.client_max_window_bits
                if self._decompressor is None:
                    self._decompressor = zlib.decompressobj(-int(bits))

        return RsvBits(True, False, False)

    def frame_inbound_payload_data(
        self, proto: FrameDecoder | FrameProtocol, data: bytes,
    ) -> bytes | CloseReason:
        if not self._inbound_compressed or not self._inbound_is_compressible:
            return data
        assert self._decompressor is not None

        try:
            return self._decompressor.decompress(bytes(data))
        except zlib.error:
            return CloseReason.INVALID_FRAME_PAYLOAD_DATA

    def frame_inbound_complete(
        self, proto: FrameDecoder | FrameProtocol, fin: bool,
    ) -> bytes | CloseReason | None:
        if not fin:
            return None
        # An interleaved control frame must not reset the compression state of
        # an in-progress message; that state is only cleared by a message
        # frame.
        if not self._inbound_is_compressible:
            return None
        if not self._inbound_compressed:
            self._inbound_compressed = None
            return None
        assert self._decompressor is not None

        try:
            data = self._decompressor.decompress(b"\x00\x00\xff\xff")
            data += self._decompressor.flush()
        except zlib.error:
            return CloseReason.INVALID_FRAME_PAYLOAD_DATA

        if proto.client:
            no_context_takeover = self.server_no_context_takeover
        else:
            no_context_takeover = self.client_no_context_takeover

        if no_context_takeover:
            self._decompressor = None

        self._inbound_compressed = None

        return data

    def _resolve_compression(
        self, opcode: Opcode, data: bytes, fin: bool, compress: bool | None,
    ) -> bool:
        # An explicit per-message hint takes precedence over the policy.
        if compress is not None:
            return compress
        policy = self._compress
        if policy is None or isinstance(policy, bool):
            # None keeps the historical behavior of compressing every
            # data message.
            return policy is not False
        # A strategy is called with the metadata known before the first
        # frame of the message is sent; any exception it raises
        # propagates before a single byte of the message is emitted.
        return bool(
            policy(
                MessageMetadata(
                    opcode=opcode, payload_length=len(data), message_finished=fin,
                ),
            ),
        )

    def frame_outbound(
        self,
        proto: FrameDecoder | FrameProtocol,
        opcode: Opcode,
        rsv: RsvBits,
        data: bytes,
        fin: bool,
        compress: bool | None = None,
    ) -> tuple[RsvBits, bytes]:
        if not self._compressible_opcode(opcode):
            return (rsv, data)

        if opcode is not Opcode.CONTINUATION:
            # The compression decision is taken once, before the first
            # frame of the message is serialized, and carries over to
            # every continuation frame of the message.
            self._outbound_compressed = self._resolve_compression(
                opcode, data, fin, compress,
            )
        elif self._outbound_compressed is None:
            msg = "CONTINUATION frame without a preceding message"
            raise ValueError(msg)

        message_compressed = self._outbound_compressed

        if not message_compressed:
            if fin:
                self._outbound_compressed = None
            # An uncompressed message must not advance the compressor
            # state, so that a following compressed message continues
            # the compression context (or starts it, as negotiated).
            return (rsv, data)

        if opcode is not Opcode.CONTINUATION:
            rsv = RsvBits(True, rsv[1], rsv[2])

        if self._compressor is None:
            assert opcode is not Opcode.CONTINUATION
            if proto.client:
                bits = self.client_max_window_bits
            else:
                bits = self.server_max_window_bits
            self._compressor = zlib.compressobj(
                zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -int(bits),
            )

        data = self._compressor.compress(bytes(data))

        if fin:
            data += self._compressor.flush(zlib.Z_SYNC_FLUSH)
            data = data[:-4]

            if proto.client:
                no_context_takeover = self.client_no_context_takeover
            else:
                no_context_takeover = self.server_no_context_takeover

            if no_context_takeover:
                self._compressor = None

            self._outbound_compressed = None

        return (rsv, data)

    def __repr__(self) -> str:
        descr = [f"client_max_window_bits={self.client_max_window_bits}"]
        if self.client_no_context_takeover:
            descr.append("client_no_context_takeover")
        descr.append(f"server_max_window_bits={self.server_max_window_bits}")
        if self.server_no_context_takeover:
            descr.append("server_no_context_takeover")

        return "<{} {}>".format(self.__class__.__name__, "; ".join(descr))


#: SUPPORTED_EXTENSIONS maps all supported extension names to their class.
#: This can be used to iterate all supported extensions of wsproto, instantiate
#: new extensions based on their name, or check if a given extension is
#: supported or not.
SUPPORTED_EXTENSIONS = {PerMessageDeflate.name: PerMessageDeflate}
