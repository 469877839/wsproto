"""
wsproto/extensions
~~~~~~~~~~~~~~~~~~

WebSocket extensions.
"""
from __future__ import annotations

import copy
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from .events import RejectConnection
from .frame_protocol import CloseReason, FrameDecoder, FrameProtocol, Opcode, RsvBits
from .utilities import RemoteProtocolError


@dataclass(frozen=True)
class ExtensionOffer:
    """
    A single element of a Sec-WebSocket-Extensions header.

    The parameter handling is left to the extension itself; this object only
    separates the extension name from the raw element so that parsing an offer
    does not touch any negotiation or runtime state.
    """

    name: str
    raw: str


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

    # ------------------------------------------------------------------
    # Three-phase negotiation API.
    #
    # ``parse_offer`` turns one header element into a plain value without
    # touching the extension. ``can_accept``/``can_finalize`` decide whether
    # the parsed value is acceptable without establishing anything. Only
    # ``establish`` mutates the extension into the runtime state for one
    # connection.
    #
    # The defaults forward to the legacy ``accept``/``finalize`` methods so
    # that extensions written against the old interface keep working.
    # ------------------------------------------------------------------

    def parse_offer(self, offer: ExtensionOffer) -> object | None:
        return offer

    def can_accept(self, offer: object) -> str | bool | None:
        if isinstance(offer, ExtensionOffer):
            return self.accept(offer.raw)
        return None

    def can_finalize(self, offer: object) -> bool:
        if isinstance(offer, ExtensionOffer):
            self.finalize(offer.raw)
            return True
        return False

    def response_allowed(self, response: object, offered: object) -> bool:
        return True

    def establish(self, offer: object) -> None:
        return None

    def reserved_bits(self) -> RsvBits:
        return RsvBits(False, False, False)

    def copy(self) -> Extension:
        """
        Return an independent extension for another connection.

        Negotiated configuration is preserved, but per-connection runtime
        state (compressor, decompressor, in-flight frame state) is reset.
        """
        try:
            return copy.deepcopy(self)
        except TypeError:
            return self

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
    ) -> tuple[RsvBits, bytes]:
        return (rsv, data)


@dataclass(frozen=True)
class PerMessageDeflateParameters:
    """
    Parsed values from one permessage-deflate header element.

    Window bits are kept as ``None`` when the corresponding parameter is
    absent. A bare ``client_max_window_bits`` parameter in a client offer is
    represented by ``client_max_window_bits_bare`` being true.
    """

    client_no_context_takeover: bool = False
    client_max_window_bits: int | None = None
    client_max_window_bits_bare: bool = False
    server_no_context_takeover: bool = False
    server_max_window_bits: int | None = None
    server_max_window_bits_bare: bool = False


class PerMessageDeflate(Extension):
    name = "permessage-deflate"

    DEFAULT_CLIENT_MAX_WINDOW_BITS = 15
    DEFAULT_SERVER_MAX_WINDOW_BITS = 15

    MIN_WINDOW_BITS = 9
    MAX_WINDOW_BITS = 15
    PARAMETERS = frozenset(
        {
            "client_no_context_takeover",
            "client_max_window_bits",
            "server_no_context_takeover",
            "server_max_window_bits",
        },
    )

    def __init__(
        self,
        client_no_context_takeover: bool = False,
        client_max_window_bits: int | None = None,
        server_no_context_takeover: bool = False,
        server_max_window_bits: int | None = None,
    ) -> None:
        self.client_no_context_takeover = client_no_context_takeover
        self.server_no_context_takeover = server_no_context_takeover
        self._client_max_window_bits = self.DEFAULT_CLIENT_MAX_WINDOW_BITS
        self._server_max_window_bits = self.DEFAULT_SERVER_MAX_WINDOW_BITS
        if client_max_window_bits is not None:
            self.client_max_window_bits = client_max_window_bits
        if server_max_window_bits is not None:
            self.server_max_window_bits = server_max_window_bits

        self._compressor: Optional[zlib._Compress] = None  # noqa
        self._decompressor: Optional[zlib._Decompress] = None  # noqa
        # This refers to the current frame
        self._inbound_is_compressible: bool | None = None
        # This refers to the ongoing message (which might span multiple
        # frames). Only the first frame in a fragmented message is flagged for
        # compression, so this carries that bit forward.
        self._inbound_compressed: bool | None = None

        self._enabled = False

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

    def _parse_parameters(self, raw: str) -> PerMessageDeflateParameters | None:
        """
        Parse a single permessage-deflate header element.

        Returns ``None`` when the element is malformed: an unknown parameter,
        a duplicated parameter, a value on a value-less parameter, or a
        window size that is not an integer between 9 and 15 inclusive.
        """
        parts = [part.strip() for part in raw.split(";")]
        if not parts or parts[0] != self.name:
            return None

        seen: set[str] = set()
        values: dict[str, int | None] = {}
        for part in parts[1:]:
            if not part:
                return None
            if "=" in part:
                param_name, _, param_value = part.partition("=")
                param_name = param_name.strip()
                param_value = param_value.strip()
            else:
                param_name = part
                param_value = ""
            if param_name not in self.PARAMETERS:
                return None
            if param_name in seen:
                return None
            seen.add(param_name)
            if param_name.endswith("no_context_takeover"):
                if param_value:
                    return None
                values[param_name] = None
            else:
                if param_value == "":
                    values[param_name] = None
                else:
                    try:
                        bits = int(param_value)
                    except ValueError:
                        return None
                    if bits < self.MIN_WINDOW_BITS or bits > self.MAX_WINDOW_BITS:
                        return None
                    values[param_name] = bits

        return PerMessageDeflateParameters(
            client_no_context_takeover="client_no_context_takeover" in values,
            client_max_window_bits=values.get("client_max_window_bits"),
            client_max_window_bits_bare=(
                "client_max_window_bits" in values
                and values["client_max_window_bits"] is None
            ),
            server_no_context_takeover="server_no_context_takeover" in values,
            server_max_window_bits=values.get("server_max_window_bits"),
            server_max_window_bits_bare=(
                "server_max_window_bits" in values
                and values["server_max_window_bits"] is None
            ),
        )

    def parse_offer(self, offer: ExtensionOffer) -> PerMessageDeflateParameters | None:
        if offer.name != self.name:
            return None
        return self._parse_parameters(offer.raw)

    def _normalize_offer(self, offer: str) -> str:
        # The legacy accept/finalize methods used to accept parameter
        # strings without the leading extension name.
        name = offer.split(";", 1)[0].strip()
        if name != self.name:
            return f"{self.name}; {offer}"
        return offer

    def can_accept(self, offer: object) -> str | None:
        """
        Judge a client offer as a server, without changing any state.

        Returns the parameters to send back in the response, or ``None`` if
        the offer cannot be accepted.
        """
        if not isinstance(offer, PerMessageDeflateParameters):
            return None

        parameters: list[str] = []

        if offer.client_no_context_takeover or self.client_no_context_takeover:
            parameters.append("client_no_context_takeover")
        if offer.server_no_context_takeover or self.server_no_context_takeover:
            parameters.append("server_no_context_takeover")

        if offer.client_max_window_bits_bare:
            parameters.append(
                f"client_max_window_bits={self.client_max_window_bits}",
            )
        elif offer.client_max_window_bits is not None:
            parameters.append(
                f"client_max_window_bits={min(offer.client_max_window_bits, self.client_max_window_bits)}",
            )
        elif self.client_max_window_bits != self.DEFAULT_CLIENT_MAX_WINDOW_BITS:
            # The offer did not include client_max_window_bits, so the
            # response must not include it either; the server cannot accept
            # an offer it cannot honour.
            return None

        if offer.server_max_window_bits_bare:
            return None
        if offer.server_max_window_bits is not None:
            parameters.append(
                f"server_max_window_bits={min(offer.server_max_window_bits, self.server_max_window_bits)}",
            )
        elif self.server_max_window_bits != self.DEFAULT_SERVER_MAX_WINDOW_BITS:
            parameters.append(
                f"server_max_window_bits={self.server_max_window_bits}",
            )

        return "; ".join(parameters)

    def can_finalize(self, offer: object) -> bool:
        """
        Judge a server response as a client, without changing any state.

        The response must not contain parameters that were not offered and
        must not exceed the offered window sizes.
        """
        if not isinstance(offer, PerMessageDeflateParameters):
            return False
        if offer.client_max_window_bits_bare or offer.server_max_window_bits_bare:
            return False
        if offer.client_max_window_bits is not None:
            if offer.client_max_window_bits > self.client_max_window_bits:
                return False
        if offer.server_max_window_bits is not None:
            if offer.server_max_window_bits > self.server_max_window_bits:
                return False
        return True

    def response_allowed(self, response: object, offered: object) -> bool:
        """
        Check a server response against the parameters the client offered.

        A response must not contain ``client_max_window_bits`` unless the
        offer included it, and window sizes must not exceed the offered
        values.
        """
        if not isinstance(response, PerMessageDeflateParameters):
            return False
        if not isinstance(offered, PerMessageDeflateParameters):
            return False
        if response.client_max_window_bits is not None:
            if (
                offered.client_max_window_bits is None
                and not offered.client_max_window_bits_bare
            ):
                return False
            if (
                offered.client_max_window_bits is not None
                and response.client_max_window_bits > offered.client_max_window_bits
            ):
                return False
        if response.server_max_window_bits is not None:
            if (
                offered.server_max_window_bits is not None
                and response.server_max_window_bits > offered.server_max_window_bits
            ):
                return False
        return True

    def establish(self, offer: object) -> None:
        """Establish the runtime state for one connection from a parsed offer."""
        if not isinstance(offer, PerMessageDeflateParameters):
            msg = "Cannot establish permessage-deflate from an unparsed offer"
            raise ValueError(msg)

        self.client_no_context_takeover = offer.client_no_context_takeover
        self.server_no_context_takeover = offer.server_no_context_takeover
        if offer.client_max_window_bits is not None:
            self.client_max_window_bits = offer.client_max_window_bits
        if offer.server_max_window_bits is not None:
            self.server_max_window_bits = offer.server_max_window_bits

        self._compressor = None
        self._decompressor = None
        self._inbound_is_compressible = None
        self._inbound_compressed = None
        self._enabled = True

    def finalize(self, offer: str) -> None:
        parameters = self._parse_parameters(self._normalize_offer(offer))
        if parameters is None:
            msg = f"Invalid permessage-deflate parameters: {offer}"
            raise RemoteProtocolError(msg, event_hint=RejectConnection())
        if not self.can_finalize(parameters):
            msg = f"Unacceptable permessage-deflate parameters: {offer}"
            raise RemoteProtocolError(msg, event_hint=RejectConnection())
        self.establish(parameters)

    def accept(self, offer: str) -> bool | None | str:
        parameters = self._parse_parameters(self._normalize_offer(offer))
        if parameters is None:
            return None
        response = self.can_accept(parameters)
        if response is None:
            return None
        self.establish(parameters)
        return response

    def reserved_bits(self) -> RsvBits:
        return RsvBits(True, False, False)

    def copy(self) -> PerMessageDeflate:
        new = type(self)(
            client_no_context_takeover=self.client_no_context_takeover,
            client_max_window_bits=self.client_max_window_bits,
            server_no_context_takeover=self.server_no_context_takeover,
            server_max_window_bits=self.server_max_window_bits,
        )
        new._enabled = self._enabled
        return new

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

        if self._inbound_compressed is None:
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
        if not self._inbound_is_compressible:
            self._inbound_compressed = None
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

    def frame_outbound(
        self,
        proto: FrameDecoder | FrameProtocol,
        opcode: Opcode,
        rsv: RsvBits,
        data: bytes,
        fin: bool,
    ) -> tuple[RsvBits, bytes]:
        if not self._compressible_opcode(opcode):
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
