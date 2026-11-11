from __future__ import annotations

import struct
import zlib
from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional, cast

import pytest

from wsproto import extensions as wpext
from wsproto import frame_protocol as fp
from wsproto.connection import CLIENT, SERVER, Connection
from wsproto.events import BytesMessage, Ping, Pong, TextMessage

if TYPE_CHECKING:
    from typing import TypedDict

    class Params(TypedDict, total=False):
        client_no_context_takeover: bool
        client_max_window_bits: Optional[int]
        server_no_context_takeover: bool
        server_max_window_bits: Optional[int]

else:
    Params = dict


class TestPerMessageDeflate:
    parameter_sets: Sequence[Params] = [
        {
            "client_no_context_takeover": False,
            "client_max_window_bits": 15,
            "server_no_context_takeover": False,
            "server_max_window_bits": 15,
        },
        {
            "client_no_context_takeover": True,
            "client_max_window_bits": 9,
            "server_no_context_takeover": False,
            "server_max_window_bits": 15,
        },
        {
            "client_no_context_takeover": False,
            "client_max_window_bits": 15,
            "server_no_context_takeover": True,
            "server_max_window_bits": 9,
        },
        {
            "client_no_context_takeover": True,
            "client_max_window_bits": 9,
            "server_no_context_takeover": True,
            "server_max_window_bits": 9,
        },
        {"client_no_context_takeover": True, "server_max_window_bits": 9},
        {"server_no_context_takeover": True, "client_max_window_bits": 9},
        {"client_max_window_bits": None, "server_max_window_bits": None},
        {},
    ]

    def make_offer_string(self, params: Params) -> str:
        offer = ["permessage-deflate"]

        if "client_max_window_bits" in params:
            if params["client_max_window_bits"] is None:
                offer.append("client_max_window_bits")
            else:
                offer.append(
                    "client_max_window_bits=%d" % params["client_max_window_bits"],
                )
        if "server_max_window_bits" in params:
            if params["server_max_window_bits"] is None:
                offer.append("server_max_window_bits")
            else:
                offer.append(
                    "server_max_window_bits=%d" % params["server_max_window_bits"],
                )
        if params.get("client_no_context_takeover", False):
            offer.append("client_no_context_takeover")
        if params.get("server_no_context_takeover", False):
            offer.append("server_no_context_takeover")

        return "; ".join(offer)

    def compare_params_to_string(
        self, params: Params, ext: wpext.PerMessageDeflate, param_string: str,
    ) -> None:
        if "client_max_window_bits" in params:
            if params["client_max_window_bits"] is None:
                bits = ext.client_max_window_bits
            else:
                bits = params["client_max_window_bits"]
            assert "client_max_window_bits=%d" % bits in param_string
        if "server_max_window_bits" in params:
            if params["server_max_window_bits"] is None:
                bits = ext.server_max_window_bits
            else:
                bits = params["server_max_window_bits"]
            assert "server_max_window_bits=%d" % bits in param_string
        if params.get("client_no_context_takeover", False):
            assert "client_no_context_takeover" in param_string
        if params.get("server_no_context_takeover", False):
            assert "server_no_context_takeover" in param_string

    @pytest.mark.parametrize("params", parameter_sets)
    def test_offer(self, params: Params) -> None:
        ext = wpext.PerMessageDeflate(**params)
        offer = ext.offer()
        offer = cast("str", offer)

        self.compare_params_to_string(params, ext, offer)

    @pytest.mark.parametrize("params", parameter_sets)
    def test_finalize(self, params: Params) -> None:
        ext = wpext.PerMessageDeflate()
        assert not ext.enabled()

        if "client_max_window_bits" in params:
            if params["client_max_window_bits"] is None:
                del params["client_max_window_bits"]
        if "server_max_window_bits" in params:
            if params["server_max_window_bits"] is None:
                del params["server_max_window_bits"]
        offer = self.make_offer_string(params)
        ext.finalize(offer)

        if params.get("client_max_window_bits", None):
            assert ext.client_max_window_bits == params["client_max_window_bits"]
        if params.get("server_max_window_bits", None):
            assert ext.server_max_window_bits == params["server_max_window_bits"]
        assert ext.client_no_context_takeover is params.get(
            "client_no_context_takeover", False,
        )
        assert ext.server_no_context_takeover is params.get(
            "server_no_context_takeover", False,
        )

        assert ext.enabled()

    def test_finalize_ignores_rubbish(self) -> None:
        ext = wpext.PerMessageDeflate()
        assert not ext.enabled()

        ext.finalize("i am the lizard queen; worship me")

        assert ext.enabled()

    @pytest.mark.parametrize("params", parameter_sets)
    def test_accept(self, params: Params) -> None:
        ext = wpext.PerMessageDeflate()
        assert not ext.enabled()

        offer = self.make_offer_string(params)
        response = ext.accept(offer)
        response = cast("str", response)

        if ext.client_no_context_takeover:
            assert "client_no_context_takeover" in response
        if ext.server_no_context_takeover:
            assert "server_no_context_takeover" in response
        if "client_max_window_bits" in params:
            if params["client_max_window_bits"] is None:
                bits = ext.client_max_window_bits
            else:
                bits = params["client_max_window_bits"]
            assert ext.client_max_window_bits == bits
            assert "client_max_window_bits=%d" % bits in response
        if "server_max_window_bits" in params:
            if params["server_max_window_bits"] is None:
                bits = ext.server_max_window_bits
            else:
                bits = params["server_max_window_bits"]
            assert ext.server_max_window_bits == bits
            assert "server_max_window_bits=%d" % bits in response

    def test_accept_ignores_rubbish(self) -> None:
        ext = wpext.PerMessageDeflate()
        assert not ext.enabled()

        ext.accept("i am the lizard queen; worship me")

        assert ext.enabled()

    def test_inbound_uncompressed_control_frame(self) -> None:
        payload = b"x" * 23

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        result = ext.frame_inbound_header(
            proto, fp.Opcode.PING, fp.RsvBits(False, False, False), len(payload),
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1

        data = ext.frame_inbound_payload_data(proto, payload)
        assert data == payload

        assert ext.frame_inbound_complete(proto, True) is None

    def test_inbound_compressed_control_frame(self) -> None:
        payload = b"x" * 23

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        result = ext.frame_inbound_header(
            proto, fp.Opcode.PING, fp.RsvBits(True, False, False), len(payload),
        )
        assert result == fp.CloseReason.PROTOCOL_ERROR

    def test_inbound_compressed_continuation_frame(self) -> None:
        payload = b"x" * 23

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        result = ext.frame_inbound_header(
            proto, fp.Opcode.CONTINUATION, fp.RsvBits(True, False, False), len(payload),
        )
        assert result == fp.CloseReason.PROTOCOL_ERROR

    def test_inbound_uncompressed_data_frame(self) -> None:
        payload = b"x" * 23

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        result = ext.frame_inbound_header(
            proto, fp.Opcode.BINARY, fp.RsvBits(False, False, False), len(payload),
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1

        data = ext.frame_inbound_payload_data(proto, payload)
        assert data == payload

        assert ext.frame_inbound_complete(proto, True) is None

    @pytest.mark.parametrize("client", [True, False])
    def test_client_inbound_compressed_single_data_frame(self, client: bool) -> None:
        payload = b"x" * 23
        compressed_payload = b"\xaa\xa8\xc0\n\x00\x00"

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        result = ext.frame_inbound_header(
            proto,
            fp.Opcode.BINARY,
            fp.RsvBits(True, False, False),
            len(compressed_payload),
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1

        data = ext.frame_inbound_payload_data(proto, compressed_payload)
        assert isinstance(data, bytes)
        data2 = ext.frame_inbound_complete(proto, True)
        assert isinstance(data2, bytes)
        assert data + data2 == payload

    @pytest.mark.parametrize("client", [True, False])
    def test_client_inbound_compressed_multiple_data_frames(self, client: bool) -> None:
        payload = b"x" * 23
        compressed_payload = b"\xaa\xa8\xc0\n\x00\x00"
        split = 3
        data = b""

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        result = ext.frame_inbound_header(
            proto, fp.Opcode.BINARY, fp.RsvBits(True, False, False), split,
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1
        result2 = ext.frame_inbound_payload_data(proto, compressed_payload[:split])
        assert not isinstance(result2, fp.CloseReason)
        data += result2
        assert ext.frame_inbound_complete(proto, False) is None

        result3 = ext.frame_inbound_header(
            proto,
            fp.Opcode.CONTINUATION,
            fp.RsvBits(False, False, False),
            len(compressed_payload) - split,
        )
        assert isinstance(result3, fp.RsvBits)
        assert result3.rsv1
        result4 = ext.frame_inbound_payload_data(proto, compressed_payload[split:])
        assert not isinstance(result4, fp.CloseReason)
        data += result4

        result5 = ext.frame_inbound_complete(proto, True)
        assert isinstance(result5, bytes)
        data += result5

        assert data == payload

    @pytest.mark.parametrize("client", [True, False])
    def test_client_decompress_after_uncompressible_frame(self, client: bool) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        # A PING frame
        ext.frame_inbound_header(
            proto, fp.Opcode.PING, fp.RsvBits(False, False, False), 0,
        )
        result2 = ext.frame_inbound_payload_data(proto, b"")
        assert not isinstance(result2, fp.CloseReason)
        assert ext.frame_inbound_complete(proto, True) is None

        # A compressed TEXT frame
        payload = b"x" * 23
        compressed_payload = b"\xaa\xa8\xc0\n\x00\x00"

        result3 = ext.frame_inbound_header(
            proto,
            fp.Opcode.TEXT,
            fp.RsvBits(True, False, False),
            len(compressed_payload),
        )
        assert isinstance(result3, fp.RsvBits)
        assert result3.rsv1
        result4 = ext.frame_inbound_payload_data(proto, compressed_payload)
        assert result4 == payload

        result5 = ext.frame_inbound_complete(proto, True)
        assert not isinstance(result5, fp.CloseReason)

    def test_inbound_bad_zlib_payload(self) -> None:
        compressed_payload = b"x" * 23

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        result = ext.frame_inbound_header(
            proto,
            fp.Opcode.BINARY,
            fp.RsvBits(True, False, False),
            len(compressed_payload),
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1
        result2 = ext.frame_inbound_payload_data(proto, compressed_payload)
        assert result2 is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    def test_inbound_bad_zlib_decoder_end_state(self) -> None:
        compressed_payload = b"x" * 23

        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        result = ext.frame_inbound_header(
            proto,
            fp.Opcode.BINARY,
            fp.RsvBits(True, False, False),
            len(compressed_payload),
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1

        class FailDecompressor:
            def decompress(self, data: bytes) -> bytes:
                return b""

            def flush(self) -> None:
                raise zlib.error()

        ext._decompressor = cast("zlib._Decompress", FailDecompressor())
        result2 = ext.frame_inbound_complete(proto, True)

        assert result2 is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    @pytest.mark.parametrize(
        "client,no_context_takeover",
        [(True, True), (True, False), (False, True), (False, False)],
    )
    def test_decompressor_reset(self, client: bool, no_context_takeover: bool) -> None:
        if client:
            args = {"server_no_context_takeover": no_context_takeover}
        else:
            args = {"client_no_context_takeover": no_context_takeover}
        ext = wpext.PerMessageDeflate(**args)
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        result = ext.frame_inbound_header(
            proto, fp.Opcode.BINARY, fp.RsvBits(True, False, False), 0,
        )
        assert isinstance(result, fp.RsvBits)
        assert result.rsv1

        assert ext._decompressor is not None

        result2 = ext.frame_inbound_complete(proto, True)
        assert not isinstance(result2, fp.CloseReason)

        if no_context_takeover:
            assert ext._decompressor is None
        else:
            assert ext._decompressor is not None

        result3 = ext.frame_inbound_header(
            proto, fp.Opcode.BINARY, fp.RsvBits(True, False, False), 0,
        )
        assert isinstance(result3, fp.RsvBits)
        assert result3.rsv1

        assert ext._decompressor is not None

    def test_outbound_uncompressible_opcode(self) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        payload = b"x" * 23

        rsv, data = ext.frame_outbound(proto, fp.Opcode.PING, rsv, payload, True)

        assert rsv.rsv1 is False
        assert data == payload

    @pytest.mark.parametrize("client", [True, False])
    def test_outbound_compress_single_frame(self, client: bool) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        payload = b"x" * 23
        compressed_payload = b"\xaa\xa8\xc0\n\x00\x00"

        rsv, data = ext.frame_outbound(proto, fp.Opcode.BINARY, rsv, payload, True)

        assert rsv.rsv1 is True
        assert data == compressed_payload

    @pytest.mark.parametrize("client", [True, False])
    def test_outbound_compress_multiple_frames(self, client: bool) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        payload = b"x" * 23
        split = 12
        compressed_payload = b"\xaa\xa8\xc0\n\x00\x00"

        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload[:split], False,
        )
        assert rsv.rsv1 is True

        rsv = fp.RsvBits(False, False, False)
        rsv, more_data = ext.frame_outbound(
            proto, fp.Opcode.CONTINUATION, rsv, payload[split:], True,
        )
        assert rsv.rsv1 is False
        assert data + more_data == compressed_payload

    @pytest.mark.parametrize(
        "client,no_context_takeover",
        [(True, True), (True, False), (False, True), (False, False)],
    )
    def test_compressor_reset(self, client: bool, no_context_takeover: bool) -> None:
        if client:
            args = {"client_no_context_takeover": no_context_takeover}
        else:
            args = {"server_no_context_takeover": no_context_takeover}
        ext = wpext.PerMessageDeflate(**args)
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])
        rsv = fp.RsvBits(False, False, False)

        rsv, data = ext.frame_outbound(proto, fp.Opcode.BINARY, rsv, b"", False)
        assert rsv.rsv1 is True
        assert ext._compressor is not None

        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(proto, fp.Opcode.CONTINUATION, rsv, b"", True)
        assert rsv.rsv1 is False
        if no_context_takeover:
            assert ext._compressor is None
        else:
            assert ext._compressor is not None

        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(proto, fp.Opcode.BINARY, rsv, b"", False)
        assert rsv.rsv1 is True
        assert ext._compressor is not None

    @pytest.mark.parametrize("params", parameter_sets)
    def test_repr(self, params: Params) -> None:
        ext = wpext.PerMessageDeflate(**params)
        self.compare_params_to_string(params, ext, repr(ext))

    def test_compress_policy_validation(self) -> None:
        for value in (None, True, False, lambda metadata: True):
            ext = wpext.PerMessageDeflate(compress=value)
            assert ext.compress is value
            ext.compress = value
            assert ext.compress is value

        with pytest.raises(TypeError):
            wpext.PerMessageDeflate(compress="compress")

        ext = wpext.PerMessageDeflate()
        with pytest.raises(TypeError):
            ext.compress = 1

    def test_negotiation_unaffected_by_compression_policy(self) -> None:
        plain = wpext.PerMessageDeflate()
        assert wpext.PerMessageDeflate(compress=True).offer() == plain.offer()
        assert wpext.PerMessageDeflate(compress=False).offer() == plain.offer()
        assert (
            wpext.PerMessageDeflate(compress=lambda metadata: True).offer()
            == plain.offer()
        )

    @pytest.mark.parametrize("client", [True, False])
    def test_outbound_uncompressed_single_frame(self, client: bool) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        payload = b"x" * 23

        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload, True, False,
        )

        assert rsv.rsv1 is False
        assert data == payload
        assert ext._compressor is None
        assert ext._outbound_compressed is None

    @pytest.mark.parametrize("client", [True, False])
    def test_outbound_uncompressed_multiple_frames(self, client: bool) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        payload = b"x" * 23
        split = 12
        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload[:split], False, False,
        )
        assert rsv.rsv1 is False
        assert data == payload[:split]

        rsv = fp.RsvBits(False, False, False)
        rsv, more_data = ext.frame_outbound(
            proto, fp.Opcode.CONTINUATION, rsv, payload[split:], True, None,
        )
        assert rsv.rsv1 is False
        assert more_data == payload[split:]
        assert data + more_data == payload
        assert ext._compressor is None
        assert ext._outbound_compressed is None

    @pytest.mark.parametrize("client", [True, False])
    def test_uncompressed_message_does_not_touch_compressor(
        self, client: bool,
    ) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        # A compressed message primes the compressor and keeps the context.
        rsv, first = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, b"hello world", True, True,
        )
        assert rsv.rsv1 is True
        assert ext._compressor is not None
        compressor = ext._compressor

        # An uncompressed message leaves the compressor and its context alone.
        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, b"heartbeat", True, False,
        )
        assert rsv.rsv1 is False
        assert data == b"heartbeat"
        assert ext._compressor is compressor

        # The next compressed message reuses that context.
        rsv = fp.RsvBits(False, False, False)
        rsv, again = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, b"hello world", True, True,
        )
        assert rsv.rsv1 is True
        assert again != first
        assert len(again) < len(first)

    @pytest.mark.parametrize("client", [True, False])
    def test_consecutive_uncompressed_and_compressed_messages(
        self, client: bool,
    ) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])
        payload = b"x" * 23
        compressed_payload = b"\xaa\xa8\xc0\n\x00\x00"
        decompressor = zlib.decompressobj(-15)

        for compress in (False, True, True, False):
            rsv = fp.RsvBits(False, False, False)
            rsv, data = ext.frame_outbound(
                proto, fp.Opcode.BINARY, rsv, payload, True, compress,
            )
            assert rsv.rsv1 is compress
            if compress:
                # With context takeover the exact bytes depend on the
                # preceding messages, so verify the content instead.
                inflated = decompressor.decompress(data + b"\x00\x00\xff\xff")
                assert inflated == payload
            else:
                assert data == payload

        # The very first compressed message of a fresh extension matches the
        # well-known deflate output.
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])
        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload, True, True,
        )
        assert rsv.rsv1 is True
        assert data == compressed_payload

    @pytest.mark.parametrize("client", [True, False])
    def test_continuation_cannot_change_compression_decision(
        self, client: bool,
    ) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])
        payload = b"x" * 23

        rsv = fp.RsvBits(False, False, False)
        rsv, first = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload[:10], False, False,
        )
        assert rsv.rsv1 is False
        assert ext._outbound_compressed is False

        # A hint on a continuation is ignored: it stays uncompressed.
        rsv = fp.RsvBits(False, False, False)
        rsv, rest = ext.frame_outbound(
            proto, fp.Opcode.CONTINUATION, rsv, payload[10:], True, True,
        )
        assert rsv.rsv1 is False
        assert first + rest == payload

        rsv = fp.RsvBits(False, False, False)
        rsv, first = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload[:10], False, True,
        )
        assert rsv.rsv1 is True

        rsv = fp.RsvBits(False, False, False)
        rsv, rest = ext.frame_outbound(
            proto, fp.Opcode.CONTINUATION, rsv, payload[10:], True, False,
        )
        assert rsv.rsv1 is False
        assert ext._outbound_compressed is None

    @pytest.mark.parametrize("client", [True, False])
    def test_uncompressed_empty_message(self, client: bool) -> None:
        ext = wpext.PerMessageDeflate()
        ext._enabled = True
        proto = fp.FrameProtocol(client=client, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.TEXT, rsv, b"", True, False,
        )
        assert rsv.rsv1 is False
        assert data == b""
        assert ext._compressor is None

    def test_control_frame_never_invokes_policy(self) -> None:
        def strategy(metadata: wpext.MessageMetadata) -> bool:
            raise AssertionError("strategy must not run for control frames")

        ext = wpext.PerMessageDeflate(compress=strategy)
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        rsv, data = ext.frame_outbound(
            proto, fp.Opcode.PING, rsv, b"x" * 23, True, None,
        )
        assert rsv.rsv1 is False
        assert data == b"x" * 23

    def test_strategy_receives_message_metadata(self) -> None:
        seen: list[wpext.MessageMetadata] = []

        def strategy(metadata: wpext.MessageMetadata) -> bool:
            seen.append(metadata)
            return False

        ext = wpext.PerMessageDeflate(compress=strategy)
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        ext.frame_outbound(proto, fp.Opcode.BINARY, rsv, b"abc", False, None)
        ext.frame_outbound(
            proto, fp.Opcode.CONTINUATION, rsv, b"def", True, None,
        )

        assert seen == [
            wpext.MessageMetadata(
                opcode=fp.Opcode.BINARY, payload_length=3, message_finished=False,
            ),
        ]

    def test_strategy_exception_before_any_bytes(self) -> None:
        def strategy(metadata: wpext.MessageMetadata) -> bool:
            raise RuntimeError("boom")

        ext = wpext.PerMessageDeflate(compress=strategy)
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])

        rsv = fp.RsvBits(False, False, False)
        with pytest.raises(RuntimeError, match="boom"):
            ext.frame_outbound(
                proto, fp.Opcode.BINARY, rsv, b"data", True, None,
            )
        # No partial message state leaked from the failed decision.
        assert ext._outbound_compressed is None
        assert ext._compressor is None

    @pytest.mark.parametrize("no_context_takeover", [False, True])
    def test_uncompressed_message_with_context_takeover(
        self, no_context_takeover: bool,
    ) -> None:
        ext = wpext.PerMessageDeflate(
            client_no_context_takeover=no_context_takeover,
            server_no_context_takeover=no_context_takeover,
        )
        ext._enabled = True
        proto = fp.FrameProtocol(client=True, extensions=[ext])
        payload = b"context takeover " * 8
        rsv = fp.RsvBits(False, False, False)

        rsv, first = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload, True, True,
        )
        rsv = fp.RsvBits(False, False, False)
        rsv, middle = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, b"ping", True, False,
        )
        assert middle == b"ping"
        rsv = fp.RsvBits(False, False, False)
        rsv, last = ext.frame_outbound(
            proto, fp.Opcode.BINARY, rsv, payload, True, True,
        )

        if no_context_takeover:
            assert last == first
        else:
            assert len(last) < len(first)


class RawFrame:
    """A header-level view of a single serialized WebSocket frame."""

    def __init__(
        self,
        first: int,
        payload: bytes,
    ) -> None:
        self.fin = bool(first & fp.FIN_MASK)
        self.rsv1 = bool(first & fp.RSV1_MASK)
        self.rsv2 = bool(first & fp.RSV2_MASK)
        self.rsv3 = bool(first & fp.RSV3_MASK)
        self.opcode = fp.Opcode(first & fp.OPCODE_MASK)
        self.payload = payload


def wire_frames(data: bytes) -> list[RawFrame]:
    """Split serialized output into raw frames, unmasking if needed."""
    frames = []
    rest = data
    while rest:
        first = rest[0]
        masked = bool(rest[1] & fp.MASK_MASK)
        payload_len = rest[1] & fp.PAYLOAD_LEN_MASK
        index = 2
        if payload_len == fp.PAYLOAD_LENGTH_TWO_BYTE:
            (payload_len,) = struct.unpack("!H", rest[2:4])
            index = 4
        elif payload_len == fp.PAYLOAD_LENGTH_EIGHT_BYTE:
            (payload_len,) = struct.unpack("!Q", rest[2:10])
            index = 10
        masking_key = b""
        if masked:
            masking_key = rest[index : index + 4]
            index += 4
        end = index + payload_len
        payload = bytes(rest[index:end])
        if masked:
            payload = bytes(
                byte ^ masking_key[i % 4] for i, byte in enumerate(payload)
            )
        frames.append(RawFrame(first, payload))
        rest = rest[end:]
    return frames


def negotiated_pair(
    client_ext: wpext.PerMessageDeflate | None = None,
    server_ext: wpext.PerMessageDeflate | None = None,
) -> tuple[Connection, Connection]:
    """Create connected in-memory client/server peers sharing one negotiation."""
    client_ext = client_ext or wpext.PerMessageDeflate()
    server_ext = server_ext or wpext.PerMessageDeflate()

    offer = f"{wpext.PerMessageDeflate.name}; {client_ext.offer()}"
    assert server_ext.accept(offer)
    client_ext.finalize(offer)

    assert client_ext.enabled()
    assert server_ext.enabled()
    return (
        Connection(CLIENT, [client_ext]),
        Connection(SERVER, [server_ext]),
    )


def test_connection_negotiation_unchanged() -> None:
    # The compression policy only affects outbound frames, not negotiation.
    client = wpext.PerMessageDeflate(compress=False)
    server = wpext.PerMessageDeflate()

    offer = client.offer()
    assert server.accept(offer) is not None
    client.finalize(offer)
    assert client.client_no_context_takeover is False
    assert client.server_no_context_takeover is False


@pytest.mark.parametrize("client_sends", [True, False])
@pytest.mark.parametrize(
    "message_type, payload",
    [
        (BytesMessage, b"heartbeat"),
        (TextMessage, "heartbeat"),
    ],
)
def test_peer_explicit_uncompressed(
    client_sends: bool, message_type: type, payload: bytes | str,
) -> None:
    client, server = negotiated_pair()
    local, remote = (client, server) if client_sends else (server, client)
    raw = local.send(message_type(data=payload, compress=False))

    frames = wire_frames(bytes(raw))
    assert len(frames) == 1
    assert frames[0].rsv1 is False
    assert frames[0].opcode in (fp.Opcode.TEXT, fp.Opcode.BINARY)
    assert frames[0].payload == (
        payload.encode("utf-8") if isinstance(payload, str) else payload
    )

    remote.receive_data(raw)
    event = next(remote.events())
    assert isinstance(event, message_type)
    assert event.data == payload


@pytest.mark.parametrize("client_sends", [True, False])
def test_peer_alternating_messages_fragments_and_ping(client_sends: bool) -> None:
    client, server = negotiated_pair()
    local, remote = (client, server) if client_sends else (server, client)

    # Small heartbeat: uncompressed, two fragments, with a PING in between.
    first = local.send(BytesMessage(data=b"heart", message_finished=False, compress=False))
    second = local.send(BytesMessage(data=b"beat", message_finished=False))
    ping = local.send(Ping(payload=b"alive"))
    third = local.send(BytesMessage(data=b"!", message_finished=True))

    # Large collection payload: compressed, three fragments, PONG interleaved.
    collection = b"a" * 4096
    chunks = (collection[:1024], collection[1024:3072], collection[3072:])
    c1 = local.send(BytesMessage(data=chunks[0], message_finished=False, compress=True))
    c2 = local.send(BytesMessage(data=chunks[1], message_finished=False))
    pong = local.send(Pong(payload=b"still here"))
    c3 = local.send(BytesMessage(data=chunks[2], message_finished=True))

    rsv1_flags = []
    opcodes = []
    for frame in wire_frames(b"".join((first, second, ping, third, c1, c2, pong, c3))):
        rsv1_flags.append(frame.rsv1)
        opcodes.append(frame.opcode)

    assert opcodes == [
        fp.Opcode.BINARY,
        fp.Opcode.CONTINUATION,
        fp.Opcode.PING,
        fp.Opcode.CONTINUATION,
        fp.Opcode.BINARY,
        fp.Opcode.CONTINUATION,
        fp.Opcode.PONG,
        fp.Opcode.CONTINUATION,
    ]
    # Only the first frame of each message carries RSV1, and only for the
    # compressed message. Continuations never repeat it.
    assert rsv1_flags == [
        False, False, False, False,
        True, False, False, False,
    ]

    remote.receive_data(b"".join((first, second, ping, third, c1, c2, pong, c3)))
    events = list(remote.events())
    assert [type(e) for e in events] == [
        BytesMessage,
        BytesMessage,
        Ping,
        BytesMessage,
        BytesMessage,
        BytesMessage,
        Pong,
        BytesMessage,
    ]
    assert events[0].data + events[1].data + events[3].data == b"heartbeat!"
    assert events[4].data + events[5].data + events[7].data == collection
    assert events[2].payload == b"alive"
    assert events[6].payload == b"still here"

    # The two messages use different RSV1 settings, so their wire payloads
    # differ in kind as well as content.
    assert events[0].message_finished is False
    assert events[3].message_finished is True
    assert events[5].message_finished is False
    assert events[7].message_finished is True


@pytest.mark.parametrize("client_sends", [True, False])
@pytest.mark.parametrize("message_type, payload", [(TextMessage, ""), (BytesMessage, b"")])
@pytest.mark.parametrize("compress", [False, True])
def test_peer_empty_message(
    client_sends: bool, message_type: type, payload: bytes | str, compress: bool,
) -> None:
    client, server = negotiated_pair()
    local, remote = (client, server) if client_sends else (server, client)
    raw = local.send(message_type(data=payload, compress=compress))

    remote.receive_data(raw)
    event = next(remote.events())
    assert isinstance(event, message_type)
    assert event.data == payload
    assert event.message_finished is True


@pytest.mark.parametrize("client_sends", [True, False])
def test_peer_consecutive_uncompressed_and_compressed(client_sends: bool) -> None:
    client, server = negotiated_pair()
    local, remote = (client, server) if client_sends else (server, client)

    small = b"s" * 8
    large = b"l" * 512
    data = b"".join(
        (
            local.send(BytesMessage(data=small, compress=False)),
            local.send(BytesMessage(data=large, compress=True)),
            local.send(BytesMessage(data=small, compress=False)),
            local.send(BytesMessage(data=large, compress=True)),
        ),
    )
    remote.receive_data(data)
    events = list(remote.events())
    assert [e.data for e in events] == [small, large, small, large]


@pytest.mark.parametrize("client_sends", [True, False])
@pytest.mark.parametrize("no_context_takeover", [False, True])
def test_peer_context_takeover(
    client_sends: bool, no_context_takeover: bool,
) -> None:
    client_ext = wpext.PerMessageDeflate(
        client_no_context_takeover=no_context_takeover,
        server_no_context_takeover=no_context_takeover,
    )
    client, server = negotiated_pair(client_ext)
    local, remote = (client, server) if client_sends else (server, client)
    payload = b"repeatable context " * 16

    frames = [
        local.send(BytesMessage(data=payload, compress=True)),
        local.send(BytesMessage(data=b"heartbeat", compress=False)),
        local.send(BytesMessage(data=payload, compress=True)),
    ]
    remote.receive_data(b"".join(frames))
    events = list(remote.events())
    assert [e.data for e in events] == [payload, b"heartbeat", payload]

    # Per-message compressor state only persists when context takeover is on.
    if client_sends:
        ext = client_ext
    else:
        ext = server._proto.extensions[0]
    if no_context_takeover:
        assert ext._compressor is None
    else:
        assert ext._compressor is not None


def test_strategy_decides_from_first_chunk() -> None:
    seen: list[wpext.MessageMetadata] = []

    def strategy(metadata: wpext.MessageMetadata) -> bool:
        seen.append(metadata)
        return metadata.payload_length > 64

    client_ext = wpext.PerMessageDeflate(compress=strategy)
    client, server = negotiated_pair(client_ext)

    wire = client.send(BytesMessage(data=b"tiny", compress=None))
    assert len(wire) > 0
    wire += client.send(
        BytesMessage(data=b"y" * 100, message_finished=False),
    )
    wire += client.send(BytesMessage(data=b"z" * 20))
    server.receive_data(wire)
    events = list(server.events())
    assert events[0].data == b"tiny"
    assert events[1].data + events[2].data == b"y" * 100 + b"z" * 20

    assert seen == [
        wpext.MessageMetadata(
            opcode=fp.Opcode.BINARY, payload_length=4, message_finished=True,
        ),
        wpext.MessageMetadata(
            opcode=fp.Opcode.BINARY, payload_length=100, message_finished=False,
        ),
    ]


def test_explicit_hint_overrides_strategy() -> None:
    def strategy(metadata: wpext.MessageMetadata) -> bool:
        return True

    client_ext = wpext.PerMessageDeflate(compress=strategy)
    client, server = negotiated_pair(client_ext)

    wire = client.send(BytesMessage(data=b"x" * 200, compress=False))
    server.receive_data(wire)
    assert next(server.events()).data == b"x" * 200

    assert client_ext._compressor is None


def test_strategy_error_before_message_bytes() -> None:
    class Boom(Exception):
        pass

    def strategy(metadata: wpext.MessageMetadata) -> bool:
        raise Boom

    client_ext = wpext.PerMessageDeflate(compress=strategy)
    client, server = negotiated_pair(client_ext)

    with pytest.raises(Boom):
        client.send(BytesMessage(data=b"never sent"))

    # The failed decision did not emit any frame, nor mutate wire state.
    assert client_ext._outbound_compressed is None
    assert client_ext._compressor is None

    # A following message with an explicit decision still works.
    wire = client.send(BytesMessage(data=b"sent", compress=False))
    server.receive_data(wire)
    assert next(server.events()).data == b"sent"
