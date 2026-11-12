from __future__ import annotations

import zlib
from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional, cast

import pytest

from wsproto import extensions as wpext
from wsproto import frame_protocol as fp
from wsproto.extensions import ExtensionOffer
from wsproto.utilities import RemoteProtocolError

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
        {"client_max_window_bits": None},
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

    def test_finalize_rejects_rubbish(self) -> None:
        ext = wpext.PerMessageDeflate()
        assert not ext.enabled()

        with pytest.raises(RemoteProtocolError):
            ext.finalize("i am the lizard queen; worship me")

        assert not ext.enabled()

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

    def test_accept_rejects_rubbish(self) -> None:
        ext = wpext.PerMessageDeflate()
        assert not ext.enabled()

        response = ext.accept("i am the lizard queen; worship me")

        assert response is None
        assert not ext.enabled()

    @pytest.mark.parametrize(
        "offer",
        [
            "permessage-deflate; unknown_parameter",
            "permessage-deflate; server_max_window_bits=10; server_max_window_bits=11",
            "permessage-deflate; server_max_window_bits=99",
            "permessage-deflate; server_max_window_bits=abc",
            "permessage-deflate; client_no_context_takeover=true",
            "i am the lizard queen; worship me",
        ],
    )
    def test_parse_rejects_invalid_offers(self, offer: str) -> None:
        ext = wpext.PerMessageDeflate()
        assert ext.parse_offer(ExtensionOffer(ext.name, offer)) is None
        assert ext.accept(offer) is None
        assert not ext.enabled()
        assert ext.server_max_window_bits == ext.DEFAULT_SERVER_MAX_WINDOW_BITS
        assert ext.client_max_window_bits == ext.DEFAULT_CLIENT_MAX_WINDOW_BITS

    def test_accept_rejects_bare_server_window_bits(self) -> None:
        ext = wpext.PerMessageDeflate()
        offer = ext.parse_offer(
            ExtensionOffer(ext.name, "permessage-deflate; server_max_window_bits"),
        )
        assert offer is not None
        assert ext.can_accept(offer) is None
        assert not ext.enabled()

    def test_accept_plain_offer(self) -> None:
        ext = wpext.PerMessageDeflate()
        offer = ext.parse_offer(ExtensionOffer(ext.name, "permessage-deflate"))
        assert offer is not None
        assert ext.can_accept(offer) == ""
        assert not ext.enabled()

    def test_legacy_methods_accept_parameter_only_strings(self) -> None:
        client_ext = wpext.PerMessageDeflate()
        client_ext.finalize(client_ext.offer())
        assert client_ext.enabled()

        server_ext = wpext.PerMessageDeflate()
        response = server_ext.accept(server_ext.offer())
        assert response == "client_max_window_bits=15; server_max_window_bits=15"
        assert server_ext.enabled()

    def test_failed_candidate_leaves_no_state(self) -> None:
        ext = wpext.PerMessageDeflate()
        bad = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits=9; server_max_window_bits=99",
            ),
        )
        assert bad is None
        good = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits=9; server_max_window_bits=10",
            ),
        )
        assert good is not None
        assert ext.can_accept(good) == (
            "client_max_window_bits=9; server_max_window_bits=10"
        )
        # Judging an offer does not establish anything.
        assert not ext.enabled()
        assert ext.client_max_window_bits == ext.DEFAULT_CLIENT_MAX_WINDOW_BITS
        assert ext.server_max_window_bits == ext.DEFAULT_SERVER_MAX_WINDOW_BITS
        assert ext._compressor is None
        assert ext._decompressor is None

    def test_accept_picks_within_offered_window_bits(self) -> None:
        ext = wpext.PerMessageDeflate(client_max_window_bits=12, server_max_window_bits=11)
        offer = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits=10; server_max_window_bits=14",
            ),
        )
        assert offer is not None
        assert ext.can_accept(offer) == (
            "client_max_window_bits=10; server_max_window_bits=11"
        )

    def test_accept_rejects_offer_without_client_window_bits_support(self) -> None:
        ext = wpext.PerMessageDeflate(client_max_window_bits=12)
        offer = ext.parse_offer(ExtensionOffer(ext.name, "permessage-deflate"))
        assert offer is not None
        # A response must not contain client_max_window_bits if the offer
        # didn't, so a server requiring it cannot accept this offer.
        assert ext.can_accept(offer) is None

    def test_accept_bare_client_window_bits(self) -> None:
        ext = wpext.PerMessageDeflate(client_max_window_bits=12)
        offer = ext.parse_offer(
            ExtensionOffer(ext.name, "permessage-deflate; client_max_window_bits"),
        )
        assert offer is not None
        assert offer.client_max_window_bits_bare
        assert ext.can_accept(offer) == "client_max_window_bits=12"

    def test_client_finalize_rejects_unoffered_client_window_bits(self) -> None:
        ext = wpext.PerMessageDeflate()
        # A bare client_max_window_bits is only legal in an offer, never in a
        # response, and an offer that did not include the parameter cannot be
        # answered with one.
        response = ext.parse_offer(
            ExtensionOffer(ext.name, "permessage-deflate; client_max_window_bits"),
        )
        assert response is not None
        assert not ext.can_finalize(response)
        with pytest.raises(RemoteProtocolError):
            ext.finalize("permessage-deflate; client_max_window_bits")
        assert not ext.enabled()

    def test_client_finalize_rejects_excessive_window_bits(self) -> None:
        ext = wpext.PerMessageDeflate(client_max_window_bits=10, server_max_window_bits=10)
        with pytest.raises(RemoteProtocolError):
            ext.finalize("permessage-deflate; server_max_window_bits=12")
        assert not ext.enabled()

    def test_establish_creates_independent_runtime(self) -> None:
        ext = wpext.PerMessageDeflate()
        offer = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits=11; server_max_window_bits=12",
            ),
        )
        assert offer is not None
        ext.establish(offer)
        assert ext.enabled()

        clone = ext.copy()
        assert clone is not ext
        assert clone.enabled()
        assert clone.client_max_window_bits == 11
        assert clone.server_max_window_bits == 12
        assert clone._compressor is None
        assert clone._decompressor is None

        # Using the clone must not create a compressor on the original.
        proto = fp.FrameProtocol(client=True, extensions=[clone])
        rsv = fp.RsvBits(False, False, False)
        clone.frame_outbound(proto, fp.Opcode.TEXT, rsv, b"hello world", True)
        assert clone._compressor is not None
        assert ext._compressor is None

    def test_reserved_bits(self) -> None:
        assert wpext.PerMessageDeflate().reserved_bits() == fp.RsvBits(True, False, False)

    def test_response_allowed_against_offer(self) -> None:
        ext = wpext.PerMessageDeflate()
        offered = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits; server_max_window_bits=12",
            ),
        )
        allowed = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits=10; server_max_window_bits=11",
            ),
        )
        not_allowed = ext.parse_offer(
            ExtensionOffer(
                ext.name,
                "permessage-deflate; client_max_window_bits=10; server_max_window_bits=13",
            ),
        )
        assert offered is not None
        assert allowed is not None
        assert not_allowed is not None
        assert ext.response_allowed(allowed, offered)
        assert not ext.response_allowed(not_allowed, offered)

    def test_response_allowed_rejects_unoffered_client_window_bits(self) -> None:
        ext = wpext.PerMessageDeflate()
        offered = ext.parse_offer(
            ExtensionOffer(ext.name, "permessage-deflate; server_max_window_bits=12"),
        )
        response = ext.parse_offer(
            ExtensionOffer(ext.name, "permessage-deflate; client_max_window_bits=10"),
        )
        assert offered is not None
        assert response is not None
        assert not ext.response_allowed(response, offered)

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
