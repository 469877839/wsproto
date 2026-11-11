"""
UTF-8 consistency tests for text message delivery.

The expected decoded chunks are produced with the standard library
incremental UTF-8 decoder, while message boundaries, control frames, binary
messages and close codes are handled independently of that decoder. Frames
are built by hand (unmasked, as received by a client) and may be fed to the
connection in arbitrary byte-sized chunks.
"""
from __future__ import annotations

import itertools
import struct
import zlib
from codecs import getincrementaldecoder
from typing import TYPE_CHECKING

import pytest

from wsproto import extensions as wpext
from wsproto import frame_protocol as fp
from wsproto.connection import CLIENT, Connection
from wsproto.events import (
    BytesMessage,
    CloseConnection,
    Event,
    Ping,
    Pong,
    TextMessage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

# Short texts covering every UTF-8 length class, kept deliberately small so
# every segmentation can be enumerated.
SHORT_TEXTS = (
    "a",
    "ab",
    "é",
    "aé",
    "€",
    "a€b",
    "😀",
    "a😀b",
    "é€😀",
    "😀😀",
)

# Fixed invalid UTF-8 samples rather than generated fuzz inputs, spanning the
# failure classes the incremental decoder rejects.
BAD_UTF8_SAMPLES: tuple[tuple[str, bytes], ...] = (
    ("lone continuation byte", b"\x80"),
    ("incomplete two byte sequence", b"\xc2"),
    ("incomplete three byte sequence", b"\xe2\x82"),
    ("incomplete four byte sequence", b"\xf0\x9f\x98"),
    ("overlong two byte encoding of slash", b"\xc0\xaf"),
    ("overlong three byte encoding of slash", b"\xe0\x80\xaf"),
    ("utf-16 surrogate", b"\xed\xa0\x80"),
    ("code point beyond U+10FFFF", b"\xf4\x90\x80\x80"),
    ("valid prefix then lone continuation", b"a\x80b"),
)


def make_frame(
    opcode: fp.Opcode, payload: bytes = b"", *, fin: bool = True, rsv1: bool = False,
) -> bytes:
    """Serialize an unmasked frame, as received by a client."""
    first_byte = int(opcode)
    if fin:
        first_byte |= 0x80
    if rsv1:
        first_byte |= 0x40
    length = len(payload)
    if length <= fp.MAX_PAYLOAD_NORMAL:
        header = bytes([first_byte, length])
    elif length <= fp.MAX_PAYLOAD_TWO_BYTE:
        header = bytes([first_byte, fp.PAYLOAD_LENGTH_TWO_BYTE]) + struct.pack(
            "!H", length,
        )
    else:
        header = bytes([first_byte, fp.PAYLOAD_LENGTH_EIGHT_BYTE]) + struct.pack(
            "!Q", length,
        )
    return header + payload


def make_fragmented_frames(
    payload: bytes,
    split_points: Sequence[int],
    *,
    start_opcode: fp.Opcode = fp.Opcode.TEXT,
    rsv1: bool = False,
) -> bytes:
    """Split payload at byte offsets into one start frame and continuations."""
    boundaries = (0, *split_points, len(payload))
    chunks = [payload[left:right] for left, right in zip(boundaries, boundaries[1:])]
    wire = b""
    for index, chunk in enumerate(chunks):
        opcode = start_opcode if index == 0 else fp.Opcode.CONTINUATION
        wire += make_frame(
            opcode, chunk, fin=index == len(chunks) - 1, rsv1=rsv1 and index == 0,
        )
    return wire


def message_frames(
    payload: bytes,
    split_points: Sequence[int],
    *,
    start_opcode: fp.Opcode = fp.Opcode.TEXT,
) -> list[tuple[fp.Opcode, bytes, bool]]:
    """Describe the same fragmentation as (opcode, chunk, message_finished)."""
    boundaries = (0, *split_points, len(payload))
    return [
        (
            start_opcode if index == 0 else fp.Opcode.CONTINUATION,
            payload[left:right],
            index == len(boundaries) - 2,
        )
        for index, (left, right) in enumerate(zip(boundaries, boundaries[1:]))
    ]


def collect_events(
    wire: bytes,
    *,
    chunk_size: int | None = None,
    extensions: list[wpext.Extension] | None = None,
) -> list[Event]:
    """Feed wire bytes to a fresh client connection and drain its events."""
    connection = Connection(CLIENT, extensions=extensions)
    events: list[Event] = []
    if chunk_size is None:
        connection.receive_data(wire)
    else:
        for offset in range(0, len(wire), chunk_size):
            connection.receive_data(wire[offset : offset + chunk_size])
    events.extend(connection.events())
    return events


def oracle_text_chunks(
    frames: Sequence[tuple[fp.Opcode, bytes, bool]],
) -> list[str]:
    """Reference decoder output for the TEXT frames of a frame sequence.

    A fresh stdlib incremental decoder is created for every text message and
    reset when the message finishes. Binary frames and control frames are not
    part of the text byte stream, so message boundaries and control frame
    rules remain independent of the reference decoder.
    """
    chunks: list[str] = []
    decoder = None
    for opcode, payload, message_finished in frames:
        if opcode is fp.Opcode.TEXT:
            decoder = getincrementaldecoder("utf-8")()
            chunks.append(decoder.decode(payload, message_finished))
        elif opcode is fp.Opcode.CONTINUATION:
            assert decoder is not None
            chunks.append(decoder.decode(payload, message_finished))
        if message_finished:
            decoder = None
    return chunks


def assert_text_events_match(
    events: Sequence[Event],
    expected_chunks: Sequence[str],
    *,
    frame_finished: bool = True,
) -> None:
    """Check TextMessage content, finish markers and message reassembly."""
    text_events = [event for event in events if isinstance(event, TextMessage)]
    assert len(text_events) == len(expected_chunks)
    for event, expected in zip(text_events, expected_chunks):
        assert event.data == expected
        assert event.frame_finished is frame_finished
    for event in text_events[:-1]:
        assert event.message_finished is False
    assert text_events[-1].message_finished is True
    assert "".join(event.data for event in text_events) == "".join(expected_chunks)


def all_segmentations(length: int) -> list[tuple[int, ...]]:
    """Enumerate every ordered way to cut a byte string of this length."""
    positions = range(1, length)
    return [
        tuple(combo)
        for size in range(length)
        for combo in itertools.combinations(positions, size)
    ]


FRAGMENTATION_CASES = [
    pytest.param(
        text,
        split_points,
        id=f"{index}-{text.encode('utf-8').hex()}-{split_points}",
    )
    for index, text in enumerate(SHORT_TEXTS)
    for split_points in all_segmentations(len(text.encode("utf-8")))
]


class TestExhaustiveFragmentation:
    """Every segmentation of every short text, delivered over every chunk size."""

    @pytest.mark.parametrize("text,split_points", FRAGMENTATION_CASES)
    @pytest.mark.parametrize("chunk_size", [None, 1, 2, 3, 7])
    def test_all_splits(
        self, text: str, split_points: tuple[int, ...], chunk_size: int | None,
    ) -> None:
        payload = text.encode("utf-8")
        wire = make_fragmented_frames(payload, split_points)
        events = collect_events(wire, chunk_size=chunk_size)

        expected_chunks = oracle_text_chunks(message_frames(payload, split_points))

        assert_text_events_match(events, expected_chunks)

    def test_total_case_count(self) -> None:
        # Guard against accidentally shrinking the enumeration silently.
        counts = [
            len(all_segmentations(len(text.encode("utf-8")))) for text in SHORT_TEXTS
        ]
        assert counts == [1, 2, 2, 4, 4, 16, 8, 32, 256, 128]
        assert sum(counts) == 453


class TestCharacterSplitAcrossFrames:
    """A single multibyte character torn across several continuation frames."""

    @pytest.mark.parametrize(
        "text,splits",
        [
            ("😀", (1, 2, 3)),
            ("€", (1, 2)),
            ("a😀b", (1, 2, 3, 4)),
            ("é€😀", (1, 3, 4, 5, 6)),
        ],
    )
    @pytest.mark.parametrize("chunk_size", [None, 1, 3])
    def test_partial_character_per_frame(
        self, text: str, splits: tuple[int, ...], chunk_size: int | None,
    ) -> None:
        payload = text.encode("utf-8")
        wire = make_fragmented_frames(payload, splits)
        events = collect_events(wire, chunk_size=chunk_size)

        expected_chunks = oracle_text_chunks(message_frames(payload, splits))

        assert_text_events_match(events, expected_chunks)
        assert "".join(
            event.data for event in events if isinstance(event, TextMessage)
        ) == text

    def test_multibyte_character_emits_empty_intermediate_chunks(self) -> None:
        # Four-byte U+1F600 delivered one byte per frame: only the last frame
        # contains a decoded character, the others deliver empty strings.
        payload = "😀".encode("utf-8")
        wire = make_fragmented_frames(payload, (1, 2, 3))
        events = collect_events(wire)
        text_events = [event for event in events if isinstance(event, TextMessage)]

        assert [event.data for event in text_events] == ["", "", "", "😀"]
        assert [event.message_finished for event in text_events] == [
            False,
            False,
            False,
            True,
        ]
        assert all(event.frame_finished for event in text_events)


class TestPartialFrameDelivery:
    """A frame arriving over several byte chunks surfaces intermediate events
    with frame_finished=False until the frame is complete."""

    @staticmethod
    def feed_steps(wire: bytes, step: int) -> list[list[Event]]:
        connection = Connection(CLIENT)
        batches = []
        for offset in range(0, len(wire), step):
            connection.receive_data(wire[offset : offset + step])
            batches.append(list(connection.events()))
        return batches

    @pytest.mark.parametrize("step", [1, 2, 3])
    def test_single_frame_split_into_byte_chunks(self, step: int) -> None:
        text = "aé😀b"
        wire = make_frame(fp.Opcode.TEXT, text.encode())
        batches = self.feed_steps(wire, step)

        text_events = [
            event
            for batch in batches
            for event in batch
            if isinstance(event, TextMessage)
        ]
        assert "".join(event.data for event in text_events) == text
        assert all(event.frame_finished is False for event in text_events[:-1])
        assert text_events[-1].frame_finished is True
        assert all(event.message_finished is False for event in text_events[:-1])
        assert text_events[-1].message_finished is True

    @pytest.mark.parametrize("step", [1, 2])
    def test_fragmented_message_with_partial_frames(self, step: int) -> None:
        text = "é€😀"
        payload = text.encode()
        wire = (
            make_frame(fp.Opcode.TEXT, payload[:3], fin=False)
            + make_frame(fp.Opcode.CONTINUATION, payload[3:], fin=True)
        )
        batches = self.feed_steps(wire, step)

        text_events = [
            event
            for batch in batches
            for event in batch
            if isinstance(event, TextMessage)
        ]
        assert "".join(event.data for event in text_events) == text
        assert [event.frame_finished for event in text_events].count(True) == 2
        assert [event.message_finished for event in text_events].count(True) == 1
        assert text_events[-1].frame_finished is True
        assert text_events[-1].message_finished is True


class TestControlFramesInsideFragmentedText:
    """Control frames may arrive between fragments and must not disturb the
    text byte stream."""

    @pytest.mark.parametrize("chunk_size", [None, 1, 5])
    def test_ping_and_pong_between_fragments(self, chunk_size: int | None) -> None:
        text = "héllo😀"
        payload = text.encode("utf-8")
        wire = (
            make_frame(fp.Opcode.TEXT, payload[:4], fin=False)
            + make_frame(fp.Opcode.PING, b"ping-payload")
            + make_frame(fp.Opcode.CONTINUATION, payload[4:6], fin=False)
            + make_frame(fp.Opcode.PONG, b"pong-payload")
            + make_frame(fp.Opcode.CONTINUATION, payload[6:], fin=True)
        )
        events = collect_events(wire, chunk_size=chunk_size)

        pings = [event for event in events if isinstance(event, Ping)]
        pongs = [event for event in events if isinstance(event, Pong)]
        assert [event.payload for event in pings] == [b"ping-payload"]
        assert [event.payload for event in pongs] == [b"pong-payload"]

        frames = [
            (fp.Opcode.TEXT, payload[:4], False),
            (fp.Opcode.CONTINUATION, payload[4:6], False),
            (fp.Opcode.CONTINUATION, payload[6:], True),
        ]
        assert_text_events_match(events, oracle_text_chunks(frames))
        assert not any(isinstance(event, CloseConnection) for event in events)

    def test_control_frame_inside_split_character(self) -> None:
        # The ping arrives in the middle of U+1F600's four bytes.
        payload = "😀".encode("utf-8")
        wire = (
            make_frame(fp.Opcode.TEXT, payload[:2], fin=False)
            + make_frame(fp.Opcode.PING, b"!")
            + make_frame(fp.Opcode.CONTINUATION, payload[2:], fin=True)
        )
        events = collect_events(wire)
        text_events = [event for event in events if isinstance(event, TextMessage)]

        assert [event.data for event in text_events] == ["", "😀"]
        assert [event.message_finished for event in text_events] == [False, True]

    def test_close_between_fragments(self) -> None:
        wire = (
            make_frame(fp.Opcode.TEXT, "é".encode()[:1], fin=False)
            + make_frame(
                fp.Opcode.CLOSE,
                struct.pack("!H", fp.CloseReason.NORMAL_CLOSURE) + "bye".encode(),
            )
        )
        events = collect_events(wire)
        text_events = [event for event in events if isinstance(event, TextMessage)]
        closes = [event for event in events if isinstance(event, CloseConnection)]

        assert [event.data for event in text_events] == [""]
        assert [event.message_finished for event in text_events] == [False]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.NORMAL_CLOSURE
        assert closes[0].reason == "bye"


class TestInvalidUtf8CloseCodes:
    """Invalid text input must close with INVALID_FRAME_PAYLOAD_DATA (1007)."""

    @pytest.mark.parametrize(
        "description,payload",
        BAD_UTF8_SAMPLES,
        ids=[description for description, _ in BAD_UTF8_SAMPLES],
    )
    @pytest.mark.parametrize("chunk_size", [None, 1])
    def test_bad_utf8_closes_with_1007(
        self, description: str, payload: bytes, chunk_size: int | None,
    ) -> None:
        wire = make_frame(fp.Opcode.TEXT, payload)
        events = collect_events(wire, chunk_size=chunk_size)

        assert not any(isinstance(event, TextMessage) for event in events)
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    @pytest.mark.parametrize(
        "description,payload",
        BAD_UTF8_SAMPLES,
        ids=[description for description, _ in BAD_UTF8_SAMPLES],
    )
    def test_bad_utf8_in_continuation_frame(
        self, description: str, payload: bytes,
    ) -> None:
        # The bad bytes arrive in a later fragment, after a valid start frame.
        wire = (
            make_frame(fp.Opcode.TEXT, "ok".encode(), fin=False)
            + make_frame(fp.Opcode.CONTINUATION, payload, fin=True)
        )
        events = collect_events(wire)

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == ["ok"]
        assert [event.message_finished for event in text_events] == [False]
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    @pytest.mark.parametrize("chunk_size", [None, 1, 2])
    def test_final_frame_leaves_incomplete_character(
        self, chunk_size: int | None,
    ) -> None:
        # "é" is 0xC3 0xA9; the message ends after the lead byte.
        payload = "café".encode("utf-8")
        wire = (
            make_frame(fp.Opcode.TEXT, payload[:3], fin=False)
            + make_frame(fp.Opcode.CONTINUATION, payload[3:4], fin=True)
        )
        events = collect_events(wire, chunk_size=chunk_size)

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == ["caf"]
        assert [event.message_finished for event in text_events] == [False]
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    def test_incomplete_character_in_single_frame_message(self) -> None:
        wire = make_frame(fp.Opcode.TEXT, "é".encode()[:1])
        events = collect_events(wire)

        assert not any(isinstance(event, TextMessage) for event in events)
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA


class TestContinuationProtocolErrors:
    """Illegal continuation frames are protocol errors (1002), not UTF-8
    errors."""

    def test_continuation_without_message(self) -> None:
        wire = make_frame(fp.Opcode.CONTINUATION, b"hello")
        events = collect_events(wire)

        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.PROTOCOL_ERROR

    def test_continuation_after_completed_message(self) -> None:
        wire = (
            make_frame(fp.Opcode.TEXT, "done".encode())
            + make_frame(fp.Opcode.CONTINUATION, b"extra")
        )
        events = collect_events(wire)

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == ["done"]
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.PROTOCOL_ERROR

    def test_new_text_frame_inside_fragmented_message(self) -> None:
        wire = (
            make_frame(fp.Opcode.TEXT, "un".encode(), fin=False)
            + make_frame(fp.Opcode.TEXT, "finished".encode())
        )
        events = collect_events(wire)

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == ["un"]
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.PROTOCOL_ERROR


class TestDecoderStateIsolation:
    """Leftover decode state must never leak from one message into the next."""

    def test_message_decoder_resets_between_messages(self) -> None:
        decoder = fp.MessageDecoder()

        first = fp.Frame(
            opcode=fp.Opcode.TEXT,
            payload="héllo".encode(),
            frame_finished=True,
            message_finished=True,
        )
        frame = decoder.process_frame(first)
        assert frame.payload == "héllo"
        assert frame.message_finished is True
        assert decoder.decoder is None
        assert decoder.opcode is None

        # A lone continuation byte would decode fine only if the previous
        # message's decoder state were still attached.
        second = fp.Frame(
            opcode=fp.Opcode.TEXT,
            payload=b"\x80",
            frame_finished=True,
            message_finished=True,
        )
        with pytest.raises(fp.ParseFailed) as excinfo:
            decoder.process_frame(second)
        assert excinfo.value.code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    def test_fresh_message_after_split_character_message(self) -> None:
        first_text = "😀😀"
        second_text = "é€😀"
        wire = (
            make_fragmented_frames(first_text.encode(), (1, 2, 3, 5, 6, 7))
            + make_fragmented_frames(second_text.encode(), (1, 3, 4, 5, 6))
        )
        events = collect_events(wire, chunk_size=1)

        first_frames = message_frames(first_text.encode(), (1, 2, 3, 5, 6, 7))
        second_frames = message_frames(second_text.encode(), (1, 3, 4, 5, 6))
        expected = oracle_text_chunks(first_frames + second_frames)
        text_events = [event for event in events if isinstance(event, TextMessage)]

        assert [event.data for event in text_events] == expected
        assert [event.message_finished for event in text_events] == (
            [False] * 6 + [True] + [False] * 5 + [True]
        )
        assert "".join(event.data for event in text_events) == first_text + second_text

    def test_new_message_starting_with_continuation_byte_fails(self) -> None:
        # After a clean message, a fresh message starting with a bare
        # continuation byte must be rejected with 1007.
        wire = (
            make_frame(fp.Opcode.TEXT, "é".encode())
            + make_frame(fp.Opcode.TEXT, b"\x80")
        )
        events = collect_events(wire)

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == ["é"]
        assert [event.message_finished for event in text_events] == [True]
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA


class TestBinaryMessagesInterspersed:
    """Binary messages between text messages must not disturb text decoding,
    and binary payloads are never passed through the UTF-8 decoder."""

    @pytest.mark.parametrize("chunk_size", [None, 1, 4])
    def test_binary_between_text_messages(self, chunk_size: int | None) -> None:
        first_text = "héllo😀"
        second_text = "é€😀"
        binary_payload = b"\x80\xff\xc2\xfe"  # invalid UTF-8, legal binary
        wire = (
            make_fragmented_frames(first_text.encode(), (2, 5))
            + make_frame(fp.Opcode.BINARY, binary_payload)
            + make_fragmented_frames(second_text.encode(), (1, 4))
        )
        events = collect_events(wire, chunk_size=chunk_size)

        expected_text = oracle_text_chunks(
            message_frames(first_text.encode(), (2, 5))
            + message_frames(second_text.encode(), (1, 4)),
        )
        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == expected_text
        assert [event.message_finished for event in text_events] == (
            [False, False, True] + [False, False, True]
        )

        binary_events = [event for event in events if isinstance(event, BytesMessage)]
        assert [event.data for event in binary_events] == [binary_payload]
        assert [event.message_finished for event in binary_events] == [True]

        assert not any(isinstance(event, CloseConnection) for event in events)

    def test_fragmented_binary_between_fragmented_text(self) -> None:
        text = "€😀"
        binary_payload = b"\x00\x01\x02\x03\x04"
        wire = (
            make_fragmented_frames(text.encode(), (2, 3, 4))
            + make_fragmented_frames(
                binary_payload, (2,), start_opcode=fp.Opcode.BINARY,
            )
            + make_frame(fp.Opcode.TEXT, "é".encode())
        )
        events = collect_events(wire, chunk_size=1)

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert "".join(event.data for event in text_events) == "€😀é"
        assert [event.message_finished for event in text_events] == (
            [False, False, False, True] + [True]
        )

        binary_events = [event for event in events if isinstance(event, BytesMessage)]
        assert [event.data for event in binary_events] == [b"\x00\x01", b"\x02\x03\x04"]
        assert [event.message_finished for event in binary_events] == [False, True]

        assert not any(isinstance(event, CloseConnection) for event in events)


def compress_payload(payload: bytes) -> bytes:
    """Compress like permessage-deflate: raw deflate, sync flush, tail cut."""
    compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15)
    compressed = compressor.compress(payload) + compressor.flush(zlib.Z_SYNC_FLUSH)
    return compressed[:-4]


def deflate_split_ending_mid_character(compressed: bytes) -> int:
    """Find a compressed-stream split whose first frame decodes to a prefix
    that ends in the middle of a UTF-8 character."""
    for split in range(1, len(compressed)):
        decompressor = zlib.decompressobj(-15)
        prefix = decompressor.decompress(compressed[:split])
        if not prefix:
            continue
        try:
            prefix.decode("utf-8")
        except UnicodeDecodeError as exc:
            if exc.reason == "unexpected end of data":
                return split
    msg = "no mid-character split found"
    raise AssertionError(msg)


class TestCompressedText:
    """UTF-8 consistency through permessage-deflate compressed messages."""

    @staticmethod
    def enabled_extension(**kwargs: bool) -> wpext.PerMessageDeflate:
        extension = wpext.PerMessageDeflate(**kwargs)
        extension._enabled = True
        return extension

    @pytest.mark.parametrize("chunk_size", [None, 1, 3])
    def test_compressed_fragmented_text(self, chunk_size: int | None) -> None:
        text = "héllo😀 wörld" * 3
        compressed = compress_payload(text.encode())
        wire = make_fragmented_frames(
            compressed, (len(compressed) // 2,), rsv1=True,
        )
        events = collect_events(
            wire, chunk_size=chunk_size, extensions=[self.enabled_extension()],
        )

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert "".join(event.data for event in text_events) == text
        assert [event.message_finished for event in text_events] == [False, True]
        assert all(event.frame_finished for event in text_events)
        assert not any(isinstance(event, CloseConnection) for event in events)

    def test_compressed_split_ends_mid_character(self) -> None:
        text = "😀" * 16
        payload = text.encode()
        compressed = compress_payload(payload)
        split = deflate_split_ending_mid_character(compressed)

        # Independent reference: what the decompressed prefix should decode to.
        decompressor = zlib.decompressobj(-15)
        first_decompressed = decompressor.decompress(compressed[:split])
        with pytest.raises(UnicodeDecodeError):
            first_decompressed.decode("utf-8")
        first_text = getincrementaldecoder("utf-8")().decode(
            first_decompressed, False,
        )

        wire = make_fragmented_frames(compressed, (split,), rsv1=True)
        events = collect_events(wire, extensions=[self.enabled_extension()])

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.message_finished for event in text_events] == [False, True]
        assert text_events[0].data == first_text
        assert "".join(event.data for event in text_events) == text

    def test_two_compressed_messages(self) -> None:
        texts = ["first message héllo", "second message 😀😀"]
        wire = b"".join(
            make_frame(fp.Opcode.TEXT, compress_payload(text.encode()), rsv1=True)
            for text in texts
        )
        events = collect_events(wire, extensions=[self.enabled_extension()])

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == texts
        assert all(event.message_finished for event in text_events)
        assert not any(isinstance(event, CloseConnection) for event in events)

    def test_two_compressed_messages_no_context_takeover(self) -> None:
        texts = ["repeat repeat repeat", "repeat repeat repeat"]
        wire = b"".join(
            make_frame(fp.Opcode.TEXT, compress_payload(text.encode()), rsv1=True)
            for text in texts
        )
        events = collect_events(
            wire,
            extensions=[self.enabled_extension(server_no_context_takeover=True)],
        )

        text_events = [event for event in events if isinstance(event, TextMessage)]
        assert [event.data for event in text_events] == texts
        assert not any(isinstance(event, CloseConnection) for event in events)

    def test_compressed_invalid_utf8_closes_with_1007(self) -> None:
        wire = make_frame(fp.Opcode.TEXT, compress_payload(b"\xc2"), rsv1=True)
        events = collect_events(wire, extensions=[self.enabled_extension()])

        assert not any(isinstance(event, TextMessage) for event in events)
        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    def test_invalid_zlib_payload_closes_with_1007(self) -> None:
        wire = make_frame(fp.Opcode.TEXT, b"not-deflate-data", rsv1=True)
        events = collect_events(wire, extensions=[self.enabled_extension()])

        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.INVALID_FRAME_PAYLOAD_DATA

    def test_rsv1_on_continuation_closes_with_1002(self) -> None:
        compressed = compress_payload("héllo".encode())
        wire = (
            make_frame(fp.Opcode.TEXT, compressed[:2], fin=False, rsv1=True)
            + make_frame(fp.Opcode.CONTINUATION, compressed[2:], rsv1=True)
        )
        events = collect_events(wire, extensions=[self.enabled_extension()])

        closes = [event for event in events if isinstance(event, CloseConnection)]
        assert len(closes) == 1
        assert closes[0].code is fp.CloseReason.PROTOCOL_ERROR
