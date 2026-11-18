import pytest

from minigrid import envelope


def test_encode_decode_roundtrip():
    raw = envelope.encode("t1", "pi", "def step(s, d, a): pass", b"\x00\x01", {"steps": "20"}, [])
    out = envelope.decode(raw)
    assert out["task_id"] == "t1"
    assert out["recipe"] == "pi"
    assert out["data"] == b"\x00\x01"
    assert out["params"]["steps"] == "20"


def test_encode_rejects_oversized_payload():
    with pytest.raises(envelope.PayloadTooLarge):
        envelope.encode("t1", "pi", "", b"x" * (envelope.MAX_PAYLOAD + 1), {}, [])


def test_encode_accepts_payload_exactly_at_the_limit():
    raw = envelope.encode("t1", "compress", "", b"x" * envelope.MAX_PAYLOAD, {}, [])
    assert len(envelope.decode(raw)["data"]) == envelope.MAX_PAYLOAD


def test_requires_travels_in_the_envelope():
    raw = envelope.encode("t2", "img.filter", "code", b"", {}, ["legacy:imagemagick"])
    assert envelope.decode(raw)["requires"] == ["legacy:imagemagick"]


def test_submitted_at_is_present_and_numeric():
    out = envelope.decode(envelope.encode("t3", "pi", "", b"", {}, []))
    assert isinstance(out["submitted_at"], (int, float)) and out["submitted_at"] > 0
