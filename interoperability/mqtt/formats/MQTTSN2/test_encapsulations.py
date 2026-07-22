"""
*******************************************************************
  Copyright (c) 2026 IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v2.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     https://www.eclipse.org/legal/epl-2.0/
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************

Tests for MQTT-SN 2.0 encapsulation packet serialization and deserialization.

Covers: Forwarder Encapsulation (0xFD), Connection Encapsulation (0xFE),
and Protection Encapsulation (0xFF).

Each test exercises a complete pack/unpack round-trip and verifies every
individual field.  The three packet types are sparse (their type codes do not
fit in the dense classes[] list) so unpackPacket dispatch is tested via the
encapsulation_classes dict.

Run with:
    python3 -m pytest test_mqttsn2_encapsulations.py -v
    python3 test_mqttsn2_encapsulations.py
"""

import unittest
import sys
import logging

import MQTTSN2

logging.basicConfig(level=logging.WARNING)


def roundtrip(pkt):
    """Pack pkt and deserialize it into a fresh instance; return both."""
    buf = pkt.pack()
    cls = type(pkt)
    pkt2 = cls(buf)
    return pkt2, buf


def make_pingreq():
    """Return a raw PINGREQ buffer to use as a representative inner packet."""
    pkt = MQTTSN2.Pingreqs()
    pkt.PacketId = 0x0042
    return pkt.pack()


def make_publish():
    """Return a raw QoS-1 PUBLISH buffer as a larger inner packet."""
    pkt = MQTTSN2.Publishes()
    pkt.Flags.QoS = 1
    pkt.Flags.TopicType = MQTTSN2.PublishFlags.TOPIC_TYPE_SESSION
    pkt.TopicAlias = 7
    pkt.PacketId = 1
    pkt.Data = b"sensor-reading"
    return pkt.pack()


# ---------------------------------------------------------------------------
# Forwarder Encapsulation
# ---------------------------------------------------------------------------

class TestForwarderEncapsulation(unittest.TestCase):

    # --- minimal: no addressing info, no inner packet ---

    def test_minimal_round_trip(self):
        """Empty ClientAddressingInfo, empty MQTTSNPacket."""
        pkt = MQTTSN2.ForwarderEncapsulations()

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ClientAddressingInfo, b"")
        self.assertEqual(pkt2.MQTTSNPacket,         b"")

    def test_minimal_wire_length(self):
        """Minimal packet is 2 bytes: outer_length(1) + type(1)."""
        buf = MQTTSN2.ForwarderEncapsulations().pack()
        self.assertEqual(len(buf), 2)

    # --- with ClientAddressingInfo, no inner packet ---

    def test_addressing_info_only(self):
        """ClientAddressingInfo present, no inner packet."""
        pkt = MQTTSN2.ForwarderEncapsulations()
        pkt.ClientAddressingInfo = b"\x01\x02\x03"

        pkt2, buf = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ClientAddressingInfo, b"\x01\x02\x03")
        self.assertEqual(pkt2.MQTTSNPacket,         b"")
        # outer_length = 1(len) + 1(type) + 3(addr) = 5; inner absent
        self.assertEqual(len(buf), 5)
        self.assertEqual(buf[0], 5)   # outer_length byte value

    def test_addressing_info_single_byte(self):
        """Single-byte addressing info (e.g. ZigBee node id)."""
        pkt = MQTTSN2.ForwarderEncapsulations()
        pkt.ClientAddressingInfo = b"\xff"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ClientAddressingInfo, b"\xff")

    def test_addressing_info_multi_byte(self):
        """Multi-byte addressing info."""
        pkt = MQTTSN2.ForwarderEncapsulations()
        pkt.ClientAddressingInfo = b"\xaa\xbb\xcc\xdd\xee\xff"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ClientAddressingInfo, b"\xaa\xbb\xcc\xdd\xee\xff")

    # --- with inner packet ---

    def test_with_inner_pingreq(self):
        """Inner PINGREQ packet encapsulated correctly."""
        inner = make_pingreq()
        pkt   = MQTTSN2.ForwarderEncapsulations()
        pkt.ClientAddressingInfo = b"\x01"
        pkt.MQTTSNPacket         = inner

        pkt2, buf = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.MQTTSNPacket, inner)
        # outer_length = 1+1+1 = 3; inner is 4 bytes; total = 7
        self.assertEqual(buf[0], 3)
        self.assertEqual(len(buf), 3 + len(inner))

    def test_with_inner_publish(self):
        """Larger inner packet (QoS-1 PUBLISH) round-trips correctly."""
        inner = make_publish()
        pkt   = MQTTSN2.ForwarderEncapsulations()
        pkt.ClientAddressingInfo = b"\x01\x02\x03"
        pkt.MQTTSNPacket         = inner

        pkt2, buf = roundtrip(pkt)

        self.assertEqual(pkt2.MQTTSNPacket, inner)
        # outer_length = 1+1+3 = 5; inner follows outside that count
        self.assertEqual(buf[0], 5)
        self.assertEqual(len(buf), 5 + len(inner))

    def test_outer_length_does_not_include_inner(self):
        """The outer_length byte covers only up to end of ClientAddressingInfo."""
        inner = make_pingreq()
        pkt   = MQTTSN2.ForwarderEncapsulations()
        pkt.ClientAddressingInfo = b"\x01\x02\x03"
        pkt.MQTTSNPacket         = inner
        buf = pkt.pack()

        outer_len = buf[0]
        # outer section: len(1) + type(1) + addr(3)
        self.assertEqual(outer_len, 5)
        # inner packet starts at byte outer_len
        self.assertEqual(buf[outer_len:], inner)

    def test_inner_packet_survives_round_trip_intact(self):
        """Inner bytes are stored verbatim and recovered without modification."""
        inner = make_publish()
        pkt   = MQTTSN2.ForwarderEncapsulations()
        pkt.MQTTSNPacket = inner

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.MQTTSNPacket, inner)
        # Verify the inner bytes are parseable as the original packet type
        recovered_inner = MQTTSN2.unpackPacket(pkt2.MQTTSNPacket)
        self.assertIsInstance(recovered_inner, MQTTSN2.Publishes)

    # --- packetType and dispatch ---

    def test_packet_type(self):
        pkt = MQTTSN2.ForwarderEncapsulations()
        self.assertEqual(pkt.packetType,
                         MQTTSN2.PacketTypes.FOWARDER_ENCAPSULATION)

    def test_unpack_packet_dispatch(self):
        pkt  = MQTTSN2.ForwarderEncapsulations()
        pkt2 = MQTTSN2.unpackPacket(pkt.pack())
        self.assertIsInstance(pkt2, MQTTSN2.ForwarderEncapsulations)

    def test_in_encapsulation_classes_dict(self):
        self.assertIn(MQTTSN2.PacketTypes.FOWARDER_ENCAPSULATION,
                      MQTTSN2.encapsulation_classes)
        self.assertIs(
            MQTTSN2.encapsulation_classes[MQTTSN2.PacketTypes.FOWARDER_ENCAPSULATION],
            MQTTSN2.ForwarderEncapsulations)

    # --- inequality ---

    def test_inequality_different_addressing_info(self):
        pkt  = MQTTSN2.ForwarderEncapsulations(); pkt.ClientAddressingInfo  = b"\x01"
        pkt2 = MQTTSN2.ForwarderEncapsulations(); pkt2.ClientAddressingInfo = b"\x02"
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_inner_packet(self):
        pkt  = MQTTSN2.ForwarderEncapsulations(); pkt.MQTTSNPacket  = b"\x04\x0c\x00\x01"
        pkt2 = MQTTSN2.ForwarderEncapsulations(); pkt2.MQTTSNPacket = b"\x04\x0c\x00\x02"
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_inner_absent_vs_present(self):
        pkt  = MQTTSN2.ForwarderEncapsulations(); pkt.MQTTSNPacket  = b""
        pkt2 = MQTTSN2.ForwarderEncapsulations(); pkt2.MQTTSNPacket = make_pingreq()
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# Connection Encapsulation
# ---------------------------------------------------------------------------

class TestConnectionEncapsulation(unittest.TestCase):

    # --- minimal: empty ClientIdentifier, no inner packet ---

    def test_minimal_round_trip(self):
        """Empty ClientIdentifier, empty MQTTSNPacket."""
        pkt = MQTTSN2.ConnectionEncapsulations()

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ClientIdentifier, "")
        self.assertEqual(pkt2.MQTTSNPacket,     b"")

    def test_minimal_wire_length(self):
        """Minimal packet is 2 bytes: outer_length(1) + type(1)."""
        buf = MQTTSN2.ConnectionEncapsulations().pack()
        self.assertEqual(len(buf), 2)

    # --- with ClientIdentifier, no inner packet ---

    def test_client_identifier_only(self):
        """ClientIdentifier present, no inner packet."""
        pkt = MQTTSN2.ConnectionEncapsulations()
        pkt.ClientIdentifier = "my-device-01"

        pkt2, buf = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ClientIdentifier, "my-device-01")
        self.assertEqual(pkt2.MQTTSNPacket,     b"")
        # outer_length = 1(len) + 1(type) + 12(cid) = 14
        self.assertEqual(buf[0], 14)
        self.assertEqual(len(buf), 14)

    def test_client_identifier_utf8(self):
        """ClientIdentifier is a UTF-8 string and round-trips correctly."""
        pkt = MQTTSN2.ConnectionEncapsulations()
        pkt.ClientIdentifier = "sensor\u00b0C"   # degree symbol

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ClientIdentifier, "sensor\u00b0C")

    # --- with inner packet ---

    def test_with_inner_pingreq(self):
        """Inner PINGREQ encapsulated; outer_length excludes inner bytes."""
        inner = make_pingreq()
        pkt   = MQTTSN2.ConnectionEncapsulations()
        pkt.ClientIdentifier = "test"
        pkt.MQTTSNPacket     = inner

        pkt2, buf = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ClientIdentifier, "test")
        self.assertEqual(pkt2.MQTTSNPacket,     inner)
        # outer_length = 1+1+4 = 6; inner is 4 bytes; total = 10
        self.assertEqual(buf[0], 6)
        self.assertEqual(len(buf), 6 + len(inner))

    def test_with_inner_publish(self):
        """Larger inner packet (PUBLISH) round-trips correctly."""
        inner = make_publish()
        pkt   = MQTTSN2.ConnectionEncapsulations()
        pkt.ClientIdentifier = "pub-client"
        pkt.MQTTSNPacket     = inner

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.MQTTSNPacket, inner)

    def test_outer_length_does_not_include_inner(self):
        """The outer_length byte covers only up to end of ClientIdentifier."""
        inner = make_pingreq()
        pkt   = MQTTSN2.ConnectionEncapsulations()
        pkt.ClientIdentifier = "test"
        pkt.MQTTSNPacket     = inner
        buf = pkt.pack()

        outer_len = buf[0]
        self.assertEqual(outer_len, 6)           # 1+1+4
        self.assertEqual(buf[outer_len:], inner)  # inner follows outside count

    def test_inner_packet_survives_round_trip_intact(self):
        """Inner bytes are stored verbatim and recovered without modification."""
        inner = make_publish()
        pkt   = MQTTSN2.ConnectionEncapsulations()
        pkt.ClientIdentifier = "c"
        pkt.MQTTSNPacket     = inner

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.MQTTSNPacket, inner)
        recovered_inner = MQTTSN2.unpackPacket(pkt2.MQTTSNPacket)
        self.assertIsInstance(recovered_inner, MQTTSN2.Publishes)

    # --- packetType and dispatch ---

    def test_packet_type(self):
        pkt = MQTTSN2.ConnectionEncapsulations()
        self.assertEqual(pkt.packetType,
                         MQTTSN2.PacketTypes.CONNECTION_ENCAPSULATION)

    def test_unpack_packet_dispatch(self):
        pkt  = MQTTSN2.ConnectionEncapsulations()
        pkt2 = MQTTSN2.unpackPacket(pkt.pack())
        self.assertIsInstance(pkt2, MQTTSN2.ConnectionEncapsulations)

    def test_in_encapsulation_classes_dict(self):
        self.assertIn(MQTTSN2.PacketTypes.CONNECTION_ENCAPSULATION,
                      MQTTSN2.encapsulation_classes)
        self.assertIs(
            MQTTSN2.encapsulation_classes[MQTTSN2.PacketTypes.CONNECTION_ENCAPSULATION],
            MQTTSN2.ConnectionEncapsulations)

    # --- inequality ---

    def test_inequality_different_client_identifier(self):
        pkt  = MQTTSN2.ConnectionEncapsulations(); pkt.ClientIdentifier  = "a"
        pkt2 = MQTTSN2.ConnectionEncapsulations(); pkt2.ClientIdentifier = "b"
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_inner_packet(self):
        pkt  = MQTTSN2.ConnectionEncapsulations(); pkt.MQTTSNPacket  = b"\x04\x0c\x00\x01"
        pkt2 = MQTTSN2.ConnectionEncapsulations(); pkt2.MQTTSNPacket = b"\x04\x0c\x00\x02"
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_inner_absent_vs_present(self):
        pkt  = MQTTSN2.ConnectionEncapsulations(); pkt.MQTTSNPacket  = b""
        pkt2 = MQTTSN2.ConnectionEncapsulations(); pkt2.MQTTSNPacket = make_pingreq()
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# ProtectionFlags
# ---------------------------------------------------------------------------

class TestProtectionFlags(unittest.TestCase):

    def test_default_values(self):
        pf = MQTTSN2.ProtectionFlags()
        self.assertEqual(pf.CounterLen, 0)
        self.assertEqual(pf.CryptoLen,  0)
        self.assertEqual(pf.AuthTagLen, 1)

    def test_pack_unpack_round_trip(self):
        pf = MQTTSN2.ProtectionFlags()
        pf.CounterLen = 1
        pf.CryptoLen  = 2
        pf.AuthTagLen = 4
        byte_val = pf.pack()[0]
        pf2 = MQTTSN2.ProtectionFlags()
        pf2.unpack(byte_val)
        self.assertEqual(pf, pf2)
        self.assertEqual(pf2.CounterLen, 1)
        self.assertEqual(pf2.CryptoLen,  2)
        self.assertEqual(pf2.AuthTagLen, 4)

    def test_all_valid_counter_lengths(self):
        for code, expected_bytes in [(0, 0), (1, 2), (2, 4)]:
            pf = MQTTSN2.ProtectionFlags()
            pf.CounterLen = code
            pf2 = MQTTSN2.ProtectionFlags()
            pf2.unpack(pf.pack()[0])
            self.assertEqual(pf2.counter_size(), expected_bytes)

    def test_reserved_counter_length_3_rejected(self):
        """CounterLen=3 is reserved and must be rejected on unpack."""
        pf = MQTTSN2.ProtectionFlags()
        pf.CounterLen = 3
        with self.assertRaises(AssertionError):
            pf.unpack(pf.pack()[0])

    def test_all_valid_crypto_lengths(self):
        for code, expected_bytes in [(0, 0), (1, 2), (2, 4), (3, 12)]:
            pf = MQTTSN2.ProtectionFlags()
            pf.CryptoLen = code
            pf2 = MQTTSN2.ProtectionFlags()
            pf2.unpack(pf.pack()[0])
            self.assertEqual(pf2.crypto_material_size(), expected_bytes)

    def test_reserved_auth_tag_length_2_rejected(self):
        """AuthTagLen=2 is reserved and must be rejected on unpack."""
        pf = MQTTSN2.ProtectionFlags()
        pf.AuthTagLen = 2
        with self.assertRaises(AssertionError):
            pf.unpack(pf.pack()[0])

    def test_reserved_auth_tag_length_3_rejected(self):
        """AuthTagLen=3 is reserved and must be rejected on unpack."""
        pf = MQTTSN2.ProtectionFlags()
        pf.AuthTagLen = 3
        with self.assertRaises(AssertionError):
            pf.unpack(pf.pack()[0])

    def test_auth_tag_length_0_allowed(self):
        """AuthTagLen=0 (provider-defined) is valid."""
        pf = MQTTSN2.ProtectionFlags()
        pf.AuthTagLen = 0
        pf2 = MQTTSN2.ProtectionFlags()
        pf2.unpack(pf.pack()[0])
        self.assertEqual(pf2.AuthTagLen, 0)

    def test_auth_tag_length_1_allowed(self):
        """AuthTagLen=1 (nominal scheme size) is valid."""
        pf = MQTTSN2.ProtectionFlags()
        pf.AuthTagLen = 1
        pf2 = MQTTSN2.ProtectionFlags()
        pf2.unpack(pf.pack()[0])
        self.assertEqual(pf2.AuthTagLen, 1)

    def test_auth_tag_length_4_to_f_allowed(self):
        """AuthTagLen values 4–15 are valid truncation codes."""
        for code in range(4, 16):
            pf = MQTTSN2.ProtectionFlags()
            pf.AuthTagLen = code
            pf2 = MQTTSN2.ProtectionFlags()
            pf2.unpack(pf.pack()[0])
            self.assertEqual(pf2.AuthTagLen, code)

    def test_bit_layout(self):
        """Verify the bit positions: AuthTagLen in bits 7-4, CryptoLen in 3-2, CounterLen in 1-0."""
        pf = MQTTSN2.ProtectionFlags()
        pf.AuthTagLen = 0xA   # 1010 in bits 7-4
        pf.CryptoLen  = 0x3   # 11   in bits 3-2
        pf.CounterLen = 0x2   # 10   in bits 1-0
        # Expected byte: 1010_11_10 = 0xAE
        self.assertEqual(pf.pack()[0], 0xAE)

    def test_equality(self):
        pf1 = MQTTSN2.ProtectionFlags()
        pf1.CounterLen = 1; pf1.CryptoLen = 2; pf1.AuthTagLen = 5
        pf2 = MQTTSN2.ProtectionFlags()
        pf2.CounterLen = 1; pf2.CryptoLen = 2; pf2.AuthTagLen = 5
        self.assertEqual(pf1, pf2)

    def test_inequality(self):
        pf1 = MQTTSN2.ProtectionFlags(); pf1.AuthTagLen = 1
        pf2 = MQTTSN2.ProtectionFlags(); pf2.AuthTagLen = 4
        self.assertNotEqual(pf1, pf2)


# ---------------------------------------------------------------------------
# Protection Encapsulation
# ---------------------------------------------------------------------------

class TestProtectionEncapsulation(unittest.TestCase):

    def _make_minimal(self, inner=None, auth_tag=None):
        """Build a minimal Protection packet (no crypto material, no counter)."""
        pkt = MQTTSN2.ProtectionEncapsulations()
        pkt.ProtectionScheme = 0x00           # HMAC-SHA256
        pkt.SenderIdentifier = b"\xAA" * 8
        pkt.Random           = b"\xBB" * 4
        pkt.ProtectedMQTTSNPacket = inner if inner is not None else make_pingreq()
        pkt.AuthenticationTag     = auth_tag if auth_tag is not None else b"\xCC" * 32
        pkt.ProtectionFlags.AuthTagLen = 1    # nominal tag size
        return pkt

    # --- ProtectionFlags auto-derived from field lengths in pack() ---

    def test_crypto_len_flag_derived_from_field(self):
        """pack() derives CryptoLen from the actual length of CryptographicMaterial."""
        pkt = self._make_minimal()
        pkt.CryptographicMaterial = b"\x01\x02"   # 2 bytes → CryptoLen=1
        buf = pkt.pack()
        pkt2 = MQTTSN2.ProtectionEncapsulations(buf)
        self.assertEqual(pkt2.ProtectionFlags.CryptoLen, 1)

    def test_counter_len_flag_derived_from_field(self):
        """pack() derives CounterLen from the actual length of MonotonicCounter."""
        pkt = self._make_minimal()
        pkt.MonotonicCounter = b"\x00\x01\x00\x02"   # 4 bytes → CounterLen=2
        buf = pkt.pack()
        pkt2 = MQTTSN2.ProtectionEncapsulations(buf)
        self.assertEqual(pkt2.ProtectionFlags.CounterLen, 2)

    # --- no optional fields ---

    def test_no_crypto_no_counter_round_trip(self):
        """Minimal packet: no crypto material, no monotonic counter."""
        pkt = self._make_minimal()

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CryptoLen,  0)
        self.assertEqual(pkt2.ProtectionFlags.CounterLen, 0)
        self.assertEqual(pkt2.ProtectionScheme,           0x00)
        self.assertEqual(pkt2.SenderIdentifier,           b"\xAA" * 8)
        self.assertEqual(pkt2.Random,                     b"\xBB" * 4)
        self.assertEqual(pkt2.CryptographicMaterial,      b"")
        self.assertEqual(pkt2.MonotonicCounter,           b"")
        self.assertEqual(pkt2.ProtectedMQTTSNPacket,      make_pingreq())
        self.assertEqual(pkt2.AuthenticationTag,          b"\xCC" * 32)

    def test_no_crypto_no_counter_wire_length(self):
        """
        Wire length = 1(len) + 1(type) + 1(flags) + 1(scheme) +
                      8(sender) + 4(random) + 4(inner PINGREQ) + 32(tag) = 52.
        """
        pkt = self._make_minimal()
        buf = pkt.pack()
        self.assertEqual(len(buf), 52)

    # --- 2-byte crypto material ---

    def test_crypto_2_bytes_round_trip(self):
        """CryptoLen=1: 2-byte CryptographicMaterial."""
        pkt = self._make_minimal(auth_tag=b"\xFF" * 16)
        pkt.ProtectionScheme      = 0x46   # AES-GCM-128-128
        pkt.CryptographicMaterial = b"\xDE\xAD"
        pkt.AuthenticationTag     = b"\xFF" * 16

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CryptoLen, 1)
        self.assertEqual(pkt2.CryptographicMaterial,     b"\xDE\xAD")

    # --- 4-byte crypto material ---

    def test_crypto_4_bytes_round_trip(self):
        """CryptoLen=2: 4-byte CryptographicMaterial."""
        pkt = self._make_minimal(auth_tag=b"\xFF" * 16)
        pkt.CryptographicMaterial = b"\xCA\xFE\xBA\xBE"
        pkt.AuthenticationTag     = b"\xFF" * 16

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CryptoLen, 2)
        self.assertEqual(pkt2.CryptographicMaterial,     b"\xCA\xFE\xBA\xBE")

    # --- 12-byte crypto material ---

    def test_crypto_12_bytes_round_trip(self):
        """CryptoLen=3: 12-byte CryptographicMaterial."""
        pkt = self._make_minimal(auth_tag=b"\xFF" * 16)
        pkt.CryptographicMaterial = b"\x01\x02\x03\x04\x05\x06" \
                                    b"\x07\x08\x09\x0A\x0B\x0C"
        pkt.AuthenticationTag     = b"\xFF" * 16

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CryptoLen, 3)
        self.assertEqual(len(pkt2.CryptographicMaterial), 12)
        self.assertEqual(pkt2.CryptographicMaterial,
                         b"\x01\x02\x03\x04\x05\x06"
                         b"\x07\x08\x09\x0A\x0B\x0C")

    # --- 2-byte monotonic counter ---

    def test_counter_2_bytes_round_trip(self):
        """CounterLen=1: 2-byte MonotonicCounter."""
        pkt = self._make_minimal()
        pkt.MonotonicCounter = b"\x00\x2A"   # value 42

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CounterLen, 1)
        self.assertEqual(pkt2.MonotonicCounter,           b"\x00\x2A")

    # --- 4-byte monotonic counter ---

    def test_counter_4_bytes_round_trip(self):
        """CounterLen=2: 4-byte MonotonicCounter."""
        pkt = self._make_minimal()
        pkt.MonotonicCounter = b"\x00\x00\x01\x00"   # value 256

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CounterLen, 2)
        self.assertEqual(pkt2.MonotonicCounter,           b"\x00\x00\x01\x00")

    # --- all optional fields present ---

    def test_all_optional_fields_round_trip(self):
        """12-byte crypto material + 4-byte counter together."""
        pkt = MQTTSN2.ProtectionEncapsulations()
        pkt.ProtectionFlags.AuthTagLen = 1
        pkt.ProtectionScheme      = 0x46
        pkt.SenderIdentifier      = b"\x01\x02\x03\x04\x05\x06\x07\x08"
        pkt.Random                = b"\x09\x0A\x0B\x0C"
        pkt.CryptographicMaterial = b"\xDE" * 12
        pkt.MonotonicCounter      = b"\x00\x00\x00\xFF"
        pkt.ProtectedMQTTSNPacket = make_pingreq()
        pkt.AuthenticationTag     = b"\xAB" * 16

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.ProtectionFlags.CryptoLen,  3)
        self.assertEqual(pkt2.ProtectionFlags.CounterLen, 2)
        self.assertEqual(pkt2.CryptographicMaterial,      b"\xDE" * 12)
        self.assertEqual(pkt2.MonotonicCounter,           b"\x00\x00\x00\xFF")

    # --- AuthTagLen codes ---

    def test_auth_tag_len_code_1_nominal(self):
        """AuthTagLen=1: tag length is scheme-nominal; stored as raw bytes."""
        pkt = self._make_minimal(auth_tag=b"\xCC" * 32)
        pkt.ProtectionFlags.AuthTagLen = 1

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectionFlags.AuthTagLen, 1)
        self.assertEqual(pkt2.AuthenticationTag,          b"\xCC" * 32)

    def test_auth_tag_len_code_4_fixed_8_bytes(self):
        """AuthTagLen=4: tag is 4 × 2 = 8 bytes."""
        pkt = self._make_minimal(auth_tag=b"\xAB" * 8)
        pkt.ProtectionFlags.AuthTagLen = 4

        pkt2, buf = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectionFlags.AuthTagLen, 4)
        self.assertEqual(pkt2.AuthenticationTag,          b"\xAB" * 8)
        # wire: 1+1+1+1+8+4+0+0+4+8 = 28
        self.assertEqual(len(buf), 28)

    def test_auth_tag_len_code_8_fixed_16_bytes(self):
        """AuthTagLen=8: tag is 8 × 2 = 16 bytes."""
        pkt = self._make_minimal(auth_tag=b"\xEE" * 16)
        pkt.ProtectionFlags.AuthTagLen = 8

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.AuthenticationTag, b"\xEE" * 16)
        self.assertEqual(len(pkt2.AuthenticationTag), 8 * 2)

    def test_auth_tag_len_code_f_fixed_30_bytes(self):
        """AuthTagLen=0xF: tag is 15 × 2 = 30 bytes."""
        pkt = self._make_minimal(auth_tag=b"\xFF" * 30)
        pkt.ProtectionFlags.AuthTagLen = 0xF

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.AuthenticationTag, b"\xFF" * 30)
        self.assertEqual(len(pkt2.AuthenticationTag), 0xF * 2)

    # --- protection schemes ---

    def test_scheme_hmac_sha256(self):
        pkt = self._make_minimal()
        pkt.ProtectionScheme = 0x00

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectionScheme, 0x00)

    def test_scheme_aes_gcm_128_128(self):
        pkt = self._make_minimal(auth_tag=b"\xFF" * 16)
        pkt.ProtectionScheme  = 0x46
        pkt.AuthenticationTag = b"\xFF" * 16

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectionScheme, 0x46)

    def test_scheme_provider_defined_auth_only(self):
        """Provider-defined auth-only scheme (0x3C–0x3F range)."""
        pkt = self._make_minimal()
        pkt.ProtectionScheme = 0x3C

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectionScheme, 0x3C)

    def test_scheme_provider_defined_aead(self):
        """Provider-defined AEAD scheme (0xF0–0xFF range)."""
        pkt = self._make_minimal(auth_tag=b"\xAA" * 16)
        pkt.ProtectionScheme  = 0xF0
        pkt.AuthenticationTag = b"\xAA" * 16

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectionScheme, 0xF0)

    # --- sender identifier ---

    def test_sender_identifier_round_trip(self):
        """All 8 SenderIdentifier bytes are preserved exactly."""
        pkt = self._make_minimal()
        pkt.SenderIdentifier = b"\x01\x23\x45\x67\x89\xAB\xCD\xEF"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.SenderIdentifier, b"\x01\x23\x45\x67\x89\xAB\xCD\xEF")

    # --- random field ---

    def test_random_field_round_trip(self):
        """All 4 Random bytes are preserved exactly."""
        pkt = self._make_minimal()
        pkt.Random = b"\xDE\xAD\xBE\xEF"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.Random, b"\xDE\xAD\xBE\xEF")

    # --- inner packet ---

    def test_inner_pingreq_round_trip(self):
        inner = make_pingreq()
        pkt   = self._make_minimal(inner=inner)

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectedMQTTSNPacket, inner)

    def test_inner_publish_round_trip(self):
        """Larger inner packet round-trips intact."""
        inner = make_publish()
        pkt   = self._make_minimal(inner=inner, auth_tag=b"\xCC" * 32)

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.ProtectedMQTTSNPacket, inner)
        recovered = MQTTSN2.unpackPacket(pkt2.ProtectedMQTTSNPacket)
        self.assertIsInstance(recovered, MQTTSN2.Publishes)

    def test_crypto_2_counter_2_wire_length(self):
        """
        Wire length = 1+1+1+1+8+4+2(crypto)+2(counter)+4(inner)+16(tag) = 40.
        """
        pkt = self._make_minimal(auth_tag=b"\xFF" * 16)
        pkt.CryptographicMaterial = b"\xDE\xAD"
        pkt.MonotonicCounter      = b"\x00\x01"
        pkt.AuthenticationTag     = b"\xFF" * 16
        buf = pkt.pack()
        self.assertEqual(len(buf), 40)

    def test_crypto_12_counter_4_tag_8_wire_length(self):
        """
        Wire length = 1+1+1+1+8+4+12(crypto)+4(counter)+4(inner)+8(tag) = 44.
        """
        pkt = MQTTSN2.ProtectionEncapsulations()
        pkt.ProtectionFlags.AuthTagLen = 4
        pkt.ProtectionScheme      = 0x00
        pkt.SenderIdentifier      = b"\x00" * 8
        pkt.Random                = b"\x00" * 4
        pkt.CryptographicMaterial = b"\xCA" * 12
        pkt.MonotonicCounter      = b"\x00\x00\x00\x01"
        pkt.ProtectedMQTTSNPacket = make_pingreq()
        pkt.AuthenticationTag     = b"\xAB" * 8
        buf = pkt.pack()
        self.assertEqual(len(buf), 44)

    # --- packetType and dispatch ---

    def test_packet_type(self):
        pkt = MQTTSN2.ProtectionEncapsulations()
        self.assertEqual(pkt.packetType,
                         MQTTSN2.PacketTypes.PROTECTION_ENCAPSULATION)

    def test_unpack_packet_dispatch(self):
        pkt  = self._make_minimal()
        pkt2 = MQTTSN2.unpackPacket(pkt.pack())
        self.assertIsInstance(pkt2, MQTTSN2.ProtectionEncapsulations)

    def test_in_encapsulation_classes_dict(self):
        self.assertIn(MQTTSN2.PacketTypes.PROTECTION_ENCAPSULATION,
                      MQTTSN2.encapsulation_classes)
        self.assertIs(
            MQTTSN2.encapsulation_classes[MQTTSN2.PacketTypes.PROTECTION_ENCAPSULATION],
            MQTTSN2.ProtectionEncapsulations)

    # --- inequality ---

    def test_inequality_different_scheme(self):
        pkt  = self._make_minimal(); pkt.ProtectionScheme  = 0x00
        pkt2 = self._make_minimal(); pkt2.ProtectionScheme = 0x46
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_sender_identifier(self):
        pkt  = self._make_minimal(); pkt.SenderIdentifier  = b"\x01" * 8
        pkt2 = self._make_minimal(); pkt2.SenderIdentifier = b"\x02" * 8
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_random(self):
        pkt  = self._make_minimal(); pkt.Random  = b"\x01" * 4
        pkt2 = self._make_minimal(); pkt2.Random = b"\x02" * 4
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_crypto_material(self):
        pkt  = self._make_minimal(); pkt.CryptographicMaterial  = b""
        pkt2 = self._make_minimal(); pkt2.CryptographicMaterial = b"\xDE\xAD"
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_counter(self):
        pkt  = self._make_minimal(); pkt.MonotonicCounter  = b""
        pkt2 = self._make_minimal(); pkt2.MonotonicCounter = b"\x00\x01"
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_inner_packet(self):
        pkt  = self._make_minimal(inner=make_pingreq())
        pkt2 = self._make_minimal(inner=make_publish())
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_auth_tag(self):
        pkt  = self._make_minimal(auth_tag=b"\x00" * 32)
        pkt2 = self._make_minimal(auth_tag=b"\xFF" * 32)
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# encapsulation_classes dict completeness
# ---------------------------------------------------------------------------

class TestEncapsulationClassesDict(unittest.TestCase):

    def test_all_three_types_present(self):
        for type_code in (MQTTSN2.PacketTypes.FOWARDER_ENCAPSULATION,
                          MQTTSN2.PacketTypes.CONNECTION_ENCAPSULATION,
                          MQTTSN2.PacketTypes.PROTECTION_ENCAPSULATION):
            self.assertIn(type_code, MQTTSN2.encapsulation_classes)

    def test_forwarder_maps_to_correct_class(self):
        self.assertIs(
            MQTTSN2.encapsulation_classes[MQTTSN2.PacketTypes.FOWARDER_ENCAPSULATION],
            MQTTSN2.ForwarderEncapsulations)

    def test_connection_maps_to_correct_class(self):
        self.assertIs(
            MQTTSN2.encapsulation_classes[MQTTSN2.PacketTypes.CONNECTION_ENCAPSULATION],
            MQTTSN2.ConnectionEncapsulations)

    def test_protection_maps_to_correct_class(self):
        self.assertIs(
            MQTTSN2.encapsulation_classes[MQTTSN2.PacketTypes.PROTECTION_ENCAPSULATION],
            MQTTSN2.ProtectionEncapsulations)

    def test_encapsulation_types_not_in_classes_list(self):
        """The sparse type codes must not appear in the dense classes[] list."""
        for type_code in (MQTTSN2.PacketTypes.FOWARDER_ENCAPSULATION,
                          MQTTSN2.PacketTypes.CONNECTION_ENCAPSULATION,
                          MQTTSN2.PacketTypes.PROTECTION_ENCAPSULATION):
            self.assertGreater(type_code, len(MQTTSN2.classes) - 1)


if __name__ == "__main__":
    unittest.main()
