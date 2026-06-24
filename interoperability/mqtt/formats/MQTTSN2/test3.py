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

Tests for MQTT-SN 2.0 packet serialization and deserialization.

Covers: PUBWOS, SLEEPREQ, SLEEPRESP, WAKEUP, ADVERTISE, SEARCHGW, GWINFO.

Each test exercises a complete pack/unpack round-trip, then verifies that
the round-tripped object compares equal to the original using __eq__, and
that every individual field carries the expected value.  Where a packet has
optional fields, separate tests cover the absent and present variants.

Run with:
    python3 -m pytest test_mqttsn2_new_packets.py -v
    python3 test_mqttsn2_new_packets.py
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


# ---------------------------------------------------------------------------
# PUBWOS
# ---------------------------------------------------------------------------

class TestPubwos(unittest.TestCase):

    # --- Predefined alias, no Retain ---

    def test_predefined_alias_basic(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 1          # Predefined
        pkt.PubwosFlags.Retain    = False
        pkt.TopicAlias = 7
        pkt.Data       = b"hello"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.PubwosFlags.TopicType, 1)
        self.assertEqual(pkt2.PubwosFlags.Retain,    False)
        self.assertEqual(pkt2.TopicAlias,            7)
        self.assertEqual(pkt2.Data,                  b"hello")

    def test_predefined_alias_retain(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 1
        pkt.PubwosFlags.Retain    = True
        pkt.TopicAlias = 42
        pkt.Data       = b"\x01\x02\x03"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.PubwosFlags.Retain, True)
        self.assertEqual(pkt2.TopicAlias,         42)
        self.assertEqual(pkt2.Data,               b"\x01\x02\x03")

    def test_predefined_alias_max_alias(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 1
        pkt.TopicAlias = 0xFFFF
        pkt.Data       = b""

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.TopicAlias, 0xFFFF)
        self.assertEqual(pkt2.Data,       b"")

    # --- Topic Name ---

    def test_topic_name_basic(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 3          # Topic Name
        pkt.PubwosFlags.Retain    = False
        pkt.TopicName = b"sensors/temperature"
        pkt.Data      = b"22.5"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.PubwosFlags.TopicType, 3)
        self.assertEqual(pkt2.TopicName,             b"sensors/temperature")
        self.assertEqual(pkt2.Data,                  b"22.5")

    def test_topic_name_retain(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 3
        pkt.PubwosFlags.Retain    = True
        pkt.TopicName = b"home/status"
        pkt.Data      = b"online"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.PubwosFlags.Retain, True)
        self.assertEqual(pkt2.TopicName,          b"home/status")

    def test_topic_name_zero_length_payload(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 3
        pkt.TopicName = b"empty/topic"
        pkt.Data      = b""

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.Data, b"")

    def test_topic_name_binary_payload(self):
        """Binary payload with all byte values 0x00–0x7f."""
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 3
        pkt.TopicName = b"binary/data"
        pkt.Data      = bytes(range(128))

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.Data, bytes(range(128)))

    # --- packetType ---

    def test_packet_type(self):
        pkt = MQTTSN2.Pubwoses()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.PUBWOS)

    # --- unpackPacket dispatch ---

    def test_unpack_packet_dispatch(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 1
        pkt.TopicAlias = 1
        buf  = pkt.pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Pubwoses)

    # --- inequality ---

    def test_inequality_different_alias(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 1
        pkt.TopicAlias = 5
        pkt.Data       = b"x"

        pkt2, _ = roundtrip(pkt)
        pkt2.TopicAlias = 6

        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_topic_name(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 3
        pkt.TopicName = b"a/b"
        pkt.Data      = b"x"

        pkt2, _ = roundtrip(pkt)
        pkt2.TopicName = b"a/c"

        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_data(self):
        pkt = MQTTSN2.Pubwoses()
        pkt.PubwosFlags.TopicType = 1
        pkt.TopicAlias = 1
        pkt.Data       = b"a"

        pkt2, _ = roundtrip(pkt)
        pkt2.Data = b"b"

        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# SLEEPREQ
# ---------------------------------------------------------------------------

class TestSleepreq(unittest.TestCase):

    def test_basic(self):
        pkt = MQTTSN2.Sleepreqs()
        pkt.SleepreqFlags.RetainT = False
        pkt.PacketId      = 1
        pkt.SleepDuration = 60

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.SleepreqFlags.RetainT, False)
        self.assertEqual(pkt2.PacketId,              1)
        self.assertEqual(pkt2.SleepDuration,         60)

    def test_retain_topic_aliases(self):
        pkt = MQTTSN2.Sleepreqs()
        pkt.SleepreqFlags.RetainT = True
        pkt.PacketId      = 0x1234
        pkt.SleepDuration = 3600

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.SleepreqFlags.RetainT, True)
        self.assertEqual(pkt2.PacketId,              0x1234)
        self.assertEqual(pkt2.SleepDuration,         3600)

    def test_large_sleep_duration(self):
        pkt = MQTTSN2.Sleepreqs()
        pkt.PacketId      = 0xFFFF
        pkt.SleepDuration = 0xFFFFFFFF   # maximum 4-byte value

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.PacketId,      0xFFFF)
        self.assertEqual(pkt2.SleepDuration, 0xFFFFFFFF)

    def test_packet_type(self):
        pkt = MQTTSN2.Sleepreqs()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.SLEEPREQ)

    def test_unpack_packet_dispatch(self):
        pkt  = MQTTSN2.Sleepreqs()
        pkt.SleepDuration = 30
        buf  = pkt.pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Sleepreqs)

    def test_inequality_different_duration(self):
        pkt = MQTTSN2.Sleepreqs()
        pkt.PacketId = 1; pkt.SleepDuration = 60

        pkt2, _ = roundtrip(pkt)
        pkt2.SleepDuration = 61

        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_packetid(self):
        pkt = MQTTSN2.Sleepreqs()
        pkt.PacketId = 1; pkt.SleepDuration = 60

        pkt2, _ = roundtrip(pkt)
        pkt2.PacketId = 2

        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# SLEEPRESP
# ---------------------------------------------------------------------------

class TestSleepresp(unittest.TestCase):

    def test_minimal(self):
        """No SleepDuration, no ReasonCode — both absent from wire."""
        pkt = MQTTSN2.Sleepresps()
        pkt.PacketId      = 0x1234
        pkt.SleepDuration = None
        pkt.ReasonCode    = 0

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.SleeprespFlags.SleepDur, False)
        self.assertEqual(pkt2.PacketId,                0x1234)
        self.assertIsNone(pkt2.SleepDuration)
        self.assertEqual(pkt2.ReasonCode,              0)

    def test_with_sleep_duration(self):
        """Server returns a modified sleep duration."""
        pkt = MQTTSN2.Sleepresps()
        pkt.PacketId      = 42
        pkt.SleepDuration = 7200
        pkt.ReasonCode    = 0

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.SleeprespFlags.SleepDur, True)
        self.assertEqual(pkt2.SleepDuration,           7200)
        self.assertEqual(pkt2.ReasonCode,              0)

    def test_with_reason_code_only(self):
        """Failure reason code, no sleep duration."""
        pkt = MQTTSN2.Sleepresps()
        pkt.PacketId      = 10
        pkt.SleepDuration = None
        pkt.ReasonCode    = 0x97   # Topic Alias Invalid

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertIsNone(pkt2.SleepDuration)
        self.assertEqual(pkt2.ReasonCode, 0x97)

    def test_with_sleep_duration_and_reason_code(self):
        """Server sends both a modified duration and a non-success reason code."""
        pkt = MQTTSN2.Sleepresps()
        pkt.PacketId      = 99
        pkt.SleepDuration = 3600
        pkt.ReasonCode    = 0x80   # Unspecified error

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.SleepDuration, 3600)
        self.assertEqual(pkt2.ReasonCode,    0x80)

    def test_sleep_duration_flag_derived_from_field(self):
        """SleepDur flag in pack() is derived from SleepDuration being non-None."""
        pkt = MQTTSN2.Sleepresps()
        pkt.PacketId      = 1
        pkt.SleepDuration = 120

        buf  = pkt.pack()
        pkt2 = MQTTSN2.Sleepresps(buf)

        self.assertTrue(pkt2.SleeprespFlags.SleepDur)

    def test_packet_type(self):
        pkt = MQTTSN2.Sleepresps()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.SLEEPRESP)

    def test_unpack_packet_dispatch(self):
        buf  = MQTTSN2.Sleepresps().pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Sleepresps)

    def test_inequality_different_duration(self):
        pkt = MQTTSN2.Sleepresps()
        pkt.PacketId = 1; pkt.SleepDuration = 60

        pkt2, _ = roundtrip(pkt)
        pkt2.SleepDuration = 61

        self.assertNotEqual(pkt, pkt2)

    def test_inequality_none_vs_value(self):
        pkt  = MQTTSN2.Sleepresps(); pkt.PacketId = 1; pkt.SleepDuration = None
        pkt2 = MQTTSN2.Sleepresps(); pkt2.PacketId = 1; pkt2.SleepDuration = 60
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_reason_code(self):
        pkt  = MQTTSN2.Sleepresps(); pkt.PacketId = 1; pkt.ReasonCode = 0
        pkt2 = MQTTSN2.Sleepresps(); pkt2.PacketId = 1; pkt2.ReasonCode = 0x80
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# WAKEUP
# ---------------------------------------------------------------------------

class TestWakeup(unittest.TestCase):

    def test_basic(self):
        pkt = MQTTSN2.Wakeups()
        pkt2, _ = roundtrip(pkt)
        self.assertEqual(pkt, pkt2)

    def test_wire_length(self):
        """WAKEUP is 2 bytes: length(1) + type(1)."""
        buf = MQTTSN2.Wakeups().pack()
        self.assertEqual(len(buf), 2)

    def test_wire_type_byte(self):
        buf = MQTTSN2.Wakeups().pack()
        # Second byte is the packet type
        self.assertEqual(buf[1], MQTTSN2.PacketTypes.WAKEUP)

    def test_packet_type(self):
        pkt = MQTTSN2.Wakeups()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.WAKEUP)

    def test_any_two_wakeups_equal(self):
        self.assertEqual(MQTTSN2.Wakeups(), MQTTSN2.Wakeups())

    def test_unpack_packet_dispatch(self):
        buf  = MQTTSN2.Wakeups().pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Wakeups)


# ---------------------------------------------------------------------------
# ADVERTISE
# ---------------------------------------------------------------------------

class TestAdvertise(unittest.TestCase):

    def test_basic(self):
        pkt = MQTTSN2.Advertises()
        pkt.GatewayId = 1
        pkt.Duration  = 30

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.GatewayId, 1)
        self.assertEqual(pkt2.Duration,  30)

    def test_max_gateway_id(self):
        pkt = MQTTSN2.Advertises()
        pkt.GatewayId = 0xFF
        pkt.Duration  = 900

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.GatewayId, 0xFF)
        self.assertEqual(pkt2.Duration,  900)

    def test_max_duration(self):
        """Duration is 2 bytes; maximum is 65535 seconds (~18 hours)."""
        pkt = MQTTSN2.Advertises()
        pkt.GatewayId = 5
        pkt.Duration  = 0xFFFF

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.Duration, 0xFFFF)

    def test_wire_length(self):
        """ADVERTISE is 5 bytes: length(1) + type(1) + gw_id(1) + duration(2)."""
        buf = MQTTSN2.Advertises().pack()
        self.assertEqual(len(buf), 5)

    def test_packet_type(self):
        pkt = MQTTSN2.Advertises()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.ADVERTISE)

    def test_unpack_packet_dispatch(self):
        pkt  = MQTTSN2.Advertises()
        buf  = pkt.pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Advertises)

    def test_inequality_different_gateway_id(self):
        pkt  = MQTTSN2.Advertises(); pkt.GatewayId = 1; pkt.Duration = 30
        pkt2 = MQTTSN2.Advertises(); pkt2.GatewayId = 2; pkt2.Duration = 30
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_different_duration(self):
        pkt  = MQTTSN2.Advertises(); pkt.GatewayId = 1; pkt.Duration = 30
        pkt2 = MQTTSN2.Advertises(); pkt2.GatewayId = 1; pkt2.Duration = 60
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# SEARCHGW
# ---------------------------------------------------------------------------

class TestSearchgw(unittest.TestCase):

    def test_no_additional_network_info(self):
        """AdditionalNetworkInfo absent — minimal packet."""
        pkt = MQTTSN2.Searchgws()
        pkt.AdditionalNetworkInfo = b""

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.AdditionalNetworkInfo, b"")

    def test_with_additional_network_info(self):
        """AdditionalNetworkInfo present — e.g. ZigBee broadcast radius byte."""
        pkt = MQTTSN2.Searchgws()
        pkt.AdditionalNetworkInfo = b"\x0f"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.AdditionalNetworkInfo, b"\x0f")

    def test_with_multi_byte_additional_network_info(self):
        pkt = MQTTSN2.Searchgws()
        pkt.AdditionalNetworkInfo = b"\x01\x02\x03\x04"

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.AdditionalNetworkInfo, b"\x01\x02\x03\x04")

    def test_wire_length_no_ani(self):
        """Without AdditionalNetworkInfo: length(1) + type(1) = 2 bytes."""
        pkt = MQTTSN2.Searchgws()
        pkt.AdditionalNetworkInfo = b""
        buf = pkt.pack()
        self.assertEqual(len(buf), 2)

    def test_wire_length_with_ani(self):
        """With 1-byte AdditionalNetworkInfo: 3 bytes total."""
        pkt = MQTTSN2.Searchgws()
        pkt.AdditionalNetworkInfo = b"\x0f"
        buf = pkt.pack()
        self.assertEqual(len(buf), 3)

    def test_packet_type(self):
        pkt = MQTTSN2.Searchgws()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.SEARCHGW)

    def test_unpack_packet_dispatch(self):
        buf  = MQTTSN2.Searchgws().pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Searchgws)

    def test_inequality(self):
        pkt  = MQTTSN2.Searchgws(); pkt.AdditionalNetworkInfo  = b"\x01"
        pkt2 = MQTTSN2.Searchgws(); pkt2.AdditionalNetworkInfo = b"\x02"
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# GWINFO
# ---------------------------------------------------------------------------

class TestGwinfo(unittest.TestCase):

    def test_from_gateway_no_address(self):
        """Sent by a gateway — GatewayAddress absent."""
        pkt = MQTTSN2.Gwinfos()
        pkt.GatewayId      = 3
        pkt.GatewayAddress = b""

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.GatewayId,      3)
        self.assertEqual(pkt2.GatewayAddress, b"")

    def test_from_client_with_ipv4_address(self):
        """Sent by a client — includes the gateway's IPv4 address."""
        pkt = MQTTSN2.Gwinfos()
        pkt.GatewayId      = 3
        pkt.GatewayAddress = b"\xc0\xa8\x01\x01"  # 192.168.1.1

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt, pkt2)
        self.assertEqual(pkt2.GatewayAddress, b"\xc0\xa8\x01\x01")

    def test_from_client_with_longer_address(self):
        """GatewayAddress can be any length (network-type dependent)."""
        pkt = MQTTSN2.Gwinfos()
        pkt.GatewayId      = 7
        pkt.GatewayAddress = b"\xfe\x80\x00\x00\x00\x00\x00\x00" \
                             b"\x02\x11\x22\xff\xfe\x33\x44\x55"  # IPv6

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.GatewayAddress,
                         b"\xfe\x80\x00\x00\x00\x00\x00\x00"
                         b"\x02\x11\x22\xff\xfe\x33\x44\x55")

    def test_max_gateway_id(self):
        pkt = MQTTSN2.Gwinfos()
        pkt.GatewayId      = 0xFF
        pkt.GatewayAddress = b""

        pkt2, _ = roundtrip(pkt)

        self.assertEqual(pkt2.GatewayId, 0xFF)

    def test_wire_length_no_address(self):
        """Without GatewayAddress: length(1) + type(1) + gw_id(1) = 3 bytes."""
        pkt = MQTTSN2.Gwinfos()
        pkt.GatewayId = 1
        buf = pkt.pack()
        self.assertEqual(len(buf), 3)

    def test_wire_length_with_address(self):
        """With 4-byte address: length(1) + type(1) + gw_id(1) + addr(4) = 7 bytes."""
        pkt = MQTTSN2.Gwinfos()
        pkt.GatewayId      = 1
        pkt.GatewayAddress = b"\xc0\xa8\x01\x01"
        buf = pkt.pack()
        self.assertEqual(len(buf), 7)

    def test_packet_type(self):
        pkt = MQTTSN2.Gwinfos()
        self.assertEqual(pkt.packetType, MQTTSN2.PacketTypes.GWINFO)

    def test_unpack_packet_dispatch(self):
        pkt  = MQTTSN2.Gwinfos()
        pkt.GatewayId = 1
        buf  = pkt.pack()
        pkt2 = MQTTSN2.unpackPacket(buf)
        self.assertIsInstance(pkt2, MQTTSN2.Gwinfos)

    def test_inequality_different_gateway_id(self):
        pkt  = MQTTSN2.Gwinfos(); pkt.GatewayId = 1
        pkt2 = MQTTSN2.Gwinfos(); pkt2.GatewayId = 2
        self.assertNotEqual(pkt, pkt2)

    def test_inequality_address_absent_vs_present(self):
        pkt  = MQTTSN2.Gwinfos(); pkt.GatewayId = 1; pkt.GatewayAddress = b""
        pkt2 = MQTTSN2.Gwinfos(); pkt2.GatewayId = 1; pkt2.GatewayAddress = b"\x7f\x00\x00\x01"
        self.assertNotEqual(pkt, pkt2)


# ---------------------------------------------------------------------------
# Cross-packet: classes[] indices
# ---------------------------------------------------------------------------

class TestClassesIndex(unittest.TestCase):

    def _check(self, cls, expected_type):
        idx = MQTTSN2.classes.index(cls)
        self.assertEqual(idx, expected_type,
                         f"{cls.__name__} at index {idx}, expected {expected_type}")

    def test_pubwos_index(self):
        self._check(MQTTSN2.Pubwoses,   MQTTSN2.PacketTypes.PUBWOS)

    def test_sleepreq_index(self):
        self._check(MQTTSN2.Sleepreqs,  MQTTSN2.PacketTypes.SLEEPREQ)

    def test_sleepresp_index(self):
        self._check(MQTTSN2.Sleepresps, MQTTSN2.PacketTypes.SLEEPRESP)

    def test_wakeup_index(self):
        self._check(MQTTSN2.Wakeups,    MQTTSN2.PacketTypes.WAKEUP)

    def test_advertise_index(self):
        self._check(MQTTSN2.Advertises, MQTTSN2.PacketTypes.ADVERTISE)

    def test_searchgw_index(self):
        self._check(MQTTSN2.Searchgws,  MQTTSN2.PacketTypes.SEARCHGW)

    def test_gwinfo_index(self):
        self._check(MQTTSN2.Gwinfos,    MQTTSN2.PacketTypes.GWINFO)


if __name__ == "__main__":
    unittest.main()
