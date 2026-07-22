"""
*******************************************************************
  Copyright (c) 2026 Ian Craggs

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v2.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v20.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

"""
Round-trip and validation tests for MQTTSN2.py.

Coverage:
  SubscribeFlags  - pack/unpack, all fields, protocol-error assertions
  SubackFlags     - pack/unpack, reserved-bit assertion
  Subscribes      - filter, predefined alias, session alias round-trips
  Subacks         - minimal, reason-code-only, alias+reason-code round-trips
  ReasonCodes     - set/getName, pack/unpack, per-packet-type name variants,
                    MQTT-SN-specific codes, 0xA0, invalid name/identifier
  Pubacks         - success (no wire reason code), error, wrong-type rejection
  Pubrecs         - same
  Pubrels         - same
  Pubcomps        - same
  unpackPacket    - dispatch for all implemented packet types
"""

import MQTTSN2
import sys

MT = MQTTSN2.PacketTypes

# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

tests_run    = 0
tests_passed = 0
tests_failed = 0

def check(label, expr):
  global tests_run, tests_passed, tests_failed
  tests_run += 1
  if expr:
    tests_passed += 1
  else:
    tests_failed += 1
    print(f"  FAIL  {label}")

def section(title):
  print(f"\n--- {title} ---")


# ===========================================================================
# SubscribeFlags
# ===========================================================================

section("SubscribeFlags pack/unpack")

sf = MQTTSN2.SubscribeFlags()
sf.QoS = 1
sf.RetainHandling = 1
sf.RaP = True
sf.NoLocal = True
sf.TopicType = 3
buf = sf.pack()
sf2 = MQTTSN2.SubscribeFlags()
sf2.unpack(buf[0])
check("QoS round-trip",             sf2.QoS == 1)
check("RetainHandling round-trip",  sf2.RetainHandling == 1)
check("RaP round-trip",             sf2.RaP == True)
check("NoLocal round-trip",         sf2.NoLocal == True)
check("TopicType round-trip",       sf2.TopicType == 3)
check("__eq__",                     sf == sf2)

section("SubscribeFlags assertions")

def raises(fn):
  try:
    fn()
    return False
  except (AssertionError, MQTTSN2.MQTTSNException):
    return True

check("reserved TopicType 2 rejected",
      raises(lambda: MQTTSN2.SubscribeFlags().unpack(0x02)))
check("QoS 3 rejected",
      raises(lambda: MQTTSN2.SubscribeFlags().unpack(0x60)))
check("RetainHandling 3 rejected",
      raises(lambda: MQTTSN2.SubscribeFlags().unpack(0x0C)))

section("SubscribeFlags __setattr__ guard")

check("unknown attribute rejected",
      raises(lambda: setattr(MQTTSN2.SubscribeFlags(), "Bogus", 1)))


# ===========================================================================
# SubackFlags
# ===========================================================================

section("SubackFlags pack/unpack")

af = MQTTSN2.SubackFlags()
af.TopicType = 1
af.TopicAliasFlag = True
buf = af.pack()
af2 = MQTTSN2.SubackFlags()
af2.unpack(buf[0])
check("TopicType round-trip",      af2.TopicType == 1)
check("TopicAliasFlag round-trip", af2.TopicAliasFlag == True)
check("__eq__",                    af == af2)

section("SubackFlags assertions")

check("reserved bits 7-3 rejected",
      raises(lambda: MQTTSN2.SubackFlags().unpack(0xF8)))


# ===========================================================================
# Subscribes
# ===========================================================================

section("Subscribes — topic filter")

sub = MQTTSN2.Subscribes()
sub.SubscribeFlags.QoS = 1
sub.SubscribeFlags.RetainHandling = 1
sub.SubscribeFlags.RaP = True
sub.SubscribeFlags.NoLocal = True
sub.SubscribeFlags.TopicType = 3
sub.PacketId = 0x1234
sub.TopicFilter = "sensors/+/temp"
buf = sub.pack()
sub2 = MQTTSN2.Subscribes(buf)
check("flags round-trip",       sub2.SubscribeFlags == sub.SubscribeFlags)
check("PacketId round-trip",    sub2.PacketId == 0x1234)
check("TopicFilter round-trip", sub2.TopicFilter == "sensors/+/temp")
check("TopicAlias is 0",        sub2.TopicAlias == 0)
check("__eq__",                 sub == sub2)

section("Subscribes — predefined alias")

sub3 = MQTTSN2.Subscribes()
sub3.SubscribeFlags.QoS = 2
sub3.SubscribeFlags.TopicType = 1
sub3.PacketId = 0x0042
sub3.TopicAlias = 0xABCD
buf3 = sub3.pack()
sub4 = MQTTSN2.Subscribes(buf3)
check("TopicType round-trip",  sub4.SubscribeFlags.TopicType == 1)
check("PacketId round-trip",   sub4.PacketId == 0x0042)
check("TopicAlias round-trip", sub4.TopicAlias == 0xABCD)
check("TopicFilter is empty",  sub4.TopicFilter == "")
check("__eq__",                sub3 == sub4)

section("Subscribes — session alias")

sub5 = MQTTSN2.Subscribes()
sub5.SubscribeFlags.TopicType = 0
sub5.PacketId = 7
sub5.TopicAlias = 42
buf5 = sub5.pack()
sub6 = MQTTSN2.Subscribes(buf5)
check("TopicAlias round-trip", sub6.TopicAlias == 42)
check("__eq__",                sub5 == sub6)

section("Subscribes — wrong packet type rejected")

bad = bytes([6, MT.SUBACK, 0x00, 0x00, 0x01, 0x00])
check("wrong type rejected",
      raises(lambda: MQTTSN2.Subscribes(bad)))


# ===========================================================================
# Subacks
# ===========================================================================

section("Subacks — minimal (no alias, no reason code)")

ack = MQTTSN2.Subacks()
ack.PacketId = 0x1234
buf4 = ack.pack()
ack2 = MQTTSN2.Subacks(buf4)
check("PacketId round-trip",    ack2.PacketId == 0x1234)
check("ReasonCode defaults 0",  ack2.ReasonCode.value == 0)
check("TopicAlias defaults 0",  ack2.TopicAlias == 0)
check("TopicAliasFlag is False",ack2.SubackFlags.TopicAliasFlag == False)
check("__eq__",                 ack == ack2)
check("wire length is 5",       len(buf4) == 5)

section("Subacks — non-zero reason code, no alias")

ack_rc = MQTTSN2.Subacks()
ack_rc.PacketId = 5
ack_rc.ReasonCode = MQTTSN2.ReasonCodes(MT.SUBACK, identifier=0x02)   # granted QoS 2
buf_rc = ack_rc.pack()
ack_rc2 = MQTTSN2.Subacks(buf_rc)
check("ReasonCode round-trip",  ack_rc2.ReasonCode.value == 0x02)
check("__eq__",                 ack_rc == ack_rc2)
check("wire length is 6",       len(buf_rc) == 6)

section("Subacks — alias and reason code")

ack3 = MQTTSN2.Subacks()
ack3.SubackFlags.TopicAliasFlag = True
ack3.SubackFlags.TopicType = 0
ack3.PacketId = 0x1234
ack3.TopicAlias = 0x0007
ack3.ReasonCode = MQTTSN2.ReasonCodes(MT.SUBACK, identifier=0x01)   # granted QoS 1
buf6 = ack3.pack()
ack4 = MQTTSN2.Subacks(buf6)
check("TopicAliasFlag round-trip", ack4.SubackFlags.TopicAliasFlag == True)
check("TopicAlias round-trip",     ack4.TopicAlias == 0x0007)
check("ReasonCode round-trip",     ack4.ReasonCode.value == 0x01)
check("__eq__",                    ack3 == ack4)
check("wire length is 8",          len(buf6) == 8)

section("Subacks — reserved bits rejected")

bad2 = bytes([5, MT.SUBACK, 0xF8, 0x00, 0x01])
check("reserved suback bits rejected",
      raises(lambda: MQTTSN2.Subacks(bad2)))


# ===========================================================================
# ReasonCodes
# ===========================================================================

RC = MQTTSN2.ReasonCodes

section("ReasonCodes — set by name and retrieve")

rc = RC(MT.CONNACK)
check("default is Success",       str(rc) == "Success")
rc.set("Unspecified error")
check("set Unspecified error",    rc.value == 0x80)
check("json() matches getName()", rc.json() == rc.getName())

section("ReasonCodes — pack/unpack round-trip")

rc2 = RC(MT.SUBACK, "Granted QoS 1")
buf = rc2.pack()
check("pack Granted QoS 1",       buf == bytes([0x01]))
rc3 = RC(MT.SUBACK, "Granted QoS 0")
rc3.unpack(buf)
check("unpack sets value",         rc3.value == 0x01)
check("unpack sets name",          str(rc3) == "Granted QoS 1")

section("ReasonCodes — code 0x00 per packet type")

check("CONNACK 0x00 is Success",
      str(RC(MT.CONNACK,    "Success"))              == "Success")
check("DISCONNECT 0x00 is Normal disconnection",
      str(RC(MT.DISCONNECT, "Normal disconnection")) == "Normal disconnection")
check("SUBACK 0x00 is Granted QoS 0",
      str(RC(MT.SUBACK,     "Granted QoS 0"))        == "Granted QoS 0")

section("ReasonCodes — all three QoS grants")

check("Granted QoS 0 = 0x00", RC(MT.SUBACK, "Granted QoS 0").value == 0x00)
check("Granted QoS 1 = 0x01", RC(MT.SUBACK, "Granted QoS 1").value == 0x01)
check("Granted QoS 2 = 0x02", RC(MT.SUBACK, "Granted QoS 2").value == 0x02)

section("ReasonCodes — construct by identifier")

rc4 = RC(MT.DISCONNECT, identifier=0x8B)
check("0x8B is Server shutting down", str(rc4) == "Server shutting down")

section("ReasonCodes — MQTT-SN-specific codes")

check("0x1A Topic Alias Exists",
      RC(MT.REGACK,     "Topic Alias Exists").value          == 0x1A)
check("0xF0 Unknown Topic Alias",
      RC(MT.REGACK,     "Unknown Topic Alias").value         == 0xF0)
check("0xF1 Congestion (CONNACK)",
      RC(MT.CONNACK,    "Congestion").value                  == 0xF1)
check("0xF1 Congestion (PUBACK)",
      RC(MT.PUBACK,     "Congestion").value                  == 0xF1)
check("0xF4 No Virtual Connection exists",
      RC(MT.DISCONNECT, "No Virtual Connection exists").value == 0xF4)
check("0xE7 Protection scheme invalid",
      RC(MT.DISCONNECT, "Protection scheme invalid").value   == 0xE7)
check("0xE8 Unknown Sender Id",
      RC(MT.DISCONNECT, "Unknown Sender Id").value           == 0xE8)
check("0xF2 Protection packet not supported",
      RC(MT.DISCONNECT, "Protection packet not supported").value       == 0xF2)
check("0xF3 Forwarder Encapsulation not supported",
      RC(MT.DISCONNECT, "Forwarder Encapsulation not supported").value == 0xF3)

section("ReasonCodes — 0xA0 Maximum connect time")

check("0xA0 Maximum connect time",
      RC(MT.DISCONNECT, "Maximum connect time").value == 0xA0)

section("ReasonCodes — codes shared with MQTT v5")

check("0x84 Unsupported protocol version",
      RC(MT.CONNACK, "Unsupported protocol version").value       == 0x84)
check("0x9E Shared subscription not supported",
      RC(MT.SUBACK,  "Shared subscription not supported").value  == 0x9E)
check("0xA2 Wildcard subscription not supported",
      RC(MT.SUBACK,  "Wildcard subscription not supported").value == 0xA2)
check("0xA1 Subscription identifiers not supported",
      RC(MT.SUBACK,  "Subscription identifiers not supported").value == 0xA1)

section("ReasonCodes — unpack via packet buffer")

buf2 = bytes([0xF1])
rc_u = RC(MT.PUBACK, "Success")
rc_u.unpack(buf2)
check("unpack 0xF1 on PUBACK gives Congestion",
      rc_u.value == 0xF1 and str(rc_u) == "Congestion")

section("ReasonCodes — invalid name/identifier rejected")

check("invalid name for packet type raises",
      raises(lambda: RC(MT.CONNACK, "Granted QoS 2")))
check("invalid identifier for packet type raises",
      raises(lambda: RC(MT.CONNACK, identifier=0x01)))


# ===========================================================================
# Pubacks / Pubrecs / Pubrels / Pubcomps
# ===========================================================================

ack_cases = [
  (MQTTSN2.Pubacks,  MT.PUBACK,  "Puback"),
  (MQTTSN2.Pubrecs,  MT.PUBREC,  "Pubrec"),
  (MQTTSN2.Pubrels,  MT.PUBREL,  "Pubrel"),
  (MQTTSN2.Pubcomps, MT.PUBCOMP, "Pubcomp"),
]

section("Acks — success round-trip (reason code omitted from wire)")

for cls, mtype, label in ack_cases:
  p = cls()
  p.PacketId = 0x1234
  buf = p.pack()
  p2 = cls(buf)
  check(f"{label} wire length is 4",   len(buf) == 4)
  check(f"{label} PacketId round-trip", p2.PacketId == 0x1234)
  check(f"{label} ReasonCode is 0",    p2.ReasonCode.value == 0)
  check(f"{label} __eq__",             p == p2)
  check(f"{label} __str__",
        str(p2) == f"{label} (PacketId=4660, ReasonCode=Success)")

section("Acks — non-zero reason code present on wire")

for cls, mtype, label in ack_cases:
  p = cls()
  p.PacketId = 7
  # 0xE6 "Only protection packet supported" is the only non-zero code valid
  # for all of PUBACK, PUBREC, PUBREL and PUBCOMP.
  p.ReasonCode = MQTTSN2.ReasonCodes(mtype, identifier=0xE6)
  buf = p.pack()
  p2 = cls(buf)
  check(f"{label} wire length is 5",    len(buf) == 5)
  check(f"{label} ReasonCode round-trip", p2.ReasonCode.value == 0xE6)
  check(f"{label} __eq__",               p == p2)

section("Acks — cross-type inequality")

pb = MQTTSN2.Pubacks(); pb.PacketId = 1
pr = MQTTSN2.Pubrecs(); pr.PacketId = 1
check("Puback != Pubrec with same fields", pb != pr)

section("Acks — wrong packet type rejected")

for cls, mtype, label in ack_cases:
  # send a buffer whose type byte is wrong for this class
  wrong_type = MT.PUBACK if mtype != MT.PUBACK else MT.PUBREC
  bad = bytes([4, wrong_type, 0x00, 0x01])
  check(f"{label} wrong type rejected",
        raises(lambda c=cls, b=bad: c(b)))


# ===========================================================================
# unpackPacket dispatch
# ===========================================================================

section("unpackPacket — dispatch for all implemented types")

dispatch_cases = [
  (MQTTSN2.Pubacks,   "PUBACK"),
  (MQTTSN2.Pubrecs,   "PUBREC"),
  (MQTTSN2.Pubrels,   "PUBREL"),
  (MQTTSN2.Pubcomps,  "PUBCOMP"),
  (MQTTSN2.Subscribes,"SUBSCRIBE"),
  (MQTTSN2.Subacks,   "SUBACK"),
]

# Subscribes needs a topic filter; Subacks and ack types need a PacketId
def make_packet(cls):
  if cls == MQTTSN2.Subscribes:
    p = cls()
    p.SubscribeFlags.TopicType = 3
    p.PacketId = 1
    p.TopicFilter = "test"
    return p
  if cls == MQTTSN2.Subacks:
    p = cls()
    p.PacketId = 1
    return p
  p = cls()
  p.PacketId = 1
  return p

for cls, label in dispatch_cases:
  p = make_packet(cls)
  buf = p.pack()
  p2 = MQTTSN2.unpackPacket(buf)
  check(f"unpackPacket {label}", isinstance(p2, cls))

check("unpackPacket unknown type returns None",
      MQTTSN2.unpackPacket(bytes([3, 0xFC, 0x00])) is None)


# ===========================================================================
# Results
# ===========================================================================

print(f"\n=== Results: {tests_run} run, {tests_passed} passed, {tests_failed} failed ===")
sys.exit(0 if tests_failed == 0 else 1)
