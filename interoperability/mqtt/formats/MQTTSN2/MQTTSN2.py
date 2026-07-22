"""
*******************************************************************
  Copyright (c) 2013, 2026 IBM Corp., Ian Craggs

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v2.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v20.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

 * AI Disclosure: This file was partly AI-generated. The AI-generated
 * portions are made available under CC0-1.0 and not subject to the
 * project's licence. The human contributor has reviewed and verified
 * that the code is correct.
 *
 * SPDX-License-Identifier: EPL-2.0 and CC0-1.0

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

"""
This is the packet serialization and deserialization for MQTT-SN version 2.0 

Assertions are used to validate incoming data, but are omitted from outgoing packets.  This is
so that the tests that use this package can send invalid data for error testing.

"""

import logging

logger = logging.getLogger('MQTT Broker')

# Low-level protocol interface

class MQTTSNException(Exception):
  pass

MAX_PACKET_SIZE = 2**16-1
MAX_PACKETID = 2**16-1

class PacketTypes:

  indexes = [x for x in range(1, 0X19)] + [0XFD, 0XFE, 0XFF]

  # Packet types
  CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, \
  SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, \
  PINGREQ, PINGRESP, DISCONNECT, \
  AUTH, REGISTER, REGACK, \
  PUBWOS, SLEEPREQ, SLEEPRESP, WAKEUP, \
  ADVERTISE, SEARCHGW, GWINFO, \
  FOWARDER_ENCAPSULATION, CONNECTION_ENCAPSULATION, PROTECTION_ENCAPSULATION = indexes

def PacketType(buffer):
  index = 1
  if buffer[0] == 1:
    index = 3
  return buffer[index]

class Packets(object):

  Names = ["Reserved", "Connect", "Connack", \
    "Publish", "Puback", "Pubrec", "Pubrel", "Pubcomp", \
    "Subscribe", "Suback", "Unsubscribe", "Unsuback", \
    "Pingreq", "Pingresp", "Disconnect", \
    "Auth", "Register", "Regack", \
    "Pubwos", "Sleepreq", "Sleepresp", "Wakeup", \
    "Advertise", "SearchGW", "GWInfo", \
    "Fowarder Encapsulation", "Connection Encapsulation", "Protection Encapsulation"]

  classNames = [name+'es' if name == "Publish" else
                name+'s' if name != "reserved" else name for name in Names]

  def pack(self):
    buffer = self.fh.pack(0)
    return buffer

  def __str__(self):
    return str(self.fh)

  def __eq__(self, packet):
    return self.fh == packet.fh if packet else False

  def __setattr__(self, name, value):
    if name not in self.names:
      raise MQTTSNException(name + " Attribute name must be one of "+str(self.names))
    object.__setattr__(self, name, value)


def getPacket(aSocket):
  "receive the next packet"
  buf = aSocket.recv(1) # get the first byte fixed header
  if buf == b"":
    return None
  if str(aSocket).find("[closed]") != -1:
    closed = True
  else:
    closed = False
  if closed:
    return None
  # now get the remaining length - the length field is the length of the
  # entire packet including the length field itself
  if buf[0] == 1: # indicates following two bytes are the length
    buf += aSocket.recv(2)
    remlength = buf[1]*256 + buf[2] - 3
  else:
    remlength = buf[0] - 1
  # receive the remaining length if there is any
  rest = bytes([])
  if remlength > 0:
    while len(rest) < remlength:
      rest += aSocket.recv(remlength-len(rest))
  assert len(rest) == remlength
  return buf + rest

def writeInt16(length):
  return bytes([length // 256, length % 256])

def readInt16(buf):
  return buf[0]*256 + buf[1]

def writeData(data):
  # data could be a string, or bytes.  If string, encode into bytes with utf-8
  return data if type(data) == type(b"") else bytes(data, "utf-8")

def writeLenData(data):
  # data could be a string, or bytes.  If string, encode into bytes with utf-8
  return writeInt16(len(data)) + (data if type(data) == type(b"") else bytes(data, "utf-8"))

class PacketLens:

  @staticmethod
  def encode(x):
    assert 2 <= x <= 65535
    buffer = b''
    if x < 256:
      buffer = bytes([x])
    else:
      buffer = bytes([1]) + writeInt16(x)
    return buffer

  @staticmethod
  def decode(buffer):
    if buffer[0] == 1:
      bytes = 3
      value = readInt16(buffer[1:])
    else:
      bytes = 1
      value = buffer[0]
    return (value, bytes)
  
class ReasonCodes:
  """
  MQTT-SN V2.0 Reason Codes (Section 2.3).
 
  Reason Codes less than 0x80 indicate successful completion; 0x80 and above
  indicate failure.  The same byte value can carry different names depending
  on the packet type (e.g. 0x00 is "Success" in a CONNACK but "Granted QoS 0"
  in a SUBACK), so the names dict is keyed by integer code and maps each
  human-readable name to the list of packet types for which that name is valid.
  """
 
  def __getName__(self, packetType, identifier):
    """
    used when displaying the reason code
    """
    assert identifier in self.names.keys(), identifier
    names = self.names[identifier]
    namelist = [name for name in names.keys() if packetType in names[name]]
    assert len(namelist) == 1
    return namelist[0]
 
  def getId(self, name):
    """
    used when setting the reason code for a packetType
    check that only valid codes for the packet are set
    """
    identifier = None
    for code in self.names.keys():
      if name in self.names[code].keys():
        if self.packetType in self.names[code][name]:
          identifier = code
        break
    assert identifier != None, name
    return identifier
 
  def set(self, name):
    self.value = self.getId(name)
 
  def unpack(self, buffer):
    name = self.__getName__(self.packetType, buffer[0])
    self.value = self.getId(name)
    return 1
 
  def getName(self):
    return self.__getName__(self.packetType, self.value)
 
  def __str__(self):
    return self.getName()
 
  def json(self):
    return self.getName()
 
  def pack(self):
    return bytes([self.value])
 
  def __init__(self, packetType, aName="Success", identifier=-1):
    self.packetType = packetType
    self.names = {
 
      # -----------------------------------------------------------------------
      # Success codes (< 0x80)
      # -----------------------------------------------------------------------
 
      0x00 : {
        "Success" : [
          PacketTypes.CONNACK, PacketTypes.UNSUBACK,
          PacketTypes.REGACK,
          PacketTypes.PUBACK, PacketTypes.PUBREC,
          PacketTypes.PUBREL, PacketTypes.PUBCOMP,
          PacketTypes.SLEEPRESP, PacketTypes.AUTH],
        "Normal disconnection" : [PacketTypes.DISCONNECT],
        "Granted QoS 0"        : [PacketTypes.SUBACK],
      },
      0x01 : { "Granted QoS 1" : [PacketTypes.SUBACK] },
      0x02 : { "Granted QoS 2" : [PacketTypes.SUBACK] },
 
      0x04 : { "Disconnect with will message" :
               [PacketTypes.DISCONNECT] },
 
      0x10 : { "No matching subscribers" :
               [PacketTypes.PUBACK, PacketTypes.PUBREC] },
      0x11 : { "No subscription existed" : [PacketTypes.UNSUBACK] },
 
      0x18 : { "Continue authentication" : [PacketTypes.AUTH] },
      0x19 : { "Re-authenticate"         : [PacketTypes.AUTH] },
 
      # MQTT-SN only: a Session or Predefined Topic Alias already exists
      0x1A : { "Topic Alias Exists" : [PacketTypes.REGACK] },
 
      # -----------------------------------------------------------------------
      # Error codes (>= 0x80)
      # -----------------------------------------------------------------------
 
      0x80 : { "Unspecified error" : [
               PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.DISCONNECT] },
 
      0x81 : { "Malformed packet" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x82 : { "Protocol error" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x83 : { "Implementation specific error" : [
               PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.REGACK, PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.DISCONNECT] },
 
      0x84 : { "Unsupported protocol version" : [PacketTypes.CONNACK] },
      0x85 : { "Client identifier not valid"  : [PacketTypes.CONNACK] },
      0x86 : { "Bad user name or password"    : [PacketTypes.CONNACK] },
 
      0x87 : { "Not authorized" : [
               PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.REGACK, PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.DISCONNECT] },
 
      0x88 : { "Server unavailable"   : [PacketTypes.CONNACK] },
      0x89 : { "Server busy"          :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x8A : { "Banned"               : [PacketTypes.CONNACK] },
      0x8B : { "Server shutting down" : [PacketTypes.DISCONNECT] },
      0x8C : { "Bad authentication method" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x8D : { "Keep alive timeout"   : [PacketTypes.DISCONNECT] },
      0x8E : { "Session taken over"   : [PacketTypes.DISCONNECT] },
 
      0x8F : { "Topic filter invalid" : [
               PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.DISCONNECT] },
      0x90 : { "Topic name invalid" : [
               PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.DISCONNECT] },
 
      0x91 : { "Packet identifier in use" : [
               PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.REGACK,
               PacketTypes.PINGRESP, PacketTypes.SLEEPRESP] },
      0x92 : { "Packet identifier not found" :
               [PacketTypes.PUBREL, PacketTypes.PUBCOMP] },
 
      0x93 : { "Receive maximum exceeded" : [PacketTypes.DISCONNECT] },
      0x94 : { "Topic alias invalid"      : [PacketTypes.DISCONNECT] },
      0x95 : { "Packet too large" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x96 : { "Packet rate too high"   : [PacketTypes.DISCONNECT] },
      0x97 : { "Quota exceeded" : [
               PacketTypes.REGACK, PacketTypes.SUBACK,
               PacketTypes.DISCONNECT] },
      0x98 : { "Administrative action"   : [PacketTypes.DISCONNECT] },
 
      0x99 : { "Payload format invalid" : [
               PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.DISCONNECT] },
 
      0x9A : { "Retain not supported" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x9B : { "QoS not supported" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x9C : { "Use another server" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x9D : { "Server moved" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
      0x9E : { "Shared subscription not supported" :
               [PacketTypes.SUBACK, PacketTypes.DISCONNECT] },
      0x9F : { "Connection rate exceeded" :
               [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
 
      # 0xA0 = 160 decimal; the spec table's hex column reads "0xAD" which is
      # a typo — the decimal ordering (0x9F, 0xA0, 0xA1, 0xA2) is unambiguous
      0xA0 : { "Maximum connect time"  : [PacketTypes.DISCONNECT] },
 
      0xA1 : { "Subscription identifiers not supported" :
               [PacketTypes.SUBACK, PacketTypes.DISCONNECT] },
      0xA2 : { "Wildcard subscription not supported" :
               [PacketTypes.SUBACK, PacketTypes.DISCONNECT] },
 
      # -----------------------------------------------------------------------
      # MQTT-SN-specific codes (>= 0xE6)
      # -----------------------------------------------------------------------
 
      # 0xE6: receiver expected a PROTECTION-encapsulated packet
      0xE6 : { "Only protection packet supported" : [
               PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.PUBREL, PacketTypes.PUBCOMP,
               PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.REGACK, PacketTypes.DISCONNECT] },
      0xE7 : { "Protection scheme invalid"         : [PacketTypes.DISCONNECT] },
      0xE8 : { "Unknown Sender Id"                 : [PacketTypes.DISCONNECT] },
 
      0xF0 : { "Unknown Topic Alias" : [
               PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.SUBACK, PacketTypes.UNSUBACK,
               PacketTypes.REGACK] },
      0xF1 : { "Congestion" : [
               PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
               PacketTypes.SUBACK, PacketTypes.REGACK] },
      0xF2 : { "Protection packet not supported"          : [PacketTypes.DISCONNECT] },
      0xF3 : { "Forwarder Encapsulation not supported"    : [PacketTypes.DISCONNECT] },
      0xF4 : { "No Virtual Connection exists"             : [PacketTypes.DISCONNECT] },
    }
    if identifier == -1:
      self.set(aName)
    else:
      self.value = identifier
      self.getName() # check it's good

class ConnectFlags:
  """
  Connect Flags byte structure (Section 3.1.2)

  Bit 7: Reserved (must be 0)
  Bit 6: Allow Server Suggested Values (SrvSugg)
  Bit 5: Allow Network Address Changes (NetAddr)
  Bit 4: Default Awake Packets Flag   (DAM)   — governs presence of DefaultAwakeMessages field
  Bit 3: Session Expiry Flag           (SessExp) — governs presence of SessionExpiryInterval field
  Bit 2: Authentication Flag           (Auth)  — governs presence of AuthMethod/AuthData fields
  Bit 1: Will Flag                     (Will)  — governs presence of WillFlags/WillTopic/WillPayload
  Bit 0: Clean Start
  """

  def __init__(self):
    self.CleanStart = False  # bit 0
    self.Will       = False  # bit 1 — also controls WillFlags byte and Will fields
    self.Auth       = False  # bit 2 — controls AuthMethod / AuthData fields
    self.SessExp    = False  # bit 3 — controls SessionExpiryInterval field
    self.DAM        = False  # bit 4 — controls DefaultAwakeMessages field
    self.NetAddr    = False  # bit 5
    self.SrvSugg    = False  # bit 6

  def __eq__(self, flags):
    return self.CleanStart == flags.CleanStart and \
           self.Will       == flags.Will and \
           self.Auth       == flags.Auth and \
           self.SessExp    == flags.SessExp and \
           self.DAM        == flags.DAM and \
           self.NetAddr    == flags.NetAddr and \
           self.SrvSugg    == flags.SrvSugg

  def __setattr__(self, name, value):
    names = ["CleanStart", "Will", "Auth", "SessExp", "DAM", "NetAddr", "SrvSugg"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "ConnectFlags(SrvSugg=" + str(self.SrvSugg) + \
           ", NetAddr="    + str(self.NetAddr) + \
           ", DAM="        + str(self.DAM) + \
           ", SessExp="    + str(self.SessExp) + \
           ", Auth="       + str(self.Auth) + \
           ", Will="       + str(self.Will) + \
           ", CleanStart=" + str(self.CleanStart) + ")"

  def pack(self):
    "pack data into string buffer ready for transmission down socket"
    return bytes([(self.SrvSugg << 6) | (self.NetAddr << 5) |
                  (self.DAM     << 4) | (self.SessExp << 3) |
                  (self.Auth    << 2) | (self.Will    << 1) |
                  self.CleanStart])

  def unpack(self, b0):
    "unpack data from string buffer into separate fields"
    assert ((b0 >> 7) & 0x01) == 0, \
        "[MQTT-SN-3.1.2-1] bit 7 of the connect flags is reserved and must be 0"
    self.SrvSugg    = ((b0 >> 6) & 0x01) == 1
    self.NetAddr    = ((b0 >> 5) & 0x01) == 1
    self.DAM        = ((b0 >> 4) & 0x01) == 1
    self.SessExp    = ((b0 >> 3) & 0x01) == 1
    self.Auth       = ((b0 >> 2) & 0x01) == 1
    self.Will       = ((b0 >> 1) & 0x01) == 1
    self.CleanStart = (b0 & 0x01) == 1
    return 1  # length of flags


class WillFlags:
  """
  Will Flags byte structure (Section 3.1.3).
  Present in CONNECT only when ConnectFlags.Will is set.

  Bits 7-5: Reserved (must be 0)
  Bit 4:    Will Retain
  Bits 3-2: Will QoS
  Bits 1-0: Will Topic Type
  """

  def __init__(self):
    self.WillTopicType = 0   # bits 1-0
    self.WillQoS       = 0   # bits 3-2
    self.WillRetain    = False  # bit 4

  def __eq__(self, flags):
    return self.WillTopicType == flags.WillTopicType and \
           self.WillQoS       == flags.WillQoS and \
           self.WillRetain    == flags.WillRetain

  def __setattr__(self, name, value):
    names = ["WillTopicType", "WillQoS", "WillRetain"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "WillFlags(WillRetain=" + str(self.WillRetain) + \
           ", WillQoS="       + str(self.WillQoS) + \
           ", WillTopicType=" + str(self.WillTopicType) + ")"

  def pack(self):
    return bytes([(self.WillRetain << 4) |
                  ((self.WillQoS & 0x03) << 2) |
                  (self.WillTopicType & 0x03)])

  def unpack(self, b0):
    assert ((b0 >> 5) & 0x07) == 0, \
        "[MQTT-SN-3.1.3] bits 7-5 of the will flags are reserved and must be 0"
    assert ((b0 >> 2) & 0x03) != 3, \
        "[MQTT-SN-3.1.3.2] Will QoS value 3 is a Malformed Packet"
    self.WillRetain    = ((b0 >> 4) & 0x01) == 1
    self.WillQoS       = (b0 >> 2) & 0x03
    self.WillTopicType = b0 & 0x03
    return 1  # length of flags


class ConnectProtection:
  """Carries the protection metadata from a protected CONNECT packet.

  Set on a Connects object by the broker's handleRequest() when the CONNECT
  arrived inside a Protection Encapsulation, so that connect() can record the
  session's protection requirements without needing extra ad-hoc attributes.

  Attributes
  ----------
  sender_id : bytes
      The 8-byte SenderIdentifier from the Protection Encapsulation header.
  scheme : int
      The ProtectionScheme byte (e.g. 0x00 for HMAC-SHA256).
  """

  def __init__(self, sender_id: bytes, scheme: int):
    self.sender_id = sender_id
    self.scheme    = scheme

  def __repr__(self):
    return (f"ConnectProtection(sender_id={self.sender_id.hex()!r}, "
            f"scheme=0x{self.scheme:02X})")


class Connects(Packets):
  """
  CONNECT packet (Section 3.1).

  Wire format (fields marked [opt] are present only when the corresponding flag is set):
    length + type(1) + ConnectFlags(1) + [WillFlags(1)] +
    PacketId(2) + ProtocolVersion(1) + KeepAlive(2) + MaxPacketSize(2) +
    [DefaultAwakeMessages(1)]         if DAM flag set +
    [SessionExpiryInterval(4)]        if SessExp flag set +
    [WillTopicAliasOrLen(2)]          if Will flag set +
    [WillTopicName(utf-8)]            if Will flag set and WillTopicType==3 +
    [WillPayloadLen(2)]               if Will flag set +
    [WillPayload(binary)]             if Will flag set +
    [AuthMethodLen(1)]                if Auth flag set +
    [AuthMethod(utf-8)]               if Auth flag set +
    [AuthDataLen(2)]                  if Auth flag set +
    [AuthData(binary)]                if Auth flag set +
    [ClientId(utf-8, remainder)]      optional, inferred from packet length
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType",
          "ConnectFlags", "WillFlags",
          "PacketId", "ProtocolVersion", "KeepAlive", "MaxPacketSize",
          "DefaultAwakeMessages", "SessionExpiryInterval",
          "WillTopic", "WillPayload",
          "AuthMethod", "AuthData", "ClientId",
          "ConnectProtection"])
    self.packetType = PacketTypes.CONNECT

    self.ConnectFlags = ConnectFlags()
    self.WillFlags    = WillFlags()  # only transmitted when ConnectFlags.Will is set

    self.PacketId              = 1
    self.ProtocolVersion       = 2    # 0x02 = MQTT-SN 2.0
    self.KeepAlive             = 60
    self.MaxPacketSize         = 0    # 0 = unlimited
    self.DefaultAwakeMessages  = None  # None = absent (DAM flag clear)
    self.SessionExpiryInterval = None  # None = absent (SessExp flag clear)
    self.WillTopic             = None  # None = absent (Will flag clear)
    self.WillPayload           = None  # None = absent (Will flag clear)
    self.AuthMethod            = None  # None = absent (Auth flag clear)
    self.AuthData              = None  # None = absent (Auth flag clear)
    self.ClientId              = ""
    self.ConnectProtection     = None  # set by broker when CONNECT arrived protected
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    cf = self.ConnectFlags
    # Derive flags from which optional fields are present
    cf.Will    = (self.WillTopic is not None)
    cf.Auth    = (self.AuthMethod is not None)
    cf.SessExp = (self.SessionExpiryInterval is not None)
    cf.DAM     = (self.DefaultAwakeMessages is not None)

    body = bytes([PacketTypes.CONNECT]) + cf.pack()
    if cf.Will:
      body += self.WillFlags.pack()
    body += writeInt16(self.PacketId)
    body += bytes([self.ProtocolVersion])
    body += writeInt16(self.KeepAlive)
    body += writeInt16(self.MaxPacketSize)
    if cf.DAM:
      body += bytes([self.DefaultAwakeMessages])
    if cf.SessExp:
      sei = self.SessionExpiryInterval
      body += bytes([(sei >> 24) & 0xFF, (sei >> 16) & 0xFF,
                     (sei >> 8)  & 0xFF,  sei        & 0xFF])
    if cf.Will:
      will_topic = writeData(self.WillTopic) if self.WillTopic else b""
      if self.WillFlags.WillTopicType == 3:  # Topic Name: write len + name
        body += writeInt16(len(will_topic)) + will_topic
      else:                                   # Alias: write 2-byte alias value
        body += writeInt16(len(will_topic))  # alias value stored in WillTopic as int
      will_payload = writeData(self.WillPayload) if self.WillPayload else b""
      body += writeInt16(len(will_payload)) + will_payload
    if cf.Auth:
      method_bytes = writeData(self.AuthMethod)
      body += bytes([len(method_bytes)]) + method_bytes
      data_bytes = writeData(self.AuthData) if self.AuthData else b""
      body += writeInt16(len(data_bytes)) + data_bytes
    body += writeData(self.ClientId)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.CONNECT
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      pos = lenlen + 1  # byte after packet type

      # Connect Flags
      self.ConnectFlags.unpack(buffer[pos])
      pos += 1
      cf = self.ConnectFlags

      # Will Flags (present only when Will flag is set)
      if cf.Will:
        self.WillFlags.unpack(buffer[pos])
        pos += 1
      else:
        self.WillFlags = WillFlags()  # reset to defaults

      # Fixed mandatory fields
      self.PacketId        = readInt16(buffer[pos:]);  pos += 2
      self.ProtocolVersion = buffer[pos];              pos += 1
      self.KeepAlive       = readInt16(buffer[pos:]);  pos += 2
      self.MaxPacketSize   = readInt16(buffer[pos:]);  pos += 2

      # Optional: Default Awake Packets (1 byte)
      if cf.DAM:
        self.DefaultAwakeMessages = buffer[pos];  pos += 1
      else:
        self.DefaultAwakeMessages = None

      # Optional: Session Expiry Interval (4 bytes)
      if cf.SessExp:
        self.SessionExpiryInterval = (buffer[pos]   << 24) | (buffer[pos+1] << 16) | \
                                     (buffer[pos+2] <<  8) |  buffer[pos+3]
        pos += 4
      else:
        self.SessionExpiryInterval = None

      # Optional: Will Topic + Will Payload
      if cf.Will:
        will_alias_or_len = readInt16(buffer[pos:]);  pos += 2
        if self.WillFlags.WillTopicType == 3:  # Topic Name
          self.WillTopic = buffer[pos:pos + will_alias_or_len].decode("utf-8")
          pos += will_alias_or_len
        else:
          self.WillTopic = will_alias_or_len  # store alias as integer
        will_payload_len = readInt16(buffer[pos:]);  pos += 2
        self.WillPayload = buffer[pos:pos + will_payload_len]
        pos += will_payload_len
      else:
        self.WillTopic   = None
        self.WillPayload = None

      # Optional: Auth Method + Auth Data
      if cf.Auth:
        method_len = buffer[pos];  pos += 1
        self.AuthMethod = buffer[pos:pos + method_len].decode("utf-8")
        pos += method_len
        data_len = readInt16(buffer[pos:]);  pos += 2
        self.AuthData = buffer[pos:pos + data_len]
        pos += data_len
      else:
        self.AuthMethod = None
        self.AuthData   = None

      # Optional: Client Identifier (remainder of packet)
      if pos < packetlen:
        self.ClientId = buffer[pos:packetlen].decode("utf-8")
      else:
        self.ClientId = ""

    except:
      logger.exception("Validating connect packet")
      raise

  def __str__(self):
    s = "Connect (" + str(self.ConnectFlags) + \
        ", PacketId="        + str(self.PacketId) + \
        ", ProtocolVersion=" + str(self.ProtocolVersion) + \
        ", KeepAlive="       + str(self.KeepAlive) + \
        ", MaxPacketSize="   + str(self.MaxPacketSize)
    if self.ConnectFlags.DAM:
      s += ", DefaultAwakeMessages=" + str(self.DefaultAwakeMessages)
    if self.ConnectFlags.SessExp:
      s += ", SessionExpiryInterval=" + str(self.SessionExpiryInterval)
    if self.ConnectFlags.Will:
      s += ", " + str(self.WillFlags) + \
           ", WillTopic=" + str(self.WillTopic) + \
           ", WillPayload=" + str(self.WillPayload)
    if self.ConnectFlags.Auth:
      s += ", AuthMethod=" + str(self.AuthMethod)
    s += ", ClientId=" + str(self.ClientId) + ")"
    return s

  def __eq__(self, packet):
    return self.ConnectFlags == packet.ConnectFlags and \
           self.PacketId     == packet.PacketId and \
           self.ProtocolVersion       == packet.ProtocolVersion and \
           self.KeepAlive             == packet.KeepAlive and \
           self.MaxPacketSize         == packet.MaxPacketSize and \
           self.DefaultAwakeMessages  == packet.DefaultAwakeMessages and \
           self.SessionExpiryInterval == packet.SessionExpiryInterval and \
           self.WillTopic             == packet.WillTopic and \
           self.WillPayload           == packet.WillPayload and \
           self.AuthMethod            == packet.AuthMethod and \
           self.AuthData              == packet.AuthData and \
           self.ClientId              == packet.ClientId

class Connacks(Packets):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["packetType", "Flags", "PacketId", "ReasonCode", "AssignedClientId"])
    self.packetType = PacketTypes.CONNACK
    self.Flags = 0
    self.PacketId = 0
    self.ReasonCode = ReasonCodes(PacketTypes.CONNACK)
    self.AssignedClientId = ""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    msglen = 6 + len(self.AssignedClientId)
    buffer = bytes([msglen, PacketTypes.CONNACK]) + bytes([self.Flags]) +\
      writeInt16(self.PacketId) + self.ReasonCode.pack() + writeData(self.AssignedClientId)
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 3
    assert PacketType(buffer) == PacketTypes.CONNACK
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      # self.ConnectFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      self.ReasonCode = ReasonCodes(PacketTypes.CONNACK)
      self.ReasonCode.unpack(buffer[lenlen + 4:])
    except:
      logger.exception("Validating connack packet")
      raise

  def __str__(self):
    return "Connack (" +\
        ", PacketId="+str(self.PacketId) +\
        ", ReasonCode="+str(self.ReasonCode) + ")"

  def __eq__(self, packet):
    rc = self.ReasonCode.value == packet.ReasonCode.value
    return rc
  
  """
MQTT-SN 2.0 PublishFlags class

This class represents the flags byte in MQTT-SN 2.0 PUBLISH packets.

Bit layout:
  Bit 7:    DUP (Duplicate flag - for QoS 2 retransmissions)
  Bits 6-5: QoS (Quality of Service: 0, 1, or 2)
  Bit 4:    RETAIN (Retained message flag)
  Bits 3-2: Reserved (must be 0)
  Bits 1-0: TopicType (Topic Type: 0=Name, 1=Predefined, 2=Session, 3=Name)
"""

class MQTTSNException(Exception):
    pass


class PublishFlags:
    """
    MQTT-SN 2.0 PUBLISH Flags class
    
    Attributes:
        DUP (bool): Duplicate delivery flag (bit 7)
        QoS (int): Quality of Service level 0, 1, or 2 (bits 6-5)
        RETAIN (bool): Retain flag (bit 4)
        TopicType (int): Topic type 0-3 (bits 1-0)
    """
    
    # Topic Type constants
    TOPIC_TYPE_SESSION = 0         # 0b00 - Session Topic Alias
    TOPIC_TYPE_PREDEFINED = 1      # 0b01 - Predefined Topic Alias
    TOPIC_TYPE_RESERVED = 2        # 0b10 - Reserved
    TOPIC_TYPE_NAME = 3            # 0b11 - Topic Name
    
    def __init__(self):
        """Initialize PublishFlags with default values"""
        self.DUP = False        # 1 bit - Duplicate delivery flag
        self.QoS = 0            # 2 bits - Quality of Service (0, 1, or 2)
        self.RETAIN = False     # 1 bit - Retain flag
        self.TopicType = 0      # 2 bits - Topic Type
    
    def __eq__(self, other):
        """Compare two PublishFlags objects for equality"""
        if not isinstance(other, PublishFlags):
            return False
        return (self.DUP == other.DUP and
                self.QoS == other.QoS and
                self.RETAIN == other.RETAIN and
                self.TopicType == other.TopicType)
    
    def __setattr__(self, name, value):
        """Validate attribute names before setting"""
        valid_names = ["DUP", "QoS", "RETAIN", "TopicType"]
        if name not in valid_names:
            raise MQTTSNException(f"{name} - Attribute name must be one of {valid_names}")
        object.__setattr__(self, name, value)
    
    def __str__(self):
        """String representation of the PublishFlags"""
        return (f"PublishFlags(DUP={self.DUP}, "
                f"QoS={self.QoS}, "
                f"RETAIN={self.RETAIN}, "
                f"TopicType={self.TopicType})")
    
    def __repr__(self):
        """Repr representation of the PublishFlags"""
        return self.__str__()
    
    def pack(self):
        """
        Pack flags into a single byte for transmission
        
        Returns:
            bytes: Single byte with packed flags
            
        Raises:
            MQTTSNException: If QoS is invalid (not 0, 1, or 2)
        """
        # Validate QoS
        if self.QoS not in (0, 1, 2):
            raise MQTTSNException(f"Invalid QoS value: {self.QoS}. Must be 0, 1, or 2")
        
        # Validate TopicType
        if self.TopicType not in (0, 1, 2, 3):
            raise MQTTSNException(f"Invalid TopicType: {self.TopicType}. Must be 0-3")
        
        # Pack the flags byte
        # Bit 7: DUP
        # Bits 6-5: QoS
        # Bit 4: RETAIN
        # Bits 3-2: Reserved (0)
        # Bits 1-0: TopicType
        flags_byte = (
            (1 if self.DUP else 0) << 7 |
            (self.QoS & 0x03) << 5 |
            (1 if self.RETAIN else 0) << 4 |
            (self.TopicType & 0x03)
        )
        
        return bytes([flags_byte])
    
    def unpack(self, byte_value):
        """
        Unpack a flags byte into separate flag fields
        
        Args:
            byte_value (int or bytes): The flags byte to unpack
            
        Returns:
            int: Number of bytes consumed (always 1)
            
        Raises:
            MQTTSNException: If reserved bits are not 0 (malformed packet)
        """
        # Handle both int and bytes input
        if isinstance(byte_value, bytes):
            if len(byte_value) == 0:
                raise MQTTSNException("Empty byte value provided to unpack")
            b0 = byte_value[0]
        else:
            b0 = byte_value
        
        # Extract flags from byte
        self.DUP = ((b0 >> 7) & 0x01) == 1
        self.QoS = (b0 >> 5) & 0x03
        self.RETAIN = ((b0 >> 4) & 0x01) == 1
        self.TopicType = b0 & 0x03
        
        # Check reserved bits (bits 3-2) - must be 0
        reserved_bits = (b0 >> 2) & 0x03
        if reserved_bits != 0:
            raise MQTTSNException(
                f"Malformed PUBLISH packet: reserved bits are not 0 (value: {reserved_bits})"
            )
        
        # Validate QoS value (0, 1, or 2 are valid; 3 is reserved)
        if self.QoS == 3:
            raise MQTTSNException("Invalid QoS value 3 in PUBLISH flags")
        
        return 1  # Return number of bytes consumed
    
    def validate(self):
        """
        Validate the flags according to MQTT-SN 2.0 specification
        
        Returns:
            bool: True if valid
            
        Raises:
            MQTTSNException: If validation fails
        """
        if self.QoS not in (0, 1, 2):
            raise MQTTSNException(f"Invalid QoS: {self.QoS}. Must be 0, 1, or 2")
        
        if self.TopicType not in (0, 1, 3):
            raise MQTTSNException(f"Invalid TopicType: {self.TopicType}. Must be 0-3")
        
        # DUP should only be set for QoS 2 retransmissions
        # But this is more of a protocol rule than a format rule
        # We'll allow it for flexibility
        
        return True

class Publishes(Packets):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["Flags", "TopicAlias", "TopicName", "PacketId", "Data"])
    object.__setattr__(self, "packetType", PacketTypes.PUBLISH)
    self.Flags = PublishFlags()
    self.TopicAlias = 0
    self.TopicName = None
    self.PacketId = 0
    self.Data = b""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([self.packetType]) + self.Flags.pack()
    if self.Flags.QoS != 0:
      body += writeInt16(self.PacketId)
    if self.Flags.TopicType == self.Flags.TOPIC_TYPE_NAME:
      body += writeLenData(self.TopicName)
    else:
      body += writeInt16(self.TopicAlias)
    body += writeData(self.Data)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBLISH
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      curlen = lenlen + 1 # add byte for packet type
      self.Flags.unpack(buffer[curlen])
      curlen += 1
      if self.Flags.QoS != 0:
        self.PacketId = readInt16(buffer[curlen:])
        curlen += 2
      if self.Flags.TopicType == self.Flags.TOPIC_TYPE_NAME:
        namelen = readInt16(buffer[curlen:])
        curlen += 2
        self.TopicName = buffer[curlen:curlen + namelen]
        curlen += namelen 
      else:
        self.TopicAlias = readInt16(buffer[curlen:])
        curlen += 2
      self.Data = buffer[curlen:]
    except:
      logger.exception("Validating publish packet")
      raise

  def __str__(self):
    output = "Publish (" + str(self.Flags)
    if self.Flags.TopicType == self.Flags.TOPIC_TYPE_NAME:
      output += ", TopicName="+str(self.TopicName)
    else:
      output += ", TopicAlias="+str(self.TopicAlias)
    if self.Flags.QoS != 0:
      output += ", PacketId=" + str(self.PacketId)
    output += ", Data="+str(self.Data) + ")"
    return output

  def __eq__(self, packet):
    if self.Flags.TopicType == self.Flags.TOPIC_TYPE_NAME:
      topic_eq = self.TopicName == packet.TopicName
    else:
      topic_eq = self.TopicAlias == packet.TopicAlias
    return self.Flags    == packet.Flags and \
           topic_eq                      and \
           self.PacketId == packet.PacketId and \
           self.Data     == packet.Data
  

class Acks(Packets):
  """
  Base class for PUBACK, PUBREC, PUBREL, and PUBCOMP (Sections 3.6.4-3.6.7).
 
  All four share the same wire format:
    length(1) + type(1) + packetid(2) + [reasoncode(1, optional)]
 
  The ReasonCode field is absent from the wire when its value is 0x00
  (success); the receiver infers success from the reduced packet length.
  """
 
  def __init__(self, packetType, buffer=None):
    object.__setattr__(self, "names", ["PacketId", "ReasonCode"])
    object.__setattr__(self, "packetType", packetType)
    self.PacketId = 0
    self.ReasonCode = ReasonCodes(packetType)
    if buffer != None:
      self.unpack(buffer)
 
  def pack(self):
    # body: type(1) + packetid(2) + [reasoncode(1, if not success)]
    body = bytes([self.packetType]) + writeInt16(self.PacketId)
    if self.ReasonCode.value != 0:
      body += self.ReasonCode.pack()
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body
 
  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == self.packetType
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      # ReasonCode is optional: present when packet length allows (Section 3.6.4-7)
      if packetlen > lenlen + 3:
        self.ReasonCode = ReasonCodes(self.packetType)
        self.ReasonCode.unpack(buffer[lenlen + 3:])
      else:
        self.ReasonCode = ReasonCodes(self.packetType)  # absent means success (0x00)
    except:
      logger.exception("Validating %s packet" % self.__class__.__name__)
      raise
 
  def __str__(self):
    # class names are e.g. "Pubacks"; strip the trailing 's' for the label
    return self.__class__.__name__[:-1] + \
           " (PacketId=" + str(self.PacketId) + \
           ", ReasonCode=" + str(self.ReasonCode) + ")"
 
  def __eq__(self, packet):
    return self.packetType == packet.packetType and \
           self.PacketId == packet.PacketId and \
           self.ReasonCode.value == packet.ReasonCode.value
 
 
class Pubacks(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, PacketTypes.PUBACK, buffer)
 
class Pubrecs(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, PacketTypes.PUBREC, buffer)
 
class Pubrels(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, PacketTypes.PUBREL, buffer)
 
class Pubcomps(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, PacketTypes.PUBCOMP, buffer)
 

class SubscribeFlags:
  """
  Subscribe Flags byte structure (Section 3.7.2)
 
  Bit 7:    No Local
  Bits 6-5: QoS
  Bit 4:    Retain as Published (RaP)
  Bits 3-2: Retain Handling
  Bits 1-0: Topic Type
  """
 
  def __init__(self, QoS = 0, TopicType = 3, RetainHandling = 0, RaP = False, NoLocal = False):
    self.TopicType = TopicType           # 2 bits: 0=Session, 1=Predefined, 3=Filter
    self.RetainHandling = RetainHandling # 2 bits: 0, 1, or 2
    self.RaP = RaP                       # 1 bit: Retain as Published
    self.QoS = QoS                       # 2 bits: 0, 1, or 2
    self.NoLocal = NoLocal               # 1 bit
 
  def __eq__(self, flags):
    return self.TopicType == flags.TopicType and \
           self.RetainHandling == flags.RetainHandling and \
           self.RaP == flags.RaP and \
           self.QoS == flags.QoS and \
           self.NoLocal == flags.NoLocal
 
  def __setattr__(self, name, value):
    names = ["TopicType", "RetainHandling", "RaP", "QoS", "NoLocal"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of "+str(names))
    object.__setattr__(self, name, value)
 
  def __str__(self):
    return "SubscribeFlags(NoLocal=" + str(self.NoLocal) + \
           ", QoS=" + str(self.QoS) + \
           ", RaP=" + str(self.RaP) + \
           ", RetainHandling=" + str(self.RetainHandling) + \
           ", TopicType=" + str(self.TopicType) + ")"
 
  def pack(self):
    "pack data into string buffer ready for transmission down socket"
    buffer = bytes([(self.NoLocal << 7) |
                    (self.QoS << 5) |
                    (self.RaP << 4) |
                    (self.RetainHandling << 2) |
                    self.TopicType])
    return buffer
 
  def unpack(self, b0):
    "unpack data from string buffer into separate fields"
    assert (b0 & 0x03) != 2, \
        "[MQTT-SN-3.7.2] Topic Type value 2 (0b10) is reserved"
    assert ((b0 >> 5) & 0x03) != 3, \
        "[MQTT-SN-3.7.2.4] QoS value 3 is a Protocol Error"
    assert ((b0 >> 2) & 0x03) != 3, \
        "[MQTT-SN-3.7.2.2] Retain Handling value 3 is a Protocol Error"
    self.NoLocal        = ((b0 >> 7) & 0x01) == 1
    self.QoS            = (b0 >> 5) & 0x03
    self.RaP            = ((b0 >> 4) & 0x01) == 1
    self.RetainHandling = (b0 >> 2) & 0x03
    self.TopicType      = b0 & 0x03
    return 1  # length of flags

class Subscribes(Packets):
 
  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType",
          "SubscribeFlags",
          "PacketId", "TopicAlias", "TopicFilter"])
    self.packetType = PacketTypes.SUBSCRIBE
    self.SubscribeFlags = SubscribeFlags()
    self.PacketId = 0
    self.TopicAlias = 0    # used when TopicType is Session (0) or Predefined (1)
    self.TopicFilter = ""  # used when TopicType is Filter (3)
    if buffer != None:
      self.unpack(buffer)
 
  def pack(self):
    topic_type = self.SubscribeFlags.TopicType
    if topic_type == 3:  # Topic Filter
      topic_data = writeData(self.TopicFilter)
      msglen = 1 + 1 + 1 + 2 + len(topic_data)  # len(1)+type(1)+flags(1)+packetid(2)+filter
    else:                # Session (0) or Predefined (1): 2-byte alias
      topic_data = writeInt16(self.TopicAlias)
      msglen = 1 + 1 + 1 + 2 + 2               # len(1)+type(1)+flags(1)+packetid(2)+alias(2)
    buffer = bytes([msglen, PacketTypes.SUBSCRIBE]) + \
             self.SubscribeFlags.pack() + \
             writeInt16(self.PacketId) + \
             topic_data
    return buffer
 
  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SUBSCRIBE
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.SubscribeFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      topic_type = self.SubscribeFlags.TopicType
      if topic_type == 3:  # Topic Filter: fills to end of packet
        self.TopicFilter = buffer[lenlen + 4:packetlen].decode("utf-8")
        self.TopicAlias = 0
      else:                # Session (0) or Predefined (1): 2-byte alias
        self.TopicAlias = readInt16(buffer[lenlen + 4:])
        self.TopicFilter = ""
    except:
      logger.exception("Validating subscribe packet")
      raise
 
  def __str__(self):
    return "Subscribe (" + str(self.SubscribeFlags) + \
           ", PacketId=" + str(self.PacketId) + \
           ", TopicAlias=" + str(self.TopicAlias) + \
           ", TopicFilter=" + str(self.TopicFilter) + ")"
 
  def __eq__(self, packet):
    return self.SubscribeFlags == packet.SubscribeFlags and \
           self.PacketId == packet.PacketId and \
           self.TopicAlias == packet.TopicAlias and \
           self.TopicFilter == packet.TopicFilter
 
class SubackFlags:
  """
  SUBACK Flags byte structure (Section 3.8.2)
 
  Bits 7-3: Reserved (must be 0)
  Bit 2:    Topic Alias Flag
  Bits 1-0: Topic Type
  """
 
  def __init__(self):
    self.TopicType = 0           # 2 bits: 0=Session, 1=Predefined
    self.TopicAliasFlag = False  # 1 bit: 1 if Topic Alias field is present
 
  def __eq__(self, flags):
    return self.TopicType == flags.TopicType and \
           self.TopicAliasFlag == flags.TopicAliasFlag
 
  def __setattr__(self, name, value):
    names = ["TopicType", "TopicAliasFlag"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of "+str(names))
    object.__setattr__(self, name, value)
 
  def __str__(self):
    return "SubackFlags(TopicType=" + str(self.TopicType) + \
           ", TopicAliasFlag=" + str(self.TopicAliasFlag) + ")"
 
  def pack(self):
    "pack data into string buffer ready for transmission down socket"
    buffer = bytes([(self.TopicAliasFlag << 2) | self.TopicType])
    return buffer
 
  def unpack(self, b0):
    "unpack data from string buffer into separate fields"
    assert ((b0 >> 3) & 0x1F) == 0, \
        "[MQTT-SN-3.8.2-1] bits 7-3 of the suback flags are reserved and must be 0"
    self.TopicAliasFlag = ((b0 >> 2) & 0x01) == 1
    self.TopicType      = b0 & 0x03
    return 1  # length of flags
 
class Subacks(Packets):
 
  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType",
          "SubackFlags",
          "PacketId", "TopicAlias", "ReasonCode"])
    self.packetType = PacketTypes.SUBACK
    self.SubackFlags = SubackFlags()
    self.PacketId = 0
    self.TopicAlias = 0    # present only when SubackFlags.TopicAliasFlag is True
    self.ReasonCode = ReasonCodes(PacketTypes.SUBACK, "Granted QoS 0")
    if buffer != None:
      self.unpack(buffer)
 
  def pack(self):
    # body: type(1) + flags(1) + packetid(2) + [alias(2)] + [reason_code(1)]
    body = bytes([PacketTypes.SUBACK]) + \
           self.SubackFlags.pack() + \
           writeInt16(self.PacketId)
    if self.SubackFlags.TopicAliasFlag:
      body += writeInt16(self.TopicAlias)
    # Reason Code is optional: omit when success and no topic alias (Section 3.8.5)
    if self.ReasonCode.value != 0 or self.SubackFlags.TopicAliasFlag:
      body += self.ReasonCode.pack()
    msglen = 1 + len(body)  # length field(1) + body
    buffer = bytes([msglen]) + body
    return buffer
 
  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SUBACK
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.SubackFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4  # next byte after packetid
      if self.SubackFlags.TopicAliasFlag:
        self.TopicAlias = readInt16(buffer[pos:])
        pos += 2
      else:
        self.TopicAlias = 0
      # Reason Code: present when packet is long enough (Section 3.8.5)
      if pos < packetlen:
        self.ReasonCode = ReasonCodes(PacketTypes.SUBACK, "Granted QoS 0")
        self.ReasonCode.unpack(buffer[pos:])
      else:
        self.ReasonCode = ReasonCodes(PacketTypes.SUBACK, "Granted QoS 0")  # absent means QoS 0 / success
    except:
      logger.exception("Validating suback packet")
      raise
 
  def __str__(self):
    return "Suback (" + str(self.SubackFlags) + \
           ", PacketId=" + str(self.PacketId) + \
           ", TopicAlias=" + str(self.TopicAlias) + \
           ", ReasonCode=" + str(self.ReasonCode) + ")"
 
  def __eq__(self, packet):
    return self.SubackFlags == packet.SubackFlags and \
           self.PacketId == packet.PacketId and \
           self.TopicAlias == packet.TopicAlias and \
           self.ReasonCode.value == packet.ReasonCode.value
 
 
class UnsubscribeFlags:
  """
  UNSUBSCRIBE Flags byte structure (Section 3.9.2)

  Bits 7-2: Reserved (must be 0)
  Bits 1-0: Topic Type
  """

  def __init__(self):
    self.TopicType = 0   # 2 bits: 0=Session, 1=Predefined, 3=Filter

  def __eq__(self, flags):
    return self.TopicType == flags.TopicType

  def __setattr__(self, name, value):
    names = ["TopicType"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "UnsubscribeFlags(TopicType=" + str(self.TopicType) + ")"

  def pack(self):
    "pack data into string buffer ready for transmission down socket"
    return bytes([self.TopicType & 0x03])

  def unpack(self, b0):
    "unpack data from string buffer into separate fields"
    assert ((b0 >> 2) & 0x3F) == 0, \
        "[MQTT-SN-3.9.2-1] bits 7-2 of the unsubscribe flags are reserved and must be 0"
    self.TopicType = b0 & 0x03
    return 1  # length of flags


class Unsubscribes(Packets):
  """
  UNSUBSCRIBE packet (Section 3.9).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               topic_alias(2)  [TopicType 0 or 1]  OR
               topic_filter    [TopicType 3, fills to end of packet]
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType",
          "UnsubscribeFlags",
          "PacketId", "TopicAlias", "TopicFilter"])
    self.packetType = PacketTypes.UNSUBSCRIBE
    self.UnsubscribeFlags = UnsubscribeFlags()
    self.PacketId = 0
    self.TopicAlias = 0    # used when TopicType is Session (0) or Predefined (1)
    self.TopicFilter = ""  # used when TopicType is Filter (3)
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    topic_type = self.UnsubscribeFlags.TopicType
    if topic_type == 3:  # Topic Filter
      topic_data = writeData(self.TopicFilter)
      msglen = 1 + 1 + 1 + 2 + len(topic_data)  # len(1)+type(1)+flags(1)+packetid(2)+filter
    else:                # Session (0) or Predefined (1): 2-byte alias
      topic_data = writeInt16(self.TopicAlias)
      msglen = 1 + 1 + 1 + 2 + 2               # len(1)+type(1)+flags(1)+packetid(2)+alias(2)
    buffer = bytes([msglen, PacketTypes.UNSUBSCRIBE]) + \
             self.UnsubscribeFlags.pack() + \
             writeInt16(self.PacketId) + \
             topic_data
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.UNSUBSCRIBE
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.UnsubscribeFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      topic_type = self.UnsubscribeFlags.TopicType
      if topic_type == 3:  # Topic Filter: fills to end of packet
        self.TopicFilter = buffer[lenlen + 4:packetlen].decode("utf-8")
        self.TopicAlias = 0
      else:                # Session (0) or Predefined (1): 2-byte alias
        self.TopicAlias = readInt16(buffer[lenlen + 4:])
        self.TopicFilter = ""
    except:
      logger.exception("Validating unsubscribe packet")
      raise

  def __str__(self):
    return "Unsubscribe (" + str(self.UnsubscribeFlags) + \
           ", PacketId=" + str(self.PacketId) + \
           ", TopicAlias=" + str(self.TopicAlias) + \
           ", TopicFilter=" + str(self.TopicFilter) + ")"

  def __eq__(self, packet):
    return self.UnsubscribeFlags == packet.UnsubscribeFlags and \
           self.PacketId == packet.PacketId and \
           self.TopicAlias == packet.TopicAlias and \
           self.TopicFilter == packet.TopicFilter


class Unsubacks(Packets):
  """
  UNSUBACK packet (Section 3.10).

  Wire format: length + type(1) + packetid(2) + [reasoncode(1, optional)]

  The ReasonCode is absent from the wire when its value is 0x00 (success);
  the receiver infers success from the reduced packet length.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["packetType", "PacketId", "ReasonCode"])
    self.packetType = PacketTypes.UNSUBACK
    self.PacketId = 0
    self.ReasonCode = ReasonCodes(PacketTypes.UNSUBACK)
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + packetid(2) + [reasoncode(1, if not success)]
    body = bytes([PacketTypes.UNSUBACK]) + writeInt16(self.PacketId)
    if self.ReasonCode.value != 0:
      body += self.ReasonCode.pack()
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.UNSUBACK
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      # ReasonCode is optional: present when packet length allows (Section 3.10.3)
      if packetlen > lenlen + 3:
        self.ReasonCode = ReasonCodes(PacketTypes.UNSUBACK)
        self.ReasonCode.unpack(buffer[lenlen + 3:])
      else:
        self.ReasonCode = ReasonCodes(PacketTypes.UNSUBACK)  # absent means success (0x00)
    except:
      logger.exception("Validating unsuback packet")
      raise

  def __str__(self):
    return "Unsuback (PacketId=" + str(self.PacketId) + \
           ", ReasonCode=" + str(self.ReasonCode) + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId and \
           self.ReasonCode.value == packet.ReasonCode.value


class Pingreqs(Packets):
  """
  PINGREQ packet (Section 3.11).

  Wire format: length + type(1) + packetid(2)
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["packetType", "PacketId"])
    self.packetType = PacketTypes.PINGREQ
    self.PacketId = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + packetid(2)
    body = bytes([PacketTypes.PINGREQ]) + writeInt16(self.PacketId)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PINGREQ
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
    except:
      logger.exception("Validating pingreq packet")
      raise

  def __str__(self):
    return "Pingreq (PacketId=" + str(self.PacketId) + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId


class Pingresps(Packets):
  """
  PINGRESP packet (Section 3.12).

  Wire format: length + type(1) + packetid(2) + [AppMsgsRemaining(1, optional)]

  AppMsgsRemaining is optional; its existence is inferred from the packet length.
  Values: 0 = none waiting; 1-254 = that many waiting; 255 = unspecified positive number.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["packetType", "PacketId", "AppMsgsRemaining"])
    self.packetType = PacketTypes.PINGRESP
    self.PacketId = 0
    self.AppMsgsRemaining = None  # None means field is absent from wire
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + packetid(2) + [AppMsgsRemaining(1)]
    body = bytes([PacketTypes.PINGRESP]) + writeInt16(self.PacketId)
    if self.AppMsgsRemaining is not None:
      body += bytes([self.AppMsgsRemaining])
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PINGRESP
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      # AppMsgsRemaining is optional (Section 3.12.3)
      if packetlen > lenlen + 3:
        self.AppMsgsRemaining = buffer[lenlen + 3]
      else:
        self.AppMsgsRemaining = None
    except:
      logger.exception("Validating pingresp packet")
      raise

  def __str__(self):
    s = "Pingresp (PacketId=" + str(self.PacketId)
    if self.AppMsgsRemaining is not None:
      s += ", AppMsgsRemaining=" + str(self.AppMsgsRemaining)
    return s + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId and \
           self.AppMsgsRemaining == packet.AppMsgsRemaining


class DisconnectFlags:
  """
  DISCONNECT Flags byte structure (Section 3.13.2)

  Bits 7-3: Reserved (must be 0)
  Bit 2:    Reason Code Flag
  Bit 1:    Session Expiry Interval Flag (Sess Exp)
  Bit 0:    Packet Identifier Flag (PacketId)
  """

  def __init__(self):
    self.PacketIdFlag = False         # bit 0: Packet Identifier present
    self.SessionExpiryFlag = False    # bit 1: Session Expiry Interval present
    self.ReasonCodeFlag = False       # bit 2: Reason Code present

  def __eq__(self, flags):
    return self.PacketIdFlag == flags.PacketIdFlag and \
           self.SessionExpiryFlag == flags.SessionExpiryFlag and \
           self.ReasonCodeFlag == flags.ReasonCodeFlag

  def __setattr__(self, name, value):
    names = ["PacketIdFlag", "SessionExpiryFlag", "ReasonCodeFlag"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "DisconnectFlags(PacketIdFlag=" + str(self.PacketIdFlag) + \
           ", SessionExpiryFlag=" + str(self.SessionExpiryFlag) + \
           ", ReasonCodeFlag=" + str(self.ReasonCodeFlag) + ")"

  def pack(self):
    "pack data into string buffer ready for transmission down socket"
    return bytes([(self.ReasonCodeFlag << 2) |
                  (self.SessionExpiryFlag << 1) |
                  self.PacketIdFlag])

  def unpack(self, b0):
    "unpack data from string buffer into separate fields"
    assert ((b0 >> 3) & 0x1F) == 0, \
        "[MQTT-SN-3.13.2-1] bits 7-3 of the disconnect flags are reserved and must be 0"
    self.ReasonCodeFlag    = ((b0 >> 2) & 0x01) == 1
    self.SessionExpiryFlag = ((b0 >> 1) & 0x01) == 1
    self.PacketIdFlag      = (b0 & 0x01) == 1
    return 1  # length of flags


class Disconnects(Packets):
  """
  DISCONNECT packet (Section 3.13).

  Wire format: length + type(1) + flags(1) +
               [packetid(2)]          if PacketIdFlag set  +
               [reasoncode(1)]        if ReasonCodeFlag set  +
               [sessionexpiry(4)]     if SessionExpiryFlag set  +
               [reasonstring(utf-8)]  remainder of packet, optional

  Flags govern which optional fields are present on the wire.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType",
          "DisconnectFlags",
          "PacketId", "ReasonCode", "SessionExpiryInterval", "ReasonString"])
    self.packetType = PacketTypes.DISCONNECT
    self.DisconnectFlags = DisconnectFlags()
    self.PacketId = 0
    self.ReasonCode = ReasonCodes(PacketTypes.DISCONNECT, "Normal disconnection")
    self.SessionExpiryInterval = None   # None means absent from wire
    self.ReasonString = None            # None means absent from wire
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    flags = self.DisconnectFlags
    # Derive flags from which fields are set
    flags.PacketIdFlag      = (self.PacketId != 0)
    flags.ReasonCodeFlag    = (self.ReasonCode.value != 0)
    flags.SessionExpiryFlag = (self.SessionExpiryInterval is not None)
    body = bytes([PacketTypes.DISCONNECT]) + flags.pack()
    if flags.PacketIdFlag:
      body += writeInt16(self.PacketId)
    if flags.ReasonCodeFlag:
      body += self.ReasonCode.pack()
    if flags.SessionExpiryFlag:
      sei = self.SessionExpiryInterval
      body += bytes([(sei >> 24) & 0xFF, (sei >> 16) & 0xFF,
                     (sei >> 8) & 0xFF, sei & 0xFF])
    if self.ReasonString is not None:
      body += writeData(self.ReasonString)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.DISCONNECT
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.DisconnectFlags.unpack(buffer[lenlen + 1])
      flags = self.DisconnectFlags
      pos = lenlen + 2  # byte after flags
      if flags.PacketIdFlag:
        self.PacketId = readInt16(buffer[pos:])
        pos += 2
      else:
        self.PacketId = 0
      if flags.ReasonCodeFlag:
        self.ReasonCode = ReasonCodes(PacketTypes.DISCONNECT, "Normal disconnection")
        self.ReasonCode.unpack(buffer[pos:])
        pos += 1
      else:
        self.ReasonCode = ReasonCodes(PacketTypes.DISCONNECT, "Normal disconnection")
      if flags.SessionExpiryFlag:
        self.SessionExpiryInterval = (buffer[pos] << 24) | (buffer[pos+1] << 16) | \
                                     (buffer[pos+2] << 8) | buffer[pos+3]
        pos += 4
      else:
        self.SessionExpiryInterval = None
      # ReasonString is optional remainder of packet
      if pos < packetlen:
        self.ReasonString = buffer[pos:packetlen].decode("utf-8")
      else:
        self.ReasonString = None
    except:
      logger.exception("Validating disconnect packet")
      raise

  def __str__(self):
    s = "Disconnect (" + str(self.DisconnectFlags) + \
        ", PacketId=" + str(self.PacketId) + \
        ", ReasonCode=" + str(self.ReasonCode)
    if self.SessionExpiryInterval is not None:
      s += ", SessionExpiryInterval=" + str(self.SessionExpiryInterval)
    if self.ReasonString is not None:
      s += ", ReasonString=" + str(self.ReasonString)
    return s + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId and \
           self.ReasonCode.value == packet.ReasonCode.value and \
           self.SessionExpiryInterval == packet.SessionExpiryInterval and \
           self.ReasonString == packet.ReasonString


class Auths(Packets):
  """
  AUTH packet (Section 3.3).

  Wire format: length + type(1) + packetid(2) + reasoncode(1) +
               authmethodlen(2) + authmethod(utf-8) + authdata(binary)

  Authentication Method is a UTF-8 string preceded by a 2-byte length.
  Authentication Data follows immediately and runs to the end of the packet.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "PacketId", "ReasonCode", "AuthMethod", "AuthData"])
    self.packetType = PacketTypes.AUTH
    self.PacketId = 0
    self.ReasonCode = ReasonCodes(PacketTypes.AUTH)
    self.AuthMethod = ""
    self.AuthData = b""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    method_bytes = writeData(self.AuthMethod)
    body = bytes([PacketTypes.AUTH]) + \
           writeInt16(self.PacketId) + \
           self.ReasonCode.pack() + \
           writeInt16(len(method_bytes)) + \
           method_bytes + \
           writeData(self.AuthData)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.AUTH
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      self.ReasonCode = ReasonCodes(PacketTypes.AUTH)
      self.ReasonCode.unpack(buffer[lenlen + 3:])
      method_len = readInt16(buffer[lenlen + 4:])
      pos = lenlen + 6
      self.AuthMethod = buffer[pos:pos + method_len].decode("utf-8")
      pos += method_len
      self.AuthData = buffer[pos:packetlen]
    except:
      logger.exception("Validating auth packet")
      raise

  def __str__(self):
    return "Auth (PacketId=" + str(self.PacketId) + \
           ", ReasonCode=" + str(self.ReasonCode) + \
           ", AuthMethod=" + str(self.AuthMethod) + \
           ", AuthData=" + str(self.AuthData) + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId and \
           self.ReasonCode.value == packet.ReasonCode.value and \
           self.AuthMethod == packet.AuthMethod and \
           self.AuthData == packet.AuthData


class RegisterFlags:
  """
  REGISTER Flags byte structure (Section 3.4.2)

  Bits 7-1: Reserved (must be 0)
  Bit 0:    Topic Alias Flag
  """

  def __init__(self):
    self.TopicAliasFlag = False  # bit 0: Topic Alias field is present

  def __eq__(self, flags):
    return self.TopicAliasFlag == flags.TopicAliasFlag

  def __setattr__(self, name, value):
    names = ["TopicAliasFlag"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "RegisterFlags(TopicAliasFlag=" + str(self.TopicAliasFlag) + ")"

  def pack(self):
    return bytes([1 if self.TopicAliasFlag else 0])

  def unpack(self, b0):
    assert ((b0 >> 1) & 0x7F) == 0, \
        "[MQTT-SN-3.4.2-1] bits 7-1 of the register flags are reserved and must be 0"
    self.TopicAliasFlag = (b0 & 0x01) == 1
    return 1  # length of flags


class Registers(Packets):
  """
  REGISTER packet (Section 3.4).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               [topicalias(2)]  if TopicAliasFlag set  +
               topicname(utf-8, fills remainder)
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "RegisterFlags", "PacketId", "TopicAlias", "TopicName"])
    self.packetType = PacketTypes.REGISTER
    self.RegisterFlags = RegisterFlags()
    self.PacketId = 0
    self.TopicAlias = 0   # present only when RegisterFlags.TopicAliasFlag is True
    self.TopicName = ""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    self.RegisterFlags.TopicAliasFlag = (self.TopicAlias != 0)
    topic_bytes = writeData(self.TopicName)
    body = bytes([PacketTypes.REGISTER]) + \
           self.RegisterFlags.pack() + \
           writeInt16(self.PacketId)
    if self.RegisterFlags.TopicAliasFlag:
      body += writeInt16(self.TopicAlias)
    body += topic_bytes
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.REGISTER
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.RegisterFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4  # byte after packetid
      if self.RegisterFlags.TopicAliasFlag:
        self.TopicAlias = readInt16(buffer[pos:])
        pos += 2
      else:
        self.TopicAlias = 0
      self.TopicName = buffer[pos:packetlen].decode("utf-8")
    except:
      logger.exception("Validating register packet")
      raise

  def __str__(self):
    return "Register (" + str(self.RegisterFlags) + \
           ", PacketId=" + str(self.PacketId) + \
           ", TopicAlias=" + str(self.TopicAlias) + \
           ", TopicName=" + str(self.TopicName) + ")"

  def __eq__(self, packet):
    return self.RegisterFlags == packet.RegisterFlags and \
           self.PacketId == packet.PacketId and \
           self.TopicAlias == packet.TopicAlias and \
           self.TopicName == packet.TopicName


class RegackFlags:
  """
  REGACK Flags byte structure (Section 3.5.2)

  Bits 7-3: Reserved (must be 0)
  Bit 2:    Topic Alias Flag
  Bits 1-0: Topic Type (0=Session, 1=Predefined)
  """

  def __init__(self):
    self.TopicType = 0           # 2 bits: 0=Session, 1=Predefined
    self.TopicAliasFlag = False  # bit 2: Topic Alias field is present

  def __eq__(self, flags):
    return self.TopicType == flags.TopicType and \
           self.TopicAliasFlag == flags.TopicAliasFlag

  def __setattr__(self, name, value):
    names = ["TopicType", "TopicAliasFlag"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "RegackFlags(TopicType=" + str(self.TopicType) + \
           ", TopicAliasFlag=" + str(self.TopicAliasFlag) + ")"

  def pack(self):
    return bytes([(self.TopicAliasFlag << 2) | (self.TopicType & 0x03)])

  def unpack(self, b0):
    assert ((b0 >> 3) & 0x1F) == 0, \
        "[MQTT-SN-3.5.2-1] bits 7-3 of the regack flags are reserved and must be 0"
    self.TopicAliasFlag = ((b0 >> 2) & 0x01) == 1
    self.TopicType      = b0 & 0x03
    return 1  # length of flags


class Regacks(Packets):
  """
  REGACK packet (Section 3.5).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               [topicalias(2)]  if TopicAliasFlag set  +
               [reasoncode(1)]  optional, inferred from packet length
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "RegackFlags", "PacketId", "TopicAlias", "ReasonCode"])
    self.packetType = PacketTypes.REGACK
    self.RegackFlags = RegackFlags()
    self.PacketId = 0
    self.TopicAlias = 0    # present only when RegackFlags.TopicAliasFlag is True
    self.ReasonCode = ReasonCodes(PacketTypes.REGACK)
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + flags(1) + packetid(2) + [alias(2)] + [reasoncode(1)]
    body = bytes([PacketTypes.REGACK]) + \
           self.RegackFlags.pack() + \
           writeInt16(self.PacketId)
    if self.RegackFlags.TopicAliasFlag:
      body += writeInt16(self.TopicAlias)
    if self.ReasonCode.value != 0 or self.RegackFlags.TopicAliasFlag:
      body += self.ReasonCode.pack()
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.REGACK
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.RegackFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4  # byte after packetid
      if self.RegackFlags.TopicAliasFlag:
        self.TopicAlias = readInt16(buffer[pos:])
        pos += 2
      else:
        self.TopicAlias = 0
      # ReasonCode: present when packet is long enough (Section 3.5.5)
      if pos < packetlen:
        self.ReasonCode = ReasonCodes(PacketTypes.REGACK)
        self.ReasonCode.unpack(buffer[pos:])
      else:
        self.ReasonCode = ReasonCodes(PacketTypes.REGACK)  # absent means success (0x00)
    except:
      logger.exception("Validating regack packet")
      raise

  def __str__(self):
    return "Regack (" + str(self.RegackFlags) + \
           ", PacketId=" + str(self.PacketId) + \
           ", TopicAlias=" + str(self.TopicAlias) + \
           ", ReasonCode=" + str(self.ReasonCode) + ")"

  def __eq__(self, packet):
    return self.RegackFlags == packet.RegackFlags and \
           self.PacketId == packet.PacketId and \
           self.TopicAlias == packet.TopicAlias and \
           self.ReasonCode.value == packet.ReasonCode.value


# classes[n] maps packet type integer n to its class.
# None entries mark packet types not yet implemented.
class PubwosFlags:
  """
  PUBWOS Flags byte structure (Section 3.6.1.2)

  Bits 7-5: Reserved (must be 0)
  Bit 4:    Retain
  Bits 3-2: Reserved (must be 0)
  Bits 1-0: Topic Type (must be PREDEFINED (1) or NAME (3) only)
  """

  def __init__(self):
    self.TopicType = 1   # 2 bits: 1=Predefined, 3=Name (Session not allowed)
    self.Retain    = False  # bit 4

  def __eq__(self, flags):
    return self.TopicType == flags.TopicType and \
           self.Retain    == flags.Retain

  def __setattr__(self, name, value):
    names = ["TopicType", "Retain"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "PubwosFlags(Retain=" + str(self.Retain) + \
           ", TopicType=" + str(self.TopicType) + ")"

  def pack(self):
    return bytes([(self.Retain << 4) | (self.TopicType & 0x03)])

  def unpack(self, b0):
    assert ((b0 >> 5) & 0x07) == 0 and ((b0 >> 2) & 0x03) == 0, \
        "[MQTT-SN-3.6.1.2-1] bits 7-5 and 3-2 of the PUBWOS flags are reserved and must be 0"
    self.Retain    = ((b0 >> 4) & 0x01) == 1
    self.TopicType = b0 & 0x03
    assert self.TopicType in (1, 3), \
        "[MQTT-SN-3.6.1.2.1-1] PUBWOS TopicType must be Predefined (1) or Topic Name (3)"
    return 1


class Pubwoses(Packets):
  """
  PUBWOS (Publish Without Session) packet (Section 3.6.1).

  Wire format: length + type(1) + flags(1) +
               topic_alias_or_len(2) + [topic_name(utf-8)] + payload(binary)

  No PacketId, no QoS, no DUP — this is a fire-and-forget session-less publish.
  TopicType must be Predefined (1) or Topic Name (3); Session alias is not allowed.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "PubwosFlags", "TopicAlias", "TopicName", "Data"])
    self.packetType = PacketTypes.PUBWOS
    self.PubwosFlags = PubwosFlags()
    self.TopicAlias  = 0    # used when TopicType is Predefined (1)
    self.TopicName   = b""  # used when TopicType is Name (3)
    self.Data        = b""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([PacketTypes.PUBWOS]) + self.PubwosFlags.pack()
    if self.PubwosFlags.TopicType == 3:  # Topic Name
      name = writeData(self.TopicName)
      body += writeInt16(len(name)) + name
    else:                                 # Predefined alias
      body += writeInt16(self.TopicAlias)
    body += writeData(self.Data)
    msglen = 1 + len(body)
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBWOS
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.PubwosFlags.unpack(buffer[lenlen + 1])
      pos = lenlen + 2
      if self.PubwosFlags.TopicType == 3:  # Topic Name
        name_len = readInt16(buffer[pos:]);  pos += 2
        self.TopicName = buffer[pos:pos + name_len];  pos += name_len
        self.TopicAlias = 0
      else:                                 # Predefined alias
        self.TopicAlias = readInt16(buffer[pos:]);  pos += 2
        self.TopicName  = b""
      self.Data = buffer[pos:packetlen]
    except:
      logger.exception("Validating pubwos packet")
      raise

  def __str__(self):
    return "Pubwos (" + str(self.PubwosFlags) + \
           ", TopicAlias=" + str(self.TopicAlias) + \
           ", TopicName=" + str(self.TopicName) + \
           ", Data=" + str(self.Data) + ")"

  def __eq__(self, packet):
    if self.PubwosFlags.TopicType == 3:
      topic_eq = self.TopicName == packet.TopicName
    else:
      topic_eq = self.TopicAlias == packet.TopicAlias
    return self.PubwosFlags == packet.PubwosFlags and \
           topic_eq and \
           self.Data == packet.Data


class SleepreqFlags:
  """
  SLEEPREQ Flags byte structure (Section 3.15.2)

  Bits 7-1: Reserved (must be 0)
  Bit 0:    Retain Topic Aliases (RetainT)
  """

  def __init__(self):
    self.RetainT = False  # bit 0: retain Session Topic Aliases during sleep

  def __eq__(self, flags):
    return self.RetainT == flags.RetainT

  def __setattr__(self, name, value):
    names = ["RetainT"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "SleepreqFlags(RetainT=" + str(self.RetainT) + ")"

  def pack(self):
    return bytes([1 if self.RetainT else 0])

  def unpack(self, b0):
    assert ((b0 >> 1) & 0x7F) == 0, \
        "[MQTT-SN-3.15.2-1] bits 7-1 of the SLEEPREQ flags are reserved and must be 0"
    self.RetainT = (b0 & 0x01) == 1
    return 1


class Sleepreqs(Packets):
  """
  SLEEPREQ packet (Section 3.15).

  Wire format: length + type(1) + flags(1) + packetid(2) + sleep_duration(4)

  sleep_duration is a 4-byte big-endian integer (seconds).  Must be > 0.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "SleepreqFlags", "PacketId", "SleepDuration"])
    self.packetType   = PacketTypes.SLEEPREQ
    self.SleepreqFlags = SleepreqFlags()
    self.PacketId      = 0
    self.SleepDuration = 0  # seconds; must be > 0 per spec
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    sd = self.SleepDuration
    body = bytes([PacketTypes.SLEEPREQ]) + \
           self.SleepreqFlags.pack() + \
           writeInt16(self.PacketId) + \
           bytes([(sd >> 24) & 0xFF, (sd >> 16) & 0xFF,
                  (sd >> 8)  & 0xFF,  sd        & 0xFF])
    return bytes([1 + len(body)]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SLEEPREQ
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.SleepreqFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4
      self.SleepDuration = (buffer[pos]   << 24) | (buffer[pos+1] << 16) | \
                           (buffer[pos+2] <<  8) |  buffer[pos+3]
    except:
      logger.exception("Validating sleepreq packet")
      raise

  def __str__(self):
    return "Sleepreq (" + str(self.SleepreqFlags) + \
           ", PacketId=" + str(self.PacketId) + \
           ", SleepDuration=" + str(self.SleepDuration) + ")"

  def __eq__(self, packet):
    return self.SleepreqFlags == packet.SleepreqFlags and \
           self.PacketId      == packet.PacketId and \
           self.SleepDuration == packet.SleepDuration


class SleeprespFlags:
  """
  SLEEPRESP Flags byte structure (Section 3.16.2)

  Bits 7-1: Reserved (must be 0)
  Bit 0:    Sleep Duration Flag (SleepDur) — governs presence of SleepDuration field
  """

  def __init__(self):
    self.SleepDur = False  # bit 0: SleepDuration field present

  def __eq__(self, flags):
    return self.SleepDur == flags.SleepDur

  def __setattr__(self, name, value):
    names = ["SleepDur"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "SleeprespFlags(SleepDur=" + str(self.SleepDur) + ")"

  def pack(self):
    return bytes([1 if self.SleepDur else 0])

  def unpack(self, b0):
    assert ((b0 >> 1) & 0x7F) == 0, \
        "[MQTT-SN-3.16.2-1] bits 7-1 of the SLEEPRESP flags are reserved and must be 0"
    self.SleepDur = (b0 & 0x01) == 1
    return 1


class Sleepresps(Packets):
  """
  SLEEPRESP packet (Section 3.16).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               [sleep_duration(4)]  if SleepDur flag set +
               [reason_code(1)]     optional, inferred from packet length

  SleepDuration is None when absent (flag clear).
  ReasonCode defaults to 0x00 (Success) when absent.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "SleeprespFlags", "PacketId",
          "SleepDuration", "ReasonCode"])
    self.packetType    = PacketTypes.SLEEPRESP
    self.SleeprespFlags = SleeprespFlags()
    self.PacketId       = 0
    self.SleepDuration  = None  # None = absent (SleepDur flag clear)
    self.ReasonCode     = ReasonCodes(PacketTypes.SLEEPRESP)
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    self.SleeprespFlags.SleepDur = (self.SleepDuration is not None)
    body = bytes([PacketTypes.SLEEPRESP]) + \
           self.SleeprespFlags.pack() + \
           writeInt16(self.PacketId)
    if self.SleeprespFlags.SleepDur:
      sd = self.SleepDuration
      body += bytes([(sd >> 24) & 0xFF, (sd >> 16) & 0xFF,
                     (sd >> 8)  & 0xFF,  sd        & 0xFF])
    if self.ReasonCode.value != 0:
      body += self.ReasonCode.pack()
    return bytes([1 + len(body)]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SLEEPRESP
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.SleeprespFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4
      if self.SleeprespFlags.SleepDur:
        self.SleepDuration = (buffer[pos]   << 24) | (buffer[pos+1] << 16) | \
                             (buffer[pos+2] <<  8) |  buffer[pos+3]
        pos += 4
      else:
        self.SleepDuration = None
      # ReasonCode: optional, inferred from remaining packet length (Section 3.16.4)
      if pos < packetlen:
        self.ReasonCode = ReasonCodes(PacketTypes.SLEEPRESP)
        self.ReasonCode.unpack(buffer[pos:])
      else:
        self.ReasonCode = ReasonCodes(PacketTypes.SLEEPRESP)  # absent means success (0x00)
    except:
      logger.exception("Validating sleepresp packet")
      raise

  def __str__(self):
    s = "Sleepresp (" + str(self.SleeprespFlags) + \
        ", PacketId=" + str(self.PacketId)
    if self.SleepDuration is not None:
      s += ", SleepDuration=" + str(self.SleepDuration)
    return s + ", ReasonCode=" + str(self.ReasonCode) + ")"

  def __eq__(self, packet):
    return self.SleeprespFlags == packet.SleeprespFlags and \
           self.PacketId       == packet.PacketId and \
           self.SleepDuration  == packet.SleepDuration and \
           self.ReasonCode.value == packet.ReasonCode.value


class Wakeups(Packets):
  """
  WAKEUP packet (Section 3.14).

  Wire format: length + type(1)

  No data fields — the packet is just the header; its presence is the signal.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["packetType"])
    self.packetType = PacketTypes.WAKEUP
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([PacketTypes.WAKEUP])
    return bytes([1 + len(body)]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.WAKEUP
    # No data fields to read

  def __str__(self):
    return "Wakeup ()"

  def __eq__(self, packet):
    return True  # no variable fields; any two Wakeup packets are equal


class Advertises(Packets):
  """
  ADVERTISE packet (Section 3.20.1).

  Wire format: length + type(1) + gateway_id(1) + duration(2)

  Broadcast periodically by a gateway to advertise its presence.
  Duration is the time interval in seconds until the next ADVERTISE.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["packetType", "GatewayId", "Duration"])
    self.packetType = PacketTypes.ADVERTISE
    self.GatewayId   = 0   # 1 byte: unique gateway identifier
    self.Duration    = 0   # 2 bytes: seconds until next ADVERTISE
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([PacketTypes.ADVERTISE, self.GatewayId]) + \
           writeInt16(self.Duration)
    return bytes([1 + len(body)]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.ADVERTISE
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.GatewayId = buffer[lenlen + 1]
      self.Duration  = readInt16(buffer[lenlen + 2:])
    except:
      logger.exception("Validating advertise packet")
      raise

  def __str__(self):
    return "Advertise (GatewayId=" + str(self.GatewayId) + \
           ", Duration=" + str(self.Duration) + ")"

  def __eq__(self, packet):
    return self.GatewayId == packet.GatewayId and \
           self.Duration  == packet.Duration


class Searchgws(Packets):
  """
  SEARCHGW packet (Section 3.20.2).

  Wire format: length + type(1) + [additional_network_info(bytes, optional)]

  AdditionalNetworkInfo is optional; its presence is inferred from packet length.
  When absent it is stored as b"".
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "AdditionalNetworkInfo"])
    self.packetType            = PacketTypes.SEARCHGW
    self.AdditionalNetworkInfo  = b""  # b"" means absent
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([PacketTypes.SEARCHGW]) + writeData(self.AdditionalNetworkInfo)
    return bytes([1 + len(body)]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SEARCHGW
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      pos = lenlen + 1  # byte after type
      # AdditionalNetworkInfo is optional; fills remainder of packet
      if pos < packetlen:
        self.AdditionalNetworkInfo = buffer[pos:packetlen]
      else:
        self.AdditionalNetworkInfo = b""
    except:
      logger.exception("Validating searchgw packet")
      raise

  def __str__(self):
    return "Searchgw (AdditionalNetworkInfo=" + \
           str(self.AdditionalNetworkInfo) + ")"

  def __eq__(self, packet):
    return self.AdditionalNetworkInfo == packet.AdditionalNetworkInfo


class Gwinfos(Packets):
  """
  GWINFO packet (Section 3.20.3).

  Wire format: length + type(1) + gateway_id(1) + [gateway_address(bytes, optional)]

  GatewayAddress is optional; present only when sent by a client (not a gateway).
  Its presence is inferred from packet length.  Stored as b"" when absent.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "GatewayId", "GatewayAddress"])
    self.packetType    = PacketTypes.GWINFO
    self.GatewayId      = 0   # 1 byte
    self.GatewayAddress = b""  # b"" means absent (packet sent by gateway)
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([PacketTypes.GWINFO, self.GatewayId]) + \
           writeData(self.GatewayAddress)
    return bytes([1 + len(body)]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.GWINFO
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      self.GatewayId = buffer[lenlen + 1]
      pos = lenlen + 2
      # GatewayAddress is optional; fills remainder of packet (Section 3.20.3.3)
      if pos < packetlen:
        self.GatewayAddress = buffer[pos:packetlen]
      else:
        self.GatewayAddress = b""
    except:
      logger.exception("Validating gwinfo packet")
      raise

  def __str__(self):
    return "Gwinfo (GatewayId=" + str(self.GatewayId) + \
           ", GatewayAddress=" + str(self.GatewayAddress) + ")"

  def __eq__(self, packet):
    return self.GatewayId      == packet.GatewayId and \
           self.GatewayAddress == packet.GatewayAddress


class ForwarderEncapsulations(Packets):
  """
  Forwarder Encapsulation packet (Section 3.19).

  A forwarder wraps client packets verbatim when relaying them to a gateway
  that is not on the same network segment.

  Wire format:
    outer_length + type(1) + ClientAddressingInfo(variable) ||
    inner_packet(variable, NOT counted in outer_length)

  The Length field in the outer header counts only up to the end of the
  ClientAddressingInfo field (inclusive of the length field itself).
  The inner MQTT-SN packet immediately follows and is not included in
  that count.  ClientAddressingInfo may be zero bytes.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "ClientAddressingInfo", "MQTTSNPacket"])
    self.packetType           = PacketTypes.FOWARDER_ENCAPSULATION
    self.ClientAddressingInfo  = b""   # variable-length addressing data
    self.MQTTSNPacket          = b""   # raw bytes of the encapsulated packet
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # Outer section: type(1) + ClientAddressingInfo
    outer_body = bytes([PacketTypes.FOWARDER_ENCAPSULATION]) + \
                 writeData(self.ClientAddressingInfo)
    outer_len = 1 + len(outer_body)  # length field(1) + outer_body
    return bytes([outer_len]) + outer_body + writeData(self.MQTTSNPacket)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.FOWARDER_ENCAPSULATION
    try:
      outer_len, lenlen = PacketLens.decode(buffer)
      # ClientAddressingInfo occupies the bytes between the type byte and
      # the end of the outer header (outer_len bytes total from buffer[0]).
      addr_start = lenlen + 1          # byte after type
      addr_end   = outer_len           # outer_len is total outer size
      self.ClientAddressingInfo = buffer[addr_start:addr_end]
      # Inner packet follows immediately after the outer header
      self.MQTTSNPacket = buffer[outer_len:]
    except:
      logger.exception("Validating forwarder encapsulation packet")
      raise

  def __str__(self):
    return "ForwarderEncapsulation (" \
           "ClientAddressingInfo=" + str(self.ClientAddressingInfo) + \
           ", MQTTSNPacket=" + str(self.MQTTSNPacket) + ")"

  def __eq__(self, packet):
    return self.ClientAddressingInfo == packet.ClientAddressingInfo and \
           self.MQTTSNPacket         == packet.MQTTSNPacket


class ConnectionEncapsulations(Packets):
  """
  Connection Encapsulation packet (Section 3.18).

  Allows a client whose network address has changed to associate a packet
  with an existing virtual connection by embedding its ClientIdentifier.
  Only clients may use this encapsulation.

  Wire format:
    outer_length + type(1) + ClientIdentifier(UTF-8, variable) ||
    inner_packet(variable, NOT counted in outer_length)

  The Length field in the outer header counts only up to the end of the
  ClientIdentifier field (inclusive of the length field itself).
  The inner MQTT-SN packet immediately follows and is not counted.
  ClientIdentifier may be zero bytes (empty string).
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType", "ClientIdentifier", "MQTTSNPacket"])
    self.packetType      = PacketTypes.CONNECTION_ENCAPSULATION
    self.ClientIdentifier = ""    # UTF-8 string; the virtual-connection client ID
    self.MQTTSNPacket     = b""   # raw bytes of the encapsulated packet
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    cid_bytes = writeData(self.ClientIdentifier)
    outer_body = bytes([PacketTypes.CONNECTION_ENCAPSULATION]) + cid_bytes
    outer_len  = 1 + len(outer_body)  # length field(1) + outer_body
    return bytes([outer_len]) + outer_body + writeData(self.MQTTSNPacket)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.CONNECTION_ENCAPSULATION
    try:
      outer_len, lenlen = PacketLens.decode(buffer)
      cid_start = lenlen + 1   # byte after type
      cid_end   = outer_len    # up to end of outer header
      self.ClientIdentifier = buffer[cid_start:cid_end].decode("utf-8")
      # Inner packet follows immediately after the outer header
      self.MQTTSNPacket = buffer[outer_len:]
    except:
      logger.exception("Validating connection encapsulation packet")
      raise

  def __str__(self):
    return "ConnectionEncapsulation (" \
           "ClientIdentifier=" + str(self.ClientIdentifier) + \
           ", MQTTSNPacket=" + str(self.MQTTSNPacket) + ")"

  def __eq__(self, packet):
    return self.ClientIdentifier == packet.ClientIdentifier and \
           self.MQTTSNPacket     == packet.MQTTSNPacket


class ProtectionFlags:
  """
  Protection Flags byte structure (Section 3.17.2).

  Bits 1-0: Monotonic Counter Length (CounterLen)
    0x0 → no counter present
    0x1 → 2-byte counter present
    0x2 → 4-byte counter present
    0x3 → reserved (MUST NOT be used)

  Bits 3-2: Cryptographic Material Length (CryptoLen)
    0x0 → no crypto material present
    0x1 → 2 bytes present
    0x2 → 4 bytes present
    0x3 → 12 bytes present

  Bits 7-4: Authentication Tag Length (AuthTagLen)
    0x0 → provider-defined length
    0x1 → scheme nominal tag size
    0x2/0x3 → reserved (MUST NOT be used)
    0x4–0xF → AuthOnly truncation; tag length = value × 2 bytes
  """

  # Maps CounterLen flag value → byte count of the MonotonicCounter field
  COUNTER_BYTES = {0: 0, 1: 2, 2: 4}

  # Maps CryptoLen flag value → byte count of the CryptographicMaterial field
  CRYPTO_BYTES = {0: 0, 1: 2, 2: 4, 3: 12}

  def __init__(self):
    self.CounterLen = 0   # bits 1-0: monotonic counter length code
    self.CryptoLen  = 0   # bits 3-2: cryptographic material length code
    self.AuthTagLen = 1   # bits 7-4: authentication tag length code

  def __eq__(self, flags):
    return self.CounterLen == flags.CounterLen and \
           self.CryptoLen  == flags.CryptoLen  and \
           self.AuthTagLen == flags.AuthTagLen

  def __setattr__(self, name, value):
    names = ["CounterLen", "CryptoLen", "AuthTagLen"]
    if name not in names:
      raise MQTTSNException(name + " Attribute name must be one of " + str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return "ProtectionFlags(AuthTagLen=" + str(self.AuthTagLen) + \
           ", CryptoLen="  + str(self.CryptoLen) + \
           ", CounterLen=" + str(self.CounterLen) + ")"

  def pack(self):
    return bytes([(self.AuthTagLen << 4) |
                  ((self.CryptoLen & 0x03) << 2) |
                  (self.CounterLen & 0x03)])

  def unpack(self, b0):
    self.AuthTagLen = (b0 >> 4) & 0x0F
    self.CryptoLen  = (b0 >> 2) & 0x03
    self.CounterLen = b0 & 0x03
    assert self.CounterLen != 3, \
        "[MQTT-SN-3.17.2.1-1] MonotonicCounterLength value 0x3 is reserved"
    assert self.AuthTagLen not in (2, 3), \
        "[MQTT-SN-3.17.2.3-3] AuthenticationTagLength values 0x2 and 0x3 are reserved"
    return 1

  def crypto_material_size(self):
    """Return the byte count of the CryptographicMaterial field."""
    return self.CRYPTO_BYTES[self.CryptoLen]

  def counter_size(self):
    """Return the byte count of the MonotonicCounter field."""
    return self.COUNTER_BYTES.get(self.CounterLen, 0)


class ProtectionEncapsulations(Packets):
  """
  Protection Encapsulation packet (Section 3.17).

  Provides cryptographic authentication (and optionally encryption) for any
  MQTT-SN packet except a Forwarder Encapsulation.

  Wire format (all fields mandatory unless flagged optional):
    length + type(1) + ProtectionFlags(1) + ProtectionScheme(1) +
    SenderIdentifier(8) + Random(4) +
    [CryptographicMaterial(2/4/12 bytes, from CryptoLen flag)] +
    [MonotonicCounter(2/4 bytes, from CounterLen flag)] +
    ProtectedMQTTSNPacket(variable) +
    AuthenticationTag(variable, length determined by AuthTagLen flag and scheme)

  This class stores raw bytes for the opaque fields (ProtectedMQTTSNPacket and
  AuthenticationTag) so it can be serialized and deserialized without needing
  to perform the actual cryptographic operations.

  Because the Authentication Tag length depends on the protection scheme's
  nominal tag size (when AuthTagLen == 0x1), the caller must supply the tag
  as a raw bytes object; the class makes no attempt to compute or verify it.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["packetType",
          "ProtectionFlags", "ProtectionScheme",
          "SenderIdentifier", "Random",
          "CryptographicMaterial", "MonotonicCounter",
          "ProtectedMQTTSNPacket", "AuthenticationTag"])
    self.packetType           = PacketTypes.PROTECTION_ENCAPSULATION
    self.ProtectionFlags       = ProtectionFlags()
    self.ProtectionScheme      = 0x00    # default: HMAC-SHA256
    self.SenderIdentifier      = b'\x00' * 8  # 8 bytes, e.g. MAC address
    self.Random                = b'\x00' * 4  # 4 bytes
    self.CryptographicMaterial = b""     # present when CryptoLen != 0
    self.MonotonicCounter      = b""     # present when CounterLen != 0
    self.ProtectedMQTTSNPacket = b""     # raw bytes of the protected packet
    self.AuthenticationTag     = b""     # raw authentication tag bytes
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # Validate sizes against flags
    pf = self.ProtectionFlags
    pf.CryptoLen  = {0: 0, 2: 1, 4: 2, 12: 3}[len(self.CryptographicMaterial)]
    pf.CounterLen = {0: 0, 2: 1, 4: 2}[len(self.MonotonicCounter)]

    body = bytes([PacketTypes.PROTECTION_ENCAPSULATION]) + \
           pf.pack() + \
           bytes([self.ProtectionScheme]) + \
           writeData(self.SenderIdentifier) + \
           writeData(self.Random) + \
           writeData(self.CryptographicMaterial) + \
           writeData(self.MonotonicCounter) + \
           writeData(self.ProtectedMQTTSNPacket) + \
           writeData(self.AuthenticationTag)
    # Use 4-byte extended length when the packet exceeds 255 bytes
    msglen = 1 + len(body)
    if msglen <= 255:
      return bytes([msglen]) + body
    else:
      # 4-byte length: 0x01 + hi + lo covers (msglen+2) total
      total = msglen + 2    # 4-byte length field is 3 bytes larger than 1-byte
      return bytes([0x01, (total >> 8) & 0xFF, total & 0xFF]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PROTECTION_ENCAPSULATION
    try:
      packetlen, lenlen = PacketLens.decode(buffer)
      pos = lenlen + 1   # byte after type

      self.ProtectionFlags.unpack(buffer[pos]);  pos += 1
      pf = self.ProtectionFlags

      self.ProtectionScheme = buffer[pos];  pos += 1

      self.SenderIdentifier = buffer[pos:pos + 8];  pos += 8
      self.Random           = buffer[pos:pos + 4];  pos += 4

      csize = pf.crypto_material_size()
      self.CryptographicMaterial = buffer[pos:pos + csize];  pos += csize

      msize = pf.counter_size()
      self.MonotonicCounter = buffer[pos:pos + msize];  pos += msize

      # Determine the AuthenticationTag byte length from the flag
      auth_tag_len_code = pf.AuthTagLen
      if auth_tag_len_code >= 4:
        # [MQTT-SN-3.17.2.3-6]: tag length = AuthTagLen × 2 bytes
        tag_bytes = auth_tag_len_code * 2
      else:
        # 0x0 (provider-defined) or 0x1 (nominal size):
        # We cannot compute the length without scheme knowledge, so we store
        # everything remaining after the protected packet as the tag.
        # The protected packet is identified by reading its own length prefix.
        tag_bytes = None   # resolved below

      # Read the ProtectedMQTTSNPacket using its own length field
      if pos < packetlen:
        inner_len, inner_lenlen = PacketLens.decode(buffer[pos:])
        self.ProtectedMQTTSNPacket = buffer[pos:pos + inner_len]
        pos += inner_len
      else:
        self.ProtectedMQTTSNPacket = b""

      # AuthenticationTag: remainder of packet (or fixed size if known)
      if tag_bytes is not None:
        self.AuthenticationTag = buffer[pos:pos + tag_bytes]
      else:
        self.AuthenticationTag = buffer[pos:packetlen]

    except:
      logger.exception("Validating protection encapsulation packet")
      raise

  def __str__(self):
    return "ProtectionEncapsulation (" \
           + str(self.ProtectionFlags) + \
           ", ProtectionScheme=0x{:02X}".format(self.ProtectionScheme) + \
           ", SenderIdentifier=" + str(self.SenderIdentifier) + \
           ", Random=" + str(self.Random) + \
           ", CryptographicMaterial=" + str(self.CryptographicMaterial) + \
           ", MonotonicCounter=" + str(self.MonotonicCounter) + \
           ", ProtectedMQTTSNPacket=" + str(self.ProtectedMQTTSNPacket) + \
           ", AuthenticationTag=" + str(self.AuthenticationTag) + ")"

  def __eq__(self, packet):
    return self.ProtectionFlags       == packet.ProtectionFlags       and \
           self.ProtectionScheme      == packet.ProtectionScheme      and \
           self.SenderIdentifier      == packet.SenderIdentifier      and \
           self.Random                == packet.Random                 and \
           self.CryptographicMaterial == packet.CryptographicMaterial and \
           self.MonotonicCounter      == packet.MonotonicCounter      and \
           self.ProtectedMQTTSNPacket == packet.ProtectedMQTTSNPacket and \
           self.AuthenticationTag     == packet.AuthenticationTag


# classes[n] maps packet type integer n to its class.
# None entries mark packet types not yet implemented.
# The three encapsulation types (0xFD, 0xFE, 0xFF) are sparse and handled
# separately in the encapsulation_classes dict below.
classes = [None,           # 0   reserved
           Connects,       # 1   CONNECT
           Connacks,       # 2   CONNACK
           Publishes,      # 3   PUBLISH
           Pubacks,        # 4   PUBACK
           Pubrecs,        # 5   PUBREC
           Pubrels,        # 6   PUBREL
           Pubcomps,       # 7   PUBCOMP
           Subscribes,     # 8   SUBSCRIBE
           Subacks,        # 9   SUBACK
           Unsubscribes,   # 10  UNSUBSCRIBE
           Unsubacks,      # 11  UNSUBACK
           Pingreqs,       # 12  PINGREQ
           Pingresps,      # 13  PINGRESP
           Disconnects,    # 14  DISCONNECT
           Auths,          # 15  AUTH
           Registers,      # 16  REGISTER
           Regacks,        # 17  REGACK
           Pubwoses,       # 18  PUBWOS
           Sleepreqs,      # 19  SLEEPREQ
           Sleepresps,     # 20  SLEEPRESP
           Wakeups,        # 21  WAKEUP
           Advertises,     # 22  ADVERTISE
           Searchgws,      # 23  SEARCHGW
           Gwinfos]        # 24  GWINFO

# Sparse dict for the three high-value encapsulation type codes
encapsulation_classes = {
    PacketTypes.FOWARDER_ENCAPSULATION:    ForwarderEncapsulations,
    PacketTypes.CONNECTION_ENCAPSULATION:  ConnectionEncapsulations,
    PacketTypes.PROTECTION_ENCAPSULATION:  ProtectionEncapsulations,
}

def unpackPacket(buffer, maximumPacketSize=MAX_PACKET_SIZE):
  packet_type = PacketType(buffer)
  if packet_type is None:
    return None
  # Check sparse encapsulation types first
  if packet_type in encapsulation_classes:
    packet = encapsulation_classes[packet_type]()
    packet.unpack(buffer)
    return packet
  # Regular indexed types
  if packet_type < len(classes) and classes[packet_type] is not None:
    packet = classes[packet_type]()
    packet.unpack(buffer)
    return packet
  return None
