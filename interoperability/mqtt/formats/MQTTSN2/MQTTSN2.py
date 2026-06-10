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

class MessageTypes:

  indexes = [x for x in range(1, 0X19)] + [0XFD, 0XFE, 0XFF]

  # Packet types
  CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP, \
  SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, \
  PINGREQ, PINGRESP, DISCONNECT, \
  AUTH, REGISTER, REGACK, \
  PUBWOS, SLEEPREQ, SLEEPRESP, WAKEUP, \
  ADVERTISE, SEARCHGW, GWINFO, \
  FOWARDER_ENCAPSULATION, SESSION_ENCAPSULATION, PROTECTION_ENCAPSULATION = indexes

def MessageType(buffer):
  index = 1
  if buffer[0] == 1:
    index = 3
  return buffer[index]

class Messages(object):

  Names = ["Reserved", "Connect", "Connack", \
    "Publish", "Puback", "Pubrec", "Pubrel", "Pubcomp", \
    "Subscribe", "Suback", "Unsubscribe", "Unsuback", \
    "Pingreq", "Pingresp", "Disconnect", \
    "Auth", "Register", "Regack", \
    "Advertise", "SearchGW", "GWInfo", \
    "Fowarder Encapsulation", "Session Encapsulation", "Protection Encapsulation"]

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

class MessageLens:

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
          MessageTypes.CONNACK, MessageTypes.UNSUBACK,
          MessageTypes.REGACK,
          MessageTypes.PUBACK, MessageTypes.PUBREC,
          MessageTypes.PUBREL, MessageTypes.PUBCOMP,
          MessageTypes.SLEEPRESP, MessageTypes.AUTH],
        "Normal disconnection" : [MessageTypes.DISCONNECT],
        "Granted QoS 0"        : [MessageTypes.SUBACK],
      },
      0x01 : { "Granted QoS 1" : [MessageTypes.SUBACK] },
      0x02 : { "Granted QoS 2" : [MessageTypes.SUBACK] },
 
      0x04 : { "Disconnect with will message" :
               [MessageTypes.DISCONNECT] },
 
      0x10 : { "No matching subscribers" :
               [MessageTypes.PUBACK, MessageTypes.PUBREC] },
      0x11 : { "No subscription existed" : [MessageTypes.UNSUBACK] },
 
      0x18 : { "Continue authentication" : [MessageTypes.AUTH] },
      0x19 : { "Re-authenticate"         : [MessageTypes.AUTH] },
 
      # MQTT-SN only: a Session or Predefined Topic Alias already exists
      0x1A : { "Topic Alias Exists" : [MessageTypes.REGACK] },
 
      # -----------------------------------------------------------------------
      # Error codes (>= 0x80)
      # -----------------------------------------------------------------------
 
      0x80 : { "Unspecified error" : [
               MessageTypes.CONNACK, MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.DISCONNECT] },
 
      0x81 : { "Malformed packet" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x82 : { "Protocol error" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x83 : { "Implementation specific error" : [
               MessageTypes.CONNACK, MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.REGACK, MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.DISCONNECT] },
 
      0x84 : { "Unsupported protocol version" : [MessageTypes.CONNACK] },
      0x85 : { "Client identifier not valid"  : [MessageTypes.CONNACK] },
      0x86 : { "Bad user name or password"    : [MessageTypes.CONNACK] },
 
      0x87 : { "Not authorized" : [
               MessageTypes.CONNACK, MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.REGACK, MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.DISCONNECT] },
 
      0x88 : { "Server unavailable"   : [MessageTypes.CONNACK] },
      0x89 : { "Server busy"          :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x8A : { "Banned"               : [MessageTypes.CONNACK] },
      0x8B : { "Server shutting down" : [MessageTypes.DISCONNECT] },
      0x8C : { "Bad authentication method" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x8D : { "Keep alive timeout"   : [MessageTypes.DISCONNECT] },
      0x8E : { "Session taken over"   : [MessageTypes.DISCONNECT] },
 
      0x8F : { "Topic filter invalid" : [
               MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.DISCONNECT] },
      0x90 : { "Topic name invalid" : [
               MessageTypes.CONNACK, MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.DISCONNECT] },
 
      0x91 : { "Packet identifier in use" : [
               MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.REGACK,
               MessageTypes.PINGRESP, MessageTypes.SLEEPRESP] },
      0x92 : { "Packet identifier not found" :
               [MessageTypes.PUBREL, MessageTypes.PUBCOMP] },
 
      0x93 : { "Receive maximum exceeded" : [MessageTypes.DISCONNECT] },
      0x94 : { "Topic alias invalid"      : [MessageTypes.DISCONNECT] },
      0x95 : { "Packet too large" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x96 : { "Packet rate too high"   : [MessageTypes.DISCONNECT] },
      0x97 : { "Quota exceeded" : [
               MessageTypes.REGACK, MessageTypes.SUBACK,
               MessageTypes.DISCONNECT] },
      0x98 : { "Administrative action"   : [MessageTypes.DISCONNECT] },
 
      0x99 : { "Payload format invalid" : [
               MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.DISCONNECT] },
 
      0x9A : { "Retain not supported" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x9B : { "QoS not supported" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x9C : { "Use another server" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x9D : { "Server moved" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
      0x9E : { "Shared subscription not supported" :
               [MessageTypes.SUBACK, MessageTypes.DISCONNECT] },
      0x9F : { "Connection rate exceeded" :
               [MessageTypes.CONNACK, MessageTypes.DISCONNECT] },
 
      # 0xA0 = 160 decimal; the spec table's hex column reads "0xAD" which is
      # a typo — the decimal ordering (0x9F, 0xA0, 0xA1, 0xA2) is unambiguous
      0xA0 : { "Maximum connect time"  : [MessageTypes.DISCONNECT] },
 
      0xA1 : { "Subscription identifiers not supported" :
               [MessageTypes.SUBACK, MessageTypes.DISCONNECT] },
      0xA2 : { "Wildcard subscription not supported" :
               [MessageTypes.SUBACK, MessageTypes.DISCONNECT] },
 
      # -----------------------------------------------------------------------
      # MQTT-SN-specific codes (>= 0xE6)
      # -----------------------------------------------------------------------
 
      # 0xE6: receiver expected a PROTECTION-encapsulated packet
      0xE6 : { "Only protection packet supported" : [
               MessageTypes.CONNACK, MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.PUBREL, MessageTypes.PUBCOMP,
               MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.REGACK, MessageTypes.DISCONNECT] },
      0xE7 : { "Protection scheme invalid"         : [MessageTypes.DISCONNECT] },
      0xE8 : { "Unknown Sender Id"                 : [MessageTypes.DISCONNECT] },
 
      0xF0 : { "Unknown Topic Alias" : [
               MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.SUBACK, MessageTypes.UNSUBACK,
               MessageTypes.REGACK] },
      0xF1 : { "Congestion" : [
               MessageTypes.CONNACK, MessageTypes.PUBACK, MessageTypes.PUBREC,
               MessageTypes.SUBACK, MessageTypes.REGACK] },
      0xF2 : { "Protection packet not supported"          : [MessageTypes.DISCONNECT] },
      0xF3 : { "Forwarder Encapsulation not supported"    : [MessageTypes.DISCONNECT] },
      0xF4 : { "No Virtual Connection exists"             : [MessageTypes.DISCONNECT] },
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
  Bit 4: Default Awake Messages Flag   (DAM)   — governs presence of DefaultAwakeMessages field
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


class Connects(Messages):
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
         ["messageType",
          "ConnectFlags", "WillFlags",
          "PacketId", "ProtocolVersion", "KeepAlive", "MaxPacketSize",
          "DefaultAwakeMessages", "SessionExpiryInterval",
          "WillTopic", "WillPayload",
          "AuthMethod", "AuthData", "ClientId"])
    self.messageType = MessageTypes.CONNECT

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
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    cf = self.ConnectFlags
    # Derive flags from which optional fields are present
    cf.Will    = (self.WillTopic is not None)
    cf.Auth    = (self.AuthMethod is not None)
    cf.SessExp = (self.SessionExpiryInterval is not None)
    cf.DAM     = (self.DefaultAwakeMessages is not None)

    body = bytes([MessageTypes.CONNECT]) + cf.pack()
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
    assert MessageType(buffer) == MessageTypes.CONNECT
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
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

      # Optional: Default Awake Messages (1 byte)
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
      if pos < messagelen:
        self.ClientId = buffer[pos:messagelen].decode("utf-8")
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

class Connacks(Messages):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["messageType", "Flags", "PacketId", "ReasonCode", "AssignedClientId"])
    self.messageType = MessageTypes.CONNACK
    self.Flags = 0
    self.PacketId = 0
    self.ReasonCode = 0
    self.AssignedClientId = ""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    msglen = 6 + len(self.AssignedClientId)
    buffer = bytes([msglen, MessageTypes.CONNACK]) + bytes([self.Flags]) +\
      writeInt16(self.PacketId) + bytes([self.ReasonCode]) + writeData(self.AssignedClientId)
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 3
    assert MessageType(buffer) == MessageTypes.CONNACK
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      # self.ConnectFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      self.ReasonCode = buffer[lenlen + 4]
    except:
      logger.exception("Validating connack packet")
      raise

  def __str__(self):
    return "Connack (" +\
        ", PacketId="+str(self.PacketId) +\
        ", ReasonCode="+str(self.ReasonCode) + ")"

  def __eq__(self, packet):
    rc = self.ReasonCode == packet.ReasonCode
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

class Publishes(Messages):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["Flags", "TopicAlias", "TopicName", "PacketId", "Data"])
    object.__setattr__(self, "messageType", MessageTypes.PUBLISH)
    self.Flags = PublishFlags()
    self.TopicAlias = 0
    self.TopicName = None
    self.PacketId = 0
    self.Data = b""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    body = bytes([self.messageType]) + self.Flags.pack()
    if self.Flags.TopicType == self.Flags.TOPIC_TYPE_NAME:
      body += writeLenData(self.TopicName)
    else:
      body += writeInt16(self.TopicAlias)
    if self.Flags.QoS != 0:
      body += writeInt16(self.PacketId)
    body += writeData(self.Data)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.PUBLISH
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      curlen = lenlen + 1 # add byte for packet type
      self.Flags.unpack(buffer[curlen])
      curlen += 1
      if self.Flags.TopicType == self.Flags.TOPIC_TYPE_NAME:
        namelen = readInt16(buffer[curlen:])
        curlen += 2
        self.TopicName = buffer[curlen:curlen + namelen]
        curlen += namelen 
      else:
        self.TopicAlias = readInt16(buffer[curlen:])
        curlen += 2
      if self.Flags.QoS != 0:
        self.PacketId = readInt16(buffer[curlen:])
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
  

class Acks(Messages):
  """
  Base class for PUBACK, PUBREC, PUBREL, and PUBCOMP (Sections 3.6.4-3.6.7).
 
  All four share the same wire format:
    length(1) + type(1) + packetid(2) + [reasoncode(1, optional)]
 
  The ReasonCode field is absent from the wire when its value is 0x00
  (success); the receiver infers success from the reduced packet length.
  """
 
  def __init__(self, messageType, buffer=None):
    object.__setattr__(self, "names", ["PacketId", "ReasonCode"])
    object.__setattr__(self, "messageType", messageType)
    self.PacketId = 0
    self.ReasonCode = 0  # 0x00 = success; omitted from wire when success
    if buffer != None:
      self.unpack(buffer)
 
  def pack(self):
    # body: type(1) + packetid(2) + [reasoncode(1, if not success)]
    body = bytes([self.messageType]) + writeInt16(self.PacketId)
    if self.ReasonCode != 0:
      body += bytes([self.ReasonCode])
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body
 
  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == self.messageType
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      # ReasonCode is optional: present when packet length allows (Section 3.6.4-7)
      if messagelen > lenlen + 3:
        self.ReasonCode = buffer[lenlen + 3]
      else:
        self.ReasonCode = 0  # absent means success
    except:
      logger.exception("Validating %s packet" % self.__class__.__name__)
      raise
 
  def __str__(self):
    # class names are e.g. "Pubacks"; strip the trailing 's' for the label
    return self.__class__.__name__[:-1] + \
           " (PacketId=" + str(self.PacketId) + \
           ", ReasonCode=" + str(self.ReasonCode) + ")"
 
  def __eq__(self, packet):
    return self.messageType == packet.messageType and \
           self.PacketId == packet.PacketId and \
           self.ReasonCode == packet.ReasonCode
 
 
class Pubacks(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, MessageTypes.PUBACK, buffer)
 
class Pubrecs(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, MessageTypes.PUBREC, buffer)
 
class Pubrels(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, MessageTypes.PUBREL, buffer)
 
class Pubcomps(Acks):
  def __init__(self, buffer=None):
    Acks.__init__(self, MessageTypes.PUBCOMP, buffer)
 

class SubscribeFlags:
  """
  Subscribe Flags byte structure (Section 3.7.2)
 
  Bit 7:    No Local
  Bits 6-5: QoS
  Bit 4:    Retain as Published (RaP)
  Bits 3-2: Retain Handling
  Bits 1-0: Topic Type
  """
 
  def __init__(self):
    self.TopicType = 0        # 2 bits: 0=Session, 1=Predefined, 3=Filter
    self.RetainHandling = 0   # 2 bits: 0, 1, or 2
    self.RaP = False          # 1 bit: Retain as Published
    self.QoS = 0              # 2 bits: 0, 1, or 2
    self.NoLocal = False      # 1 bit
 
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

class Subscribes(Messages):
 
  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType",
          "SubscribeFlags",
          "PacketId", "TopicAlias", "TopicFilter"])
    self.messageType = MessageTypes.SUBSCRIBE
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
    buffer = bytes([msglen, MessageTypes.SUBSCRIBE]) + \
             self.SubscribeFlags.pack() + \
             writeInt16(self.PacketId) + \
             topic_data
    return buffer
 
  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.SUBSCRIBE
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.SubscribeFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      topic_type = self.SubscribeFlags.TopicType
      if topic_type == 3:  # Topic Filter: fills to end of packet
        self.TopicFilter = buffer[lenlen + 4:messagelen].decode("utf-8")
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
 
class Subacks(Messages):
 
  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType",
          "SubackFlags",
          "PacketId", "TopicAlias", "ReasonCode"])
    self.messageType = MessageTypes.SUBACK
    self.SubackFlags = SubackFlags()
    self.PacketId = 0
    self.TopicAlias = 0    # present only when SubackFlags.TopicAliasFlag is True
    self.ReasonCode = 0    # optional; 0x00=success/QoS0, 0x01=QoS1, 0x02=QoS2
    if buffer != None:
      self.unpack(buffer)
 
  def pack(self):
    # body: type(1) + flags(1) + packetid(2) + [alias(2)] + [reason_code(1)]
    body = bytes([MessageTypes.SUBACK]) + \
           self.SubackFlags.pack() + \
           writeInt16(self.PacketId)
    if self.SubackFlags.TopicAliasFlag:
      body += writeInt16(self.TopicAlias)
    # Reason Code is optional: omit when success and no topic alias (Section 3.8.5)
    if self.ReasonCode != 0 or self.SubackFlags.TopicAliasFlag:
      body += bytes([self.ReasonCode])
    msglen = 1 + len(body)  # length field(1) + body
    buffer = bytes([msglen]) + body
    return buffer
 
  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.SUBACK
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.SubackFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4  # next byte after packetid
      if self.SubackFlags.TopicAliasFlag:
        self.TopicAlias = readInt16(buffer[pos:])
        pos += 2
      else:
        self.TopicAlias = 0
      # Reason Code: present when packet is long enough (Section 3.8.5)
      if pos < messagelen:
        self.ReasonCode = buffer[pos]
      else:
        self.ReasonCode = 0  # absent means success
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
           self.ReasonCode == packet.ReasonCode
 
 
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


class Unsubscribes(Messages):
  """
  UNSUBSCRIBE packet (Section 3.9).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               topic_alias(2)  [TopicType 0 or 1]  OR
               topic_filter    [TopicType 3, fills to end of packet]
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType",
          "UnsubscribeFlags",
          "PacketId", "TopicAlias", "TopicFilter"])
    self.messageType = MessageTypes.UNSUBSCRIBE
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
    buffer = bytes([msglen, MessageTypes.UNSUBSCRIBE]) + \
             self.UnsubscribeFlags.pack() + \
             writeInt16(self.PacketId) + \
             topic_data
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.UNSUBSCRIBE
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.UnsubscribeFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      topic_type = self.UnsubscribeFlags.TopicType
      if topic_type == 3:  # Topic Filter: fills to end of packet
        self.TopicFilter = buffer[lenlen + 4:messagelen].decode("utf-8")
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


class Unsubacks(Messages):
  """
  UNSUBACK packet (Section 3.10).

  Wire format: length + type(1) + packetid(2) + [reasoncode(1, optional)]

  The ReasonCode is absent from the wire when its value is 0x00 (success);
  the receiver infers success from the reduced packet length.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["messageType", "PacketId", "ReasonCode"])
    self.messageType = MessageTypes.UNSUBACK
    self.PacketId = 0
    self.ReasonCode = 0  # 0x00 = success; omitted from wire when success
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + packetid(2) + [reasoncode(1, if not success)]
    body = bytes([MessageTypes.UNSUBACK]) + writeInt16(self.PacketId)
    if self.ReasonCode != 0:
      body += bytes([self.ReasonCode])
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.UNSUBACK
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      # ReasonCode is optional: present when packet length allows (Section 3.10.3)
      if messagelen > lenlen + 3:
        self.ReasonCode = buffer[lenlen + 3]
      else:
        self.ReasonCode = 0  # absent means success
    except:
      logger.exception("Validating unsuback packet")
      raise

  def __str__(self):
    return "Unsuback (PacketId=" + str(self.PacketId) + \
           ", ReasonCode=" + str(self.ReasonCode) + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId and \
           self.ReasonCode == packet.ReasonCode


class Pingreqs(Messages):
  """
  PINGREQ packet (Section 3.11).

  Wire format: length + type(1) + packetid(2)
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["messageType", "PacketId"])
    self.messageType = MessageTypes.PINGREQ
    self.PacketId = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + packetid(2)
    body = bytes([MessageTypes.PINGREQ]) + writeInt16(self.PacketId)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.PINGREQ
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
    except:
      logger.exception("Validating pingreq packet")
      raise

  def __str__(self):
    return "Pingreq (PacketId=" + str(self.PacketId) + ")"

  def __eq__(self, packet):
    return self.PacketId == packet.PacketId


class Pingresps(Messages):
  """
  PINGRESP packet (Section 3.12).

  Wire format: length + type(1) + packetid(2) + [AppMsgsRemaining(1, optional)]

  AppMsgsRemaining is optional; its existence is inferred from the packet length.
  Values: 0 = none waiting; 1-254 = that many waiting; 255 = unspecified positive number.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["messageType", "PacketId", "AppMsgsRemaining"])
    self.messageType = MessageTypes.PINGRESP
    self.PacketId = 0
    self.AppMsgsRemaining = None  # None means field is absent from wire
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + packetid(2) + [AppMsgsRemaining(1)]
    body = bytes([MessageTypes.PINGRESP]) + writeInt16(self.PacketId)
    if self.AppMsgsRemaining is not None:
      body += bytes([self.AppMsgsRemaining])
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.PINGRESP
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      # AppMsgsRemaining is optional (Section 3.12.3)
      if messagelen > lenlen + 3:
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


class Disconnects(Messages):
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
         ["messageType",
          "DisconnectFlags",
          "PacketId", "ReasonCode", "SessionExpiryInterval", "ReasonString"])
    self.messageType = MessageTypes.DISCONNECT
    self.DisconnectFlags = DisconnectFlags()
    self.PacketId = 0
    self.ReasonCode = 0                 # 0x00 = Normal disconnection
    self.SessionExpiryInterval = None   # None means absent from wire
    self.ReasonString = None            # None means absent from wire
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    flags = self.DisconnectFlags
    # Derive flags from which fields are set
    flags.PacketIdFlag      = (self.PacketId != 0)
    flags.ReasonCodeFlag    = (self.ReasonCode != 0)
    flags.SessionExpiryFlag = (self.SessionExpiryInterval is not None)
    body = bytes([MessageTypes.DISCONNECT]) + flags.pack()
    if flags.PacketIdFlag:
      body += writeInt16(self.PacketId)
    if flags.ReasonCodeFlag:
      body += bytes([self.ReasonCode])
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
    assert MessageType(buffer) == MessageTypes.DISCONNECT
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.DisconnectFlags.unpack(buffer[lenlen + 1])
      flags = self.DisconnectFlags
      pos = lenlen + 2  # byte after flags
      if flags.PacketIdFlag:
        self.PacketId = readInt16(buffer[pos:])
        pos += 2
      else:
        self.PacketId = 0
      if flags.ReasonCodeFlag:
        self.ReasonCode = buffer[pos]
        pos += 1
      else:
        self.ReasonCode = 0
      if flags.SessionExpiryFlag:
        self.SessionExpiryInterval = (buffer[pos] << 24) | (buffer[pos+1] << 16) | \
                                     (buffer[pos+2] << 8) | buffer[pos+3]
        pos += 4
      else:
        self.SessionExpiryInterval = None
      # ReasonString is optional remainder of packet
      if pos < messagelen:
        self.ReasonString = buffer[pos:messagelen].decode("utf-8")
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
           self.ReasonCode == packet.ReasonCode and \
           self.SessionExpiryInterval == packet.SessionExpiryInterval and \
           self.ReasonString == packet.ReasonString


class Auths(Messages):
  """
  AUTH packet (Section 3.3).

  Wire format: length + type(1) + packetid(2) + reasoncode(1) +
               authmethodlen(2) + authmethod(utf-8) + authdata(binary)

  Authentication Method is a UTF-8 string preceded by a 2-byte length.
  Authentication Data follows immediately and runs to the end of the packet.
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType", "PacketId", "ReasonCode", "AuthMethod", "AuthData"])
    self.messageType = MessageTypes.AUTH
    self.PacketId = 0
    self.ReasonCode = 0     # 0x00 = Success
    self.AuthMethod = ""
    self.AuthData = b""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    method_bytes = writeData(self.AuthMethod)
    body = bytes([MessageTypes.AUTH]) + \
           writeInt16(self.PacketId) + \
           bytes([self.ReasonCode]) + \
           writeInt16(len(method_bytes)) + \
           method_bytes + \
           writeData(self.AuthData)
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.AUTH
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.PacketId = readInt16(buffer[lenlen + 1:])
      self.ReasonCode = buffer[lenlen + 3]
      method_len = readInt16(buffer[lenlen + 4:])
      pos = lenlen + 6
      self.AuthMethod = buffer[pos:pos + method_len].decode("utf-8")
      pos += method_len
      self.AuthData = buffer[pos:messagelen]
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
           self.ReasonCode == packet.ReasonCode and \
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


class Registers(Messages):
  """
  REGISTER packet (Section 3.4).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               [topicalias(2)]  if TopicAliasFlag set  +
               topicname(utf-8, fills remainder)
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType", "RegisterFlags", "PacketId", "TopicAlias", "TopicName"])
    self.messageType = MessageTypes.REGISTER
    self.RegisterFlags = RegisterFlags()
    self.PacketId = 0
    self.TopicAlias = 0   # present only when RegisterFlags.TopicAliasFlag is True
    self.TopicName = ""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    self.RegisterFlags.TopicAliasFlag = (self.TopicAlias != 0)
    topic_bytes = writeData(self.TopicName)
    body = bytes([MessageTypes.REGISTER]) + \
           self.RegisterFlags.pack() + \
           writeInt16(self.PacketId)
    if self.RegisterFlags.TopicAliasFlag:
      body += writeInt16(self.TopicAlias)
    body += topic_bytes
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.REGISTER
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.RegisterFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4  # byte after packetid
      if self.RegisterFlags.TopicAliasFlag:
        self.TopicAlias = readInt16(buffer[pos:])
        pos += 2
      else:
        self.TopicAlias = 0
      self.TopicName = buffer[pos:messagelen].decode("utf-8")
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


class Regacks(Messages):
  """
  REGACK packet (Section 3.5).

  Wire format: length + type(1) + flags(1) + packetid(2) +
               [topicalias(2)]  if TopicAliasFlag set  +
               [reasoncode(1)]  optional, inferred from packet length
  """

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType", "RegackFlags", "PacketId", "TopicAlias", "ReasonCode"])
    self.messageType = MessageTypes.REGACK
    self.RegackFlags = RegackFlags()
    self.PacketId = 0
    self.TopicAlias = 0    # present only when RegackFlags.TopicAliasFlag is True
    self.ReasonCode = 0    # optional; 0x00=success; inferred from length
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    # body: type(1) + flags(1) + packetid(2) + [alias(2)] + [reasoncode(1)]
    body = bytes([MessageTypes.REGACK]) + \
           self.RegackFlags.pack() + \
           writeInt16(self.PacketId)
    if self.RegackFlags.TopicAliasFlag:
      body += writeInt16(self.TopicAlias)
    if self.ReasonCode != 0 or self.RegackFlags.TopicAliasFlag:
      body += bytes([self.ReasonCode])
    msglen = 1 + len(body)  # length field(1) + body
    return bytes([msglen]) + body

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.REGACK
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.RegackFlags.unpack(buffer[lenlen + 1])
      self.PacketId = readInt16(buffer[lenlen + 2:])
      pos = lenlen + 4  # byte after packetid
      if self.RegackFlags.TopicAliasFlag:
        self.TopicAlias = readInt16(buffer[pos:])
        pos += 2
      else:
        self.TopicAlias = 0
      # ReasonCode: present when packet is long enough (Section 3.5.5)
      if pos < messagelen:
        self.ReasonCode = buffer[pos]
      else:
        self.ReasonCode = 0  # absent means success
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
           self.ReasonCode == packet.ReasonCode


# classes[n] maps packet type integer n to its class.
# None entries mark packet types not yet implemented.
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
           Regacks]        # 17  REGACK

def unpackPacket(buffer, maximumPacketSize=MAX_PACKET_SIZE):
  packet_type = MessageType(buffer)
  if packet_type != None and packet_type < len(classes) and classes[packet_type] != None:
    packet = classes[packet_type]()
    packet.unpack(buffer) #, maximumPacketSize=maximumPacketSize)
  else:
    packet = None
  return packet
