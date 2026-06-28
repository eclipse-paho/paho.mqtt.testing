"""
*******************************************************************
  Copyright (c) 2013, 2026 Ian Craggs, IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v2.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     https://www.eclipse.org/legal/epl-2.0/
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  AI Disclosure: This file was partly AI-generated. The AI-generated
  portions are made available under CC0-1.0 and not subject to the
  project's licence. The human contributor has reviewed and verified
  that the code is correct.
  
  SPDX-License-Identifier: EPL-2.0 and CC0-1.0

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

import traceback, random, sys, string, copy, threading, logging, socket, time, uuid

from mqtt.formats import MQTTSN2 as MQTTSN
from mqtt.formats.MQTTSN2 import ConnectProtection

from .Brokers import Brokers
from .MQTTSNProtection import KeyStore, verify_protection, wrap_protection, ProtectionError

logger = logging.getLogger('MQTT broker')

def respond(callback, packet=None):
  logger.debug("out: "+str(packet))
  if hasattr(callback[1], "handlePacket"):
    callback[1].handlePacket(packet)
  else:
    try:
      respondfn = callback[0]
      context = callback[1]
      respondfn(context, packet.pack())
    except:
      traceback.print_exc()

class MQTTSNClients:

  def __init__(self, anId, cleanStart, sessionExpiryInterval, keepalive, callback, broker):
    self.id = anId # required
    self.cleanStart = cleanStart
    # Brokers.py (the shared helper) reads aClient.cleansession to decide whether
    # to clear subscription/session state on connect - map CleanStart onto it.
    self.cleansession = cleanStart
    self.sessionExpiryInterval = sessionExpiryInterval
    self.sessionEndedTime = 0
    self.callback = callback
    self.msgid = 1
    self.outbound = [] # message objects - for ordering
    self.outmsgs = {} # msgids to message objects
    self.broker = broker
    if broker.publish_on_pubrel:
      self.inbound = {} # stored inbound QoS 2 publications
    else:
      self.inbound = []
    self.connected = False
    self.will = None
    self.keepalive = keepalive
    self.lastPacket = None
    # Session Topic Aliases (Section 4.7.2.2) - server assigned, per-Session
    self.topicNamesToAliases = {} # topic name -> alias, server's view of what it has told the client
    self.aliasesToTopicNames = {} # alias -> topic name
    self.nextTopicAlias = 1
    # Protection state: set when the client's CONNECT arrived inside a
    # Protection Encapsulation.  All subsequent inbound packets from this
    # client must also be protected [MQTT-SN-3.17-3], and all outbound
    # packets to this client must be protected by the server [MQTT-SN-3.17-2].
    self.protectionRequired  = False  # True once a protected CONNECT is seen
    self.protectionSenderId  = None   # 8-byte server sender-id used for outbound wrapping
    self.protectionScheme    = None   # scheme code used by the client; reused for outbound

  def assignTopicAlias(self, topicName):
    "find or create a Session Topic Alias for the given topic name (Section 4.7.2.2)"
    if topicName in self.topicNamesToAliases:
      return self.topicNamesToAliases[topicName]
    while self.nextTopicAlias in self.aliasesToTopicNames:
      self.nextTopicAlias += 1
    alias = self.nextTopicAlias
    self.topicNamesToAliases[topicName] = alias
    self.aliasesToTopicNames[alias] = topicName
    self.nextTopicAlias += 1
    return alias

  def getAliasTopic(self, alias):
    return self.aliasesToTopicNames.get(alias)

  def resend(self):
    logger.debug("resending unfinished publications %s", str(self.outbound))
    if len(self.outbound) > 0:
      logger.info("[MQTT-4.4.0-1] resending inflight QoS 1 and 2 messages")
    for pub in self.outbound:
      logger.debug("resending %s", pub)
      logger.info("[MQTT-4.4.0-2] dup flag must be set on in re-publish")
      if pub.Flags.QoS == 0:
        respond(self.callback, pub)
      elif pub.Flags.QoS == 1:
        pub.Flags.DUP = True
        logger.info("[MQTT-2.1.2-3] Dup when resending QoS 1 publish id %d", pub.PacketId)
        logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
        logger.info("[MQTT-4.3.2-1] Resending QoS 1 with DUP flag")
        respond(self.callback, pub)
      elif pub.Flags.QoS == 2:
        if pub.qos2state == "PUBREC":
          logger.info("[MQTT-2.1.2-3] Dup when resending QoS 2 publish id %d", pub.PacketId)
          pub.Flags.DUP = True
          logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
          logger.info("[MQTT-4.3.3-1] Resending QoS 2 with DUP flag")
          respond(self.callback, pub)
        else:
          resp = MQTTSN.Pubrels()
          logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
          resp.PacketId = pub.PacketId
          respond(self.callback, resp)

  def publishArrived(self, topic, msg, qos, retained=False):
    pub = MQTTSN.Publishes()
    logger.info("[MQTT-3.2.3-3] topic name must match the subscription's topic filter")
    # use a Session Topic Alias for this client if one has already been
    # established (Section 4.7.2.2), otherwise send the full topic name
    if topic in self.topicNamesToAliases:
      pub.Flags.TopicType = pub.Flags.TOPIC_TYPE_SESSION
      pub.TopicAlias = self.topicNamesToAliases[topic]
    else:
      pub.Flags.TopicType = pub.Flags.TOPIC_TYPE_NAME
      pub.TopicName = topic
    pub.Data = msg
    pub.Flags.QoS = qos
    pub.Flags.RETAIN = retained
    if retained:
      logger.info("[MQTT-2.1.2-7] Last retained message on matching topics sent on subscribe")
    if pub.Flags.RETAIN:
      logger.info("[MQTT-2.1.2-9] Set retained flag on retained messages")
    if qos == 2:
      pub.qos2state = "PUBREC"
    if qos in [1, 2]:
      pub.PacketId = self.msgid
      logger.debug("client id: %s msgid: %d", self.id, self.msgid)
      if self.msgid == MQTTSN.MAX_PACKETID:
        self.msgid = 1
      else:
        self.msgid += 1
      self.outbound.append(pub)
      self.outmsgs[pub.PacketId] = pub
    logger.info("[MQTT-4.6.0-6] publish packets must be sent in order of receipt from any given client")
    if self.connected:
      respond(self.callback, pub)
    else:
      if qos == 0 and not self.broker.dropQoS0:
        self.outbound.append(pub)
      if qos in [1, 2]:
        logger.info("[MQTT-3.1.2-5] storing of QoS 1 and 2 messages for disconnected client %s", self.id)

  def puback(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.Flags.QoS == 1:
        self.outbound.remove(pub)
        del self.outmsgs[msgid]
      else:
        logger.error("%s: Puback received for msgid %d, but QoS is %d", self.id, msgid, pub.Flags.QoS)
    else:
      logger.error("%s: Puback received for msgid %d, but no message found", self.id, msgid)

  def pubrec(self, msgid):
    rc = False
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.Flags.QoS == 2:
        if pub.qos2state == "PUBREC":
          pub.qos2state = "PUBCOMP"
          rc = True
        else:
          logger.error("%s: Pubrec received for msgid %d, but message in wrong state", self.id, msgid)
      else:
        logger.error("%s: Pubrec received for msgid %d, but QoS is %d", self.id, msgid, pub.Flags.QoS)
    else:
      logger.error("%s: Pubrec received for msgid %d, but no message found", self.id, msgid)
    return rc

  def pubcomp(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.Flags.QoS == 2:
        if pub.qos2state == "PUBCOMP":
          self.outbound.remove(pub)
          del self.outmsgs[msgid]
        else:
          logger.error("Pubcomp received for msgid %d, but message in wrong state", msgid)
      else:
        logger.error("Pubcomp received for msgid %d, but QoS is %d", msgid, pub.Flags.QoS)
    else:
      logger.error("Pubcomp received for msgid %d, but no message found", msgid)

  def pubrel(self, msgid):
    rc = None
    if self.broker.publish_on_pubrel:
      if msgid in self.inbound.keys():
        pub = self.inbound[msgid]
        if pub.Flags.QoS == 2:
          rc = pub
        else:
          logger.error("Pubrec received for msgid %d, but QoS is %d", msgid, pub.Flags.QoS)
    else:
      rc = msgid in self.inbound
    if not rc:
      logger.error("Pubrec received for msgid %d, but no message found", msgid)
    return rc


class MQTTSNBrokers:

  def __init__(self, publish_on_pubrel=True,
    overlapping_single=True,
    dropQoS0=True,
    zero_length_clientids=True,
    lock=None, sharedData={},
    key_store=None):

    # optional behaviours
    self.publish_on_pubrel = publish_on_pubrel
    self.dropQoS0 = dropQoS0                    # don't queue QoS 0 messages for disconnected clients
    self.zero_length_clientids = zero_length_clientids

    self.broker = Brokers(overlapping_single, sharedData=sharedData)
    self.key_store = key_store if key_store is not None else KeyStore()
    self.key_store.add_key(bytes.fromhex("DEADBEEFCAFEBABE"), 
        bytes.fromhex("00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F"
                      "10 11 12 13 14 15 16 17 18 19 1A 1B 1C 1D 1E 1F"))
    self.clients = {}   # callback -> clients
    if lock:
      logger.info("Using shared lock %d", id(lock))
      self.lock = lock
    else:
      self.lock = threading.RLock()

    logger.info("MQTT-SN Paho Test Broker")
    logger.info("Optional behaviour, publish on pubrel: %s", self.publish_on_pubrel)
    logger.info("Optional behaviour, single publish on overlapping topics: %s", self.broker.overlapping_single)
    logger.info("Optional behaviour, drop QoS 0 publications to disconnected clients: %s", self.dropQoS0)
    logger.info("Optional behaviour, support zero length clientids: %s", self.zero_length_clientids)

  def shutdown(self):
    # do we need to do anything here?
    pass

  def setBroker3(self, broker3):
    self.broker.setBroker3(broker3.broker)

  def setBroker5(self, broker5):
    self.broker.setBroker5(broker5.broker)

  def reinitialize(self):
    logger.info("Reinitializing broker")
    self.clients = {}
    self.broker.reinitialize()

  def handleRequest(self, raw_packet, client_address, callback):
    "this is going to be called from multiple threads, so synchronize"
    self.lock.acquire()
    terminate = False
    try:
      if raw_packet == None:
        # will message
        self.disconnect(client_address, None, terminate=True)
        terminate = True
      else:
        print("unpacking packet")
        connect_protection = None  # set below if the packet arrived protected
        packet = MQTTSN.unpackPacket(raw_packet)
        if packet:
          if isinstance(packet, MQTTSN.ProtectionEncapsulations):
            connect_protection = ConnectProtection(
                packet.SenderIdentifier, packet.ProtectionScheme)
            packet = self.unwrapProtection(packet, client_address, callback)
            if packet is None:
              return terminate  # error already sent; finally releases the lock
          if packet is not None:
            # Enforce protection requirement [MQTT-SN-3.17-3]:
            # if this client has an established session that required protection,
            # any unprotected inbound packet must be rejected.
            client = self.clients.get(client_address)
            if client is not None and client.protectionRequired and connect_protection is None:
              logger.error("[MQTT-SN-3.17-3] Unprotected packet received from client %s "
                           "whose session requires protection", client.id)
              self._send_only_protection_error(client_address, packet, callback)
              return terminate
            # For CONNECT packets only: attach protection metadata so that
            # connect() can record session protection state once the
            # MQTTSNClients object exists.  ConnectProtection is in Connects.names
            # so the setattr guard allows it; we do not annotate other packet types.
            if isinstance(packet, MQTTSN.Connects):
              packet.ConnectProtection = connect_protection
            terminate = self.handlePacket(packet, client_address, callback)
        else:
          raise MQTTSN.MQTTSNException("[MQTT-2.0.0-1] handleRequest: badly formed MQTT-SN packet")
    except:
      raise MQTTSN.MQTTSNException("Error handling packet")
    finally:
      self.lock.release()
    return terminate

  def handlePacket(self, packet, client_address, callback):
    terminate = False
    logger.debug("in: "+str(packet))
    if isinstance(packet, MQTTSN.ProtectionEncapsulations):
      packet = self.unwrapProtection(packet, client_address, callback)
      if packet is None:
        return terminate
    if client_address not in self.clients.keys() and not isinstance(packet, MQTTSN.Connects) and not \
      (isinstance(packet, MQTTSN.Pubwoses)):
      self.disconnect(client_address, packet)
      raise MQTTSN.MQTTSNException("[MQTT-3.1.0-1] Connect was not first packet on socket")
    else:
      getattr(self, MQTTSN.Packets.Names[packet.packetType].lower())(client_address, packet, callback)
      if client_address in self.clients.keys():
        self.clients[client_address].lastPacket = time.time()
    if packet.packetType == MQTTSN.PacketTypes.DISCONNECT:
      terminate = True
    return terminate

  def unwrapProtection(self, protection, client_address, callback):
    """Unwrap and cryptographically verify a Protection Encapsulation (Section 3.17).

    Delegates to MQTTSNProtection.verify_protection() which:
      - looks up the shared key via self.key_store using the SenderIdentifier,
      - verifies the AuthenticationTag using the negotiated protection scheme,
      - for AEAD schemes, also decrypts the ProtectedMQTTSNPacket ciphertext.

    On success, parses and returns the inner Packets object.
    On any failure (unknown sender, bad tag, unsupported scheme, parse error),
    sends DISCONNECT(Protection scheme invalid) and returns None.
    """
    logger.debug("Protection Encapsulation received: scheme=0x%02X sender=%s",
                 protection.ProtectionScheme,
                 protection.SenderIdentifier.hex())

    # Cryptographic verification (and decryption for AEAD schemes)
    inner_bytes = verify_protection(protection, self.key_store)
    if inner_bytes is None:
      logger.error("[MQTT-SN-3.17] Protection verification failed for sender %s",
                   protection.SenderIdentifier.hex())
      self._send_protection_error(client_address, callback)
      return None

    # Parse the verified plaintext as an MQTT-SN packet
    inner_packet = MQTTSN.unpackPacket(inner_bytes)
    if inner_packet is None:
      logger.error("[MQTT-SN-3.17] Could not parse inner packet from Protection Encapsulation")
      self._send_protection_error(client_address, callback)
      return None

    # [MQTT-SN-3.17.8-1] inner packet MUST NOT itself be a Protection Encapsulation
    if isinstance(inner_packet, MQTTSN.ProtectionEncapsulations):
      logger.error("[MQTT-SN-3.17.8-1] Nested Protection Encapsulation is not permitted")
      self._send_protection_error(client_address, callback)
      return None

    logger.debug("[MQTT-SN-3.17-1] Protection Encapsulation verified: inner type %d",
                 inner_packet.packetType)
    return inner_packet

  def _send_protection_error(self, client_address, callback):
    """Send DISCONNECT(Protection scheme invalid) and drop the client."""
    resp = MQTTSN.Disconnects()
    resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.DISCONNECT,
                                         "Protection scheme invalid")
    if callback:
      respond(callback, resp)
    self.disconnect(client_address, None)

  def _send_only_protection_error(self, client_address, packet, callback):
    """Reject an unprotected packet from a session that requires protection.

    Sends ReasonCode 0xE6 (Only protection packet supported) in the response
    packet type appropriate for the inbound packet type [MQTT-SN-3.17-3], then
    sends DISCONNECT and drops the client.
    """
    pt = packet.packetType
    resp = None
    rc_e6 = None
    # 0xE6 is valid on: CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP,
    #                   SUBACK, UNSUBACK, REGACK [per Table 2-5]
    if pt == MQTTSN.PacketTypes.CONNECT:
      resp = MQTTSN.Connacks()
      rc_e6 = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.CONNACK,
                                  "Only protection packet supported")
      resp.ReasonCode = rc_e6
    elif pt in (MQTTSN.PacketTypes.PUBLISH,
                MQTTSN.PacketTypes.PUBWOS):
      resp = MQTTSN.Pubacks()
      rc_e6 = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.PUBACK,
                                  "Only protection packet supported")
      resp.ReasonCode = rc_e6
      if hasattr(packet, "PacketId"):
        resp.PacketId = packet.PacketId
    elif pt == MQTTSN.PacketTypes.SUBSCRIBE:
      resp = MQTTSN.Subacks()
      rc_e6 = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.SUBACK,
                                  "Only protection packet supported")
      resp.ReasonCode = rc_e6
      resp.PacketId = packet.PacketId
    elif pt == MQTTSN.PacketTypes.UNSUBSCRIBE:
      resp = MQTTSN.Unsubacks()
      rc_e6 = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.UNSUBACK,
                                  "Only protection packet supported")
      resp.ReasonCode = rc_e6
      resp.PacketId = packet.PacketId
    elif pt == MQTTSN.PacketTypes.REGISTER:
      resp = MQTTSN.Regacks()
      rc_e6 = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.REGACK,
                                  "Only protection packet supported")
      resp.ReasonCode = rc_e6
      resp.PacketId = packet.PacketId
    # For packet types with no corresponding response carrying 0xE6
    # (e.g. PINGREQ, DISCONNECT, PUBREL), send DISCONNECT directly.
    if resp is not None and callback:
      respond(callback, resp)
    # Always follow with DISCONNECT and drop the client [MQTT-SN-3.17-3]
    disc = MQTTSN.Disconnects()
    disc.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.DISCONNECT,
                                          "Only protection packet supported")
    if callback:
      respond(callback, disc)
    self.disconnect(client_address, None)

  def _respond_protected(self, client, callback, packet):
    """Wrap packet in a Protection Encapsulation before sending, when the
    session requires protection [MQTT-SN-3.17-2].

    Uses the same scheme the client used for its CONNECT and the server's
    own sender identifier (looked up from self.key_store).
    If wrapping fails for any reason, the unprotected packet is sent and
    an error is logged; this avoids silently dropping responses.
    """
    if client is None or not client.protectionRequired:
      respond(callback, packet)
      return
    try:
      inner_bytes = packet.pack()
      prot = wrap_protection(
          inner_bytes,
          client.protectionSenderId,
          client.protectionScheme,
          self.key_store)
      respond(callback, prot)
    except ProtectionError as e:
      logger.error("[MQTT-SN-3.17-2] Failed to wrap outbound packet: %s", e)
      respond(callback, packet)  # best-effort fallback

  def connect(self, client_address, packet, callback):
    if packet.ProtocolVersion != 2:
      logger.error("[MQTT-SN-3.1.2-2] Wrong protocol version %d", packet.ProtocolVersion)
      resp = MQTTSN.Connacks()
      resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.CONNACK, "Unsupported protocol version")
      resp.PacketId = packet.PacketId
      respond(callback, resp)
      logger.info("[MQTT-SN-3.2.4-2] must delete the Virtual Connection after non-zero reason code")
      self.disconnect(client_address, None)
      logger.info("[MQTT-SN-3.1.4-5] When rejecting connect, no more data must be processed")
      return
    if client_address in self.clients.keys():    # is client already connected?
      self.disconnect(client_address, None)
      logger.info("[MQTT-SN-3.1.4-5] When rejecting connect, no more data must be processed")
      raise MQTTSN.MQTTSNException("[MQTT-3.1.0-2] Second connect packet")
    assignedClientId = False
    if len(packet.ClientId) == 0:
      if self.zero_length_clientids == False:
        logger.info("[MQTT-SN-3.1.3-9] if clientid is rejected, must send connack with non-zero reason code")
        resp = MQTTSN.Connacks()
        resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.CONNACK, "Client identifier not valid")
        resp.PacketId = packet.PacketId
        respond(callback, resp)
        logger.info("[MQTT-SN-3.2.4-2] must delete the Virtual Connection after non-zero reason code")
        self.disconnect(client_address, None)
        logger.info("[MQTT-SN-3.1.4-5] When rejecting connect, no more data must be processed")
        return
      else:
        packet.ClientId = str(uuid.uuid4()) # give the client a unique clientid
        assignedClientId = True
        logger.info("[MQTT-SN-3.2.11-2] 0-length clientid must be assigned a unique id %s", packet.ClientId)
    logger.info("[MQTT-SN-3.1.3-5] Clientids of 1 to 23 chars and ascii alphanumeric must be allowed")
    if packet.ClientId in [client.id for client in self.clients.values()]: # is this client already connected on a different connection?
      for c in list(self.clients.keys()):
        if self.clients[c].id == packet.ClientId:
          logger.info("[MQTT-SN-3.1.4-3] Disconnecting old client %s", packet.ClientId)
          self.disconnect(c, None)
          break
    me = None
    clean = False
    if packet.ConnectFlags.CleanStart:
      logger.info("[MQTT-SN-3.1.2.1-1] discard existing session when CleanStart set to 1")
      clean = True
      logger.info("[MQTT-SN-3.2.2.1-1] session present must be set to 0 if cleanstart is 1")
    else:
      me = self.broker.getClient(packet.ClientId) # find existing state, if there is any
      if me and me.sessionExpiryInterval is not None and me.sessionExpiryInterval != 0xFFFFFFFF \
          and time.time() - me.sessionEndedTime > me.sessionExpiryInterval:
        logger.info("[MQTT-SN-4.1.1-3] discard expired session state")
        me = None
      if me:
        logger.info("[MQTT-SN-3.1.3-2] clientid used to retrieve client state")
        logger.info("[MQTT-SN-3.2.2.1-2] session present must be set to 1")
      else:
        logger.info("[MQTT-SN-3.1.2.1-3] no existing session - create a new one")
    resp = MQTTSN.Connacks()
    # Session Expiry Interval: absent on the wire means 0 (Section 3.1.9)
    sessionExpiryInterval = packet.SessionExpiryInterval if packet.SessionExpiryInterval is not None else 0
    if me == None:
      me = MQTTSNClients(packet.ClientId, packet.ConnectFlags.CleanStart, sessionExpiryInterval,
                          packet.KeepAlive, callback, self)
    else:
      me.callback = callback # set existing client state to new callback/connection
      me.cleanStart = packet.ConnectFlags.CleanStart
      me.cleansession = packet.ConnectFlags.CleanStart
      me.keepalive = packet.KeepAlive
      me.sessionExpiryInterval = sessionExpiryInterval
    logger.info("[MQTT-4.1.0-1] server must store data for at least as long as the network connection lasts")
    self.clients[client_address] = me
    # Record protection state from the CONNECT packet [MQTT-SN-3.17-2/3].
    # The server re-uses the client's scheme for its own outbound packets,
    # identified by the server's own SenderIdentifier in the key store.
    # We use the client's SenderIdentifier as the server's own id for
    # simplicity — a real deployment would have a separate server id.
    if packet.ConnectProtection is not None:
      me.protectionRequired = True
      me.protectionScheme   = packet.ConnectProtection.scheme
      # Server uses its own sender id: the first registered key in the
      # key_store whose id matches the client's, i.e. the shared-key peer.
      # For the test broker we reuse the client's sender id as our own.
      me.protectionSenderId = packet.ConnectProtection.sender_id
      logger.info("[MQTT-SN-3.17-2] Session for client %s requires protection "
                  "(scheme 0x%02X, sender %s)",
                  me.id, me.protectionScheme, me.protectionSenderId.hex())
    if packet.ConnectFlags.Will:
      me.will = (packet.WillTopic, packet.WillFlags.WillQoS, packet.WillPayload, packet.WillFlags.WillRetain)
      logger.info("[MQTT-SN-3.1.2.2-2] the will message must be stored if the Will Flag is set")
    else:
      me.will = None
    self.broker.connect(me, clean)
    logger.info("[MQTT-SN-3.2.0-1] the first response to a client must be a connack")
    resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.CONNACK, "Success")
    resp.PacketId = packet.PacketId
    if assignedClientId:
      logger.info("[MQTT-SN-4.1.2-1] must return the assigned client id")
      resp.AssignedClientId = packet.ClientId
    self._respond_protected(me, callback, resp)
    me.resend()

  def disconnect(self, client_address, packet, callback=None, terminate=False):
    """
    Used both internally (to drop a client, e.g. on connect rejection or keepalive
    timeout) and as the dispatch target for an incoming DISCONNECT packet, in which
    case 'packet' is the DISCONNECT packet itself and carries an optional ReasonCode.
    """
    logger.info("[MQTT-3.14.4-2] Client must not send any more packets after disconnect")
    if client_address in self.clients.keys():
      me = self.clients[client_address]
      if packet is not None and isinstance(packet, MQTTSN.Disconnects):
        if packet.ReasonCode.value == 0:
          logger.info("[MQTT-SN-3.1.2.2-4] will message is deleted after normal disconnection")
          me.will = None
        else:
          logger.info("[MQTT-3.14.4-3] client requested disconnect with non-zero reason code %s",
                      str(packet.ReasonCode))
      if terminate:
        self.broker.terminate(me.id)
      else:
        self.broker.disconnect(me.id)
      me.sessionEndedTime = time.time()
      del self.clients[client_address]

  def disconnectAll(self):
    for client_address in list(self.clients.keys())[:]:
      self.disconnect(client_address, None)

  def register(self, client_address, packet, callback):
    "Client requests a Session Topic Alias for a Topic Name (Section 3.4 / 4.7.2.2)"
    me = self.clients[client_address]
    resp = MQTTSN.Regacks()
    resp.PacketId = packet.PacketId
    if packet.RegisterFlags.TopicAliasFlag:
      logger.error("[MQTT-SN-3.4-1] REGISTER from a Client must not contain a Topic Alias")
      resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.REGACK, "Protocol error")
    else:
      alias = me.assignTopicAlias(packet.TopicName)
      resp.RegackFlags.TopicType = 0  # Session Topic Alias
      resp.RegackFlags.TopicAliasFlag = True
      resp.TopicAlias = alias
      resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.REGACK, "Success")
    self._respond_protected(me, callback, resp)

  def subscribe(self, client_address, packet, callback):
    topic_type = packet.SubscribeFlags.TopicType
    if topic_type == 3:
      topic = packet.TopicFilter
    else:
      topic = self.clients[client_address].getAliasTopic(packet.TopicAlias)
    me = self.clients[client_address]
    self.broker.subscribe(me.id, topic, packet.SubscribeFlags.QoS)
    resp = MQTTSN.Subacks()
    logger.info("[MQTT-2.3.1-7][MQTT-3.8.4-2] Suback has same message id as subscribe")
    logger.info("[MQTT-3.8.4-1] Must respond with suback")
    resp.PacketId = packet.PacketId
    logger.info("[MQTT-3.8.4-5] return code must be returned for each topic in subscribe")
    resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.SUBACK, identifier=packet.SubscribeFlags.QoS)
    # [MQTT-SN-4.7.2.2-2] a non-wildcard Topic Filter subscription must be given a Topic Alias
    if topic_type == 3 and "+" not in topic and "#" not in topic:
      alias = me.assignTopicAlias(topic)
      resp.SubackFlags.TopicType = 0  # Session Topic Alias
      resp.SubackFlags.TopicAliasFlag = True
      resp.TopicAlias = alias
    self._respond_protected(me, callback, resp)

  def unsubscribe(self, client_address, packet, callback):
    if packet.UnsubscribeFlags.TopicType == 3:
      topic = packet.TopicFilter
    else:
      topic = self.clients[client_address].getAliasTopic(packet.TopicAlias)
    self.broker.unsubscribe(self.clients[client_address].id, topic)
    resp = MQTTSN.Unsubacks()
    logger.info("[MQTT-2.3.1-7] Unsuback has same message id as unsubscribe")
    logger.info("[MQTT-3.10.4-4] Unsuback must be sent - same message id as unsubscribe")
    me = self.clients[client_address]
    if len(me.outbound) > 0:
      logger.info("[MQTT-3.10.4-3] sending unsuback has no effect on outward inflight messages")
    resp.PacketId = packet.PacketId
    self._respond_protected(me, callback, resp)

  def pubwos(self, client_address, packet, callback):
    if packet.PubwosFlags.TopicType == 3:  # Topic Name
      topic = packet.TopicName.decode("utf-8") if isinstance(packet.TopicName, bytes) else packet.TopicName
    else:  # Predefined Topic Alias - application-specific mapping, not modelled here
      topic = str(packet.TopicAlias)
      logger.debug("topic alias %s", topic)
    self.broker.publish("$QoS-1", # no clientid for a session-less publish
           topic, packet.Data, 0, packet.PubwosFlags.Retain, time.monotonic())

  def publish(self, client_address, packet, callback):
    me = self.clients[client_address]
    if packet.Flags.TopicType == packet.Flags.TOPIC_TYPE_NAME:
      topicName = packet.TopicName.decode("utf-8") if isinstance(packet.TopicName, bytes) else packet.TopicName
    else:
      topicName = me.getAliasTopic(packet.TopicAlias)
      if topicName is None:
        logger.error("[MQTT-SN-F0] Unknown Topic Alias %d from client %s", packet.TopicAlias, me.id)
        topicName = None
    if packet.Flags.QoS == 0:
      if topicName is not None:
        self.broker.publish(me.id, topicName, packet.Data, packet.Flags.QoS, packet.Flags.RETAIN,
               time.monotonic())
    elif packet.Flags.QoS == 1:
      if packet.Flags.DUP:
        logger.info("[MQTT-3.3.1-3] Incoming publish DUP 1 ==> outgoing publish with DUP 0")
        logger.info("[MQTT-4.3.2-2] server must store message in accordance with QoS 1")
      resp = MQTTSN.Pubacks()
      logger.info("[MQTT-2.3.1-6] puback message id same as publish")
      resp.PacketId = packet.PacketId
      if topicName is None:
        resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.PUBACK, "Unknown Topic Alias")
      else:
        self.broker.publish(me.id, topicName, packet.Data, packet.Flags.QoS, packet.Flags.RETAIN,
               time.monotonic())
      self._respond_protected(me, callback, resp)
    elif packet.Flags.QoS == 2:
      if self.publish_on_pubrel:
        if packet.PacketId in me.inbound.keys():
          if packet.Flags.DUP == 0:
            logger.error("[MQTT-3.3.1-2] duplicate QoS 2 message id %d found with DUP 0", packet.PacketId)
          else:
            logger.info("[MQTT-3.3.1-2] DUP flag is 1 on redelivery")
        else:
          me.inbound[packet.PacketId] = packet
      else:
        if packet.PacketId in me.inbound:
          if packet.Flags.DUP == 0:
            logger.error("[MQTT-3.3.1-2] duplicate QoS 2 message id %d found with DUP 0", packet.PacketId)
          else:
            logger.info("[MQTT-3.3.1-2] DUP flag is 1 on redelivery")
        else:
          me.inbound.append(packet.PacketId)
          logger.info("[MQTT-4.3.3-2] server must store message in accordance with QoS 2")
          if topicName is not None:
            self.broker.publish(me.id, topicName, packet.Data, packet.Flags.QoS, packet.Flags.RETAIN,
                   time.monotonic())
      resp = MQTTSN.Pubrecs()
      logger.info("[MQTT-2.3.1-6] pubrec message id same as publish")
      resp.PacketId = packet.PacketId
      if topicName is None:
        resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.PUBREC, "Unknown Topic Alias")
      self._respond_protected(me, callback, resp)

  def pubrel(self, client_address, packet, callback):
    me = self.clients[client_address]
    pub = me.pubrel(packet.PacketId)
    if pub:
      if self.publish_on_pubrel:
        if pub.Flags.TopicType == pub.Flags.TOPIC_TYPE_NAME:
          topicName = pub.TopicName.decode("utf-8") if isinstance(pub.TopicName, bytes) else pub.TopicName
        else:
          topicName = me.getAliasTopic(pub.TopicAlias)
        if topicName is not None:
          self.broker.publish(me.id, topicName, pub.Data, pub.Flags.QoS, pub.Flags.RETAIN, time.monotonic())
        del me.inbound[packet.PacketId]
      else:
        me.inbound.remove(packet.PacketId)
    me = self.clients[client_address]
    resp = MQTTSN.Pubcomps()
    logger.info("[MQTT-2.3.1-6] pubcomp message id same as publish")
    resp.PacketId = packet.PacketId
    if not pub:
      resp.ReasonCode = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.PUBCOMP, "Packet identifier not found")
    self._respond_protected(me, callback, resp)

  def pingreq(self, client_address, packet, callback):
    me = self.clients[client_address]
    resp = MQTTSN.Pingresps()
    logger.info("[MQTT-3.12.4-1] sending pingresp in response to pingreq")
    resp.PacketId = packet.PacketId
    self._respond_protected(me, callback, resp)

  def puback(self, client_address, packet, callback):
    "confirmed reception of qos 1"
    self.clients[client_address].puback(packet.PacketId)

  def pubrec(self, client_address, packet, callback):
    "confirmed reception of qos 2"
    me = self.clients[client_address]
    if me.pubrec(packet.PacketId):
      logger.info("[MQTT-3.5.4-1] must reply with pubrel in response to pubrec")
      resp = MQTTSN.Pubrels()
      resp.PacketId = packet.PacketId
      self._respond_protected(me, me.callback, resp)

  def pubcomp(self, client_address, packet, callback):
    "confirmed reception of qos 2"
    self.clients[client_address].pubcomp(packet.PacketId)

  def keepalive(self, client_address):
    if client_address in self.clients.keys():
      client = self.clients[client_address]
      if client.keepalive > 0 and time.time() - client.lastPacket > client.keepalive * 1.5:
        # keep alive timeout
        logger.info("[MQTT-SN-3.1.2-22] keepalive timeout for client %s", client.id)
        self.disconnect(client_address, None, terminate=True)
