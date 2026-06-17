"""
*******************************************************************
  Copyright (c) 2013, 2026 Ian Craggs, IBM Corp.

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

import traceback, random, sys, string, copy, threading, logging, socket, time, uuid

from mqtt.formats import MQTTSN2 as MQTTSN

from .Brokers import Brokers

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

  def __init__(self, anId, cleansession, keepalive, socket, broker):
    self.id = anId # required
    self.cleansession = cleansession
    self.socket = socket
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

  def resend(self):
    logger.debug("resending unfinished publications %s", str(self.outbound))
    if len(self.outbound) > 0:
      logger.info("[MQTT-4.4.0-1] resending inflight QoS 1 and 2 messages")
    for pub in self.outbound:
      logger.debug("resending", pub)
      logger.info("[MQTT-4.4.0-2] dup flag must be set on in re-publish")
      if pub.fh.QoS == 0:
        respond(self.socket, pub)
      elif pub.fh.QoS == 1:
        pub.fh.DUP = 1
        logger.info("[MQTT-2.1.2-3] Dup when resending QoS 1 publish id %d", pub.messageIdentifier)
        logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
        logger.info("[MQTT-4.3.2-1] Resending QoS 1 with DUP flag")
        respond(self.socket, pub)
      elif pub.fh.QoS == 2:
        if pub.qos2state == "PUBREC":
          logger.info("[MQTT-2.1.2-3] Dup when resending QoS 2 publish id %d", pub.messageIdentifier)
          pub.fh.DUP = 1
          logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
          logger.info("[MQTT-4.3.3-1] Resending QoS 2 with DUP flag")
          respond(self.socket, pub)
        else:
          resp = MQTTSN.Pubrels()
          logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
          resp.messageIdentifier = pub.messageIdentifier
          respond(self.socket, resp)

  def publishArrived(self, topic, msg, qos, retained=False):
    pub = MQTTSN.Publishes()
    logger.info("[MQTT-3.2.3-3] topic name must match the subscription's topic filter")
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
      logger.debug("client id: %d msgid: %d", self.id, self.msgid)
      if self.msgid == 65535:
        self.msgid = 1
      else:
        self.msgid += 1
      self.outbound.append(pub)
      self.outmsgs[pub.PacketId] = pub
    logger.info("[MQTT-4.6.0-6] publish packets must be sent in order of receipt from any given client")
    if self.connected:
      respond(self.socket, pub)
    else:
      if qos == 0 and not self.broker.dropQoS0:
        self.outbound.append(pub)
      if qos in [1, 2]:
        logger.info("[MQTT-3.1.2-5] storing of QoS 1 and 2 messages for disconnected client %s", self.id)

  def puback(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 1:
        self.outbound.remove(pub)
        del self.outmsgs[msgid]
      else:
        logger.error("%s: Puback received for msgid %d, but QoS is %d", self.id, msgid, pub.fh.QoS)
    else:
      logger.error("%s: Puback received for msgid %d, but no message found", self.id, msgid)

  def pubrec(self, msgid):
    rc = False
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 2:
        if pub.qos2state == "PUBREC":
          pub.qos2state = "PUBCOMP"
          rc = True
        else:
          logger.error("%s: Pubrec received for msgid %d, but message in wrong state", self.id, msgid)
      else:
        logger.error("%s: Pubrec received for msgid %d, but QoS is %d", self.id, msgid, pub.fh.QoS)
    else:
      logger.error("%s: Pubrec received for msgid %d, but no message found", self.id, msgid)
    return rc

  def pubcomp(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 2:
        if pub.qos2state == "PUBCOMP":
          self.outbound.remove(pub)
          del self.outmsgs[msgid]
        else:
          logger.error("Pubcomp received for msgid %d, but message in wrong state", msgid)
      else:
        logger.error("Pubcomp received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:
      logger.error("Pubcomp received for msgid %d, but no message found", msgid)

  def pubrel(self, msgid):
    rc = None
    if self.broker.publish_on_pubrel:
      if msgid in self.inbound.keys():
        pub = self.inbound[msgid]
        if pub.fh.QoS == 2:
          rc = pub
        else:
          logger.error("Pubrec received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
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
    lock=None, sharedData={}):

    # optional behaviours
    self.publish_on_pubrel = publish_on_pubrel
    self.dropQoS0 = dropQoS0                    # don't queue QoS 0 messages for disconnected clients
    self.zero_length_clientids = zero_length_clientids

    self.broker = Brokers(overlapping_single, sharedData=sharedData)
    self.clients = {}   # socket -> clients
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
        self.disconnect(sock, None, terminate=True)
        terminate = True
      else:
        packet = MQTTSN.unpackPacket(raw_packet)
        if packet:
          terminate = self.handlePacket(packet, client_address, callback)
        else:
          raise MQTTSN.MQTTSNException("[MQTT-2.0.0-1] handleRequest: badly formed MQTT packet")
    except:
      raise MQTTSN.MQTTSNException("Error handling packet")
    finally:
      self.lock.release()
    return terminate

  def handlePacket(self, packet, client_address, callback):
    terminate = False
    logger.debug("in: "+str(packet))
    if client_address not in self.clients.keys() and not isinstance(packet, MQTTSN.Connects) and not \
      (isinstance(packet, MQTTSN.Publishes) and packet.Flags.QoS == -1):
      #print(self.clients.keys(), sock)
      self.disconnect(client_address, packet)
      raise MQTTSN.MQTTSNException("[MQTT-3.1.0-1] Connect was not first packet on socket")
    else:
      getattr(self, MQTTSN.Messages.Names[packet.messageType].lower())(client_address, packet, callback)
      if client_address in self.clients.keys():
        self.clients[client_address].lastPacket = time.time()
    if packet.messageType == MQTTSN.MessageTypes.DISCONNECT:
      terminate = True
    return terminate

  def connect(self, sock, packet, callback):
    if packet.ProtocolVersion != 2:
      logger.error("[MQTT-3.1.2-2] Wrong protocol version %d", packet.ProtocolVersion)
      resp = MQTTSN.Connacks()
      resp.ReturnCode = 1
      respond(callback, resp)
      logger.info("[MQTT-3.2.2-5] must close connection after non-zero connack")
      self.disconnect(sock, None)
      logger.info("[MQTT-3.1.4-5] When rejecting connect, no more data must be processed")
      return
    if sock in self.clients.keys():    # is socket is already connected?
      self.disconnect(sock, None)
      logger.info("[MQTT-3.1.4-5] When rejecting connect, no more data must be processed")
      raise MQTTSN.MQTTSNException("[MQTT-3.1.0-2] Second connect packet")
    if len(packet.ClientId) == 0:
      if self.zero_length_clientids == False or packet.CleanSession == False:
        if self.zero_length_clientids:
          logger.info("[MQTT-3.1.3-8] Reject 0-length clientid with cleansession false")
        logger.info("[MQTT-3.1.3-9] if clientid is rejected, must send connack 2 and close connection")
        resp = MQTTSN.Connacks()
        resp.returnCode = 2
        respond(callback, resp)
        logger.info("[MQTT-3.2.2-5] must close connection after non-zero connack")
        self.disconnect(sock, None)
        logger.info("[MQTT-3.1.4-5] When rejecting connect, no more data must be processed")
        return
      else:
        logger.info("[MQTT-3.1.3-7] 0-length clientid must have cleansession true")
        packet.ClientId = uuid.uuid4() # give the client a unique clientid
        logger.info("[MQTT-3.1.3-6] 0-length clientid must be assigned a unique id %s", packet.ClientId)
    logger.info("[MQTT-3.1.3-5] Clientids of 1 to 23 chars and ascii alphanumeric must be allowed")
    if packet.ClientId in [client.id for client in self.clients.values()]: # is this client already connected on a different socket?
      for s in self.clients.keys():
        if self.clients[s].id == packet.ClientId:
          logger.info("[MQTT-3.1.4-2] Disconnecting old client %s", packet.ClientId)
          self.disconnect(s, None)
          break
    me = None
    if not packet.ConnectFlags.CleanStart:
      me = self.broker.getClient(packet.ClientId) # find existing state, if there is any
      if me:
        logger.info("[MQTT-3.1.3-2] clientid used to retrieve client state")
    resp = MQTTSN.Connacks()
    if me == None:
      me = MQTTSNClients(packet.ClientId, packet.ConnectFlags.CleanStart, packet.SessionExpiryInterval, callback, self)
    else:
      me.socket = sock # set existing client state to new socket
      me.cleanstart = packet.ConnectFlags.CleanStart
      me.keepalive = packet.KeepAlive
    logger.info("[MQTT-4.1.0-1] server must store data for at least as long as the network connection lasts")
    self.clients[sock] = me
    #me.will = (packet.WillTopic, packet.WillQoS, packet.WillMessage, packet.WillRETAIN) if packet.WillFlag else None
    self.broker.connect(me)
    logger.info("[MQTT-3.2.0-1] the first response to a client must be a connack")
    resp.ReasonCode = 0
    resp.PacketId = packet.PacketId
    respond(callback, resp)
    me.resend()

  def disconnect(self, sock, packet, terminate=False):
    logger.info("[MQTT-3.14.4-2] Client must not send any more packets after disconnect")
    if sock in self.clients.keys():
      if terminate:
        self.broker.terminate(self.clients[sock].id)
      else:
        self.broker.disconnect(self.clients[sock].id)
      del self.clients[sock]

  def disconnectAll(self, sock):
    for sock in self.clients.keys():
      self.disconnect(sock, None)

  def subscribe(self, sock, packet, callback):
    topic = packet.TopicFilter
    # todo - aliases
    self.broker.subscribe(self.clients[sock].id, topic, packet.SubscribeFlags.QoS)
    reason = MQTTSN.ReasonCodes(MQTTSN.MessageTypes.SUBACK, identifier=packet.SubscribeFlags.QoS)
    resp = MQTTSN.Subacks()
    logger.info("[MQTT-2.3.1-7][MQTT-3.8.4-2] Suback has same message id as subscribe")
    logger.info("[MQTT-3.8.4-1] Must respond with suback")
    resp.PacketId = packet.PacketId
    logger.info("[MQTT-3.8.4-5] return code must be returned for each topic in subscribe")
    logger.info("[MQTT-3.9.3-1] the order of return codes must match order of topics in subscribe")
    resp.ReasonCode = reason.value
    respond(callback, resp)

  def unsubscribe(self, sock, packet):
    self.broker.unsubscribe(self.clients[sock].id, packet.data)
    resp = MQTTSN.Unsubacks()
    logger.info("[MQTT-2.3.1-7] Unsuback has same message id as unsubscribe")
    logger.info("[MQTT-3.10.4-4] Unsuback must be sent - same message id as unsubscribe")
    me = self.clients[sock]
    if len(me.outbound) > 0:
      logger.info("[MQTT-3.10.4-3] sending unsuback has no effect on outward inflight messages")
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def pubwos(self, sock, packet, callback):
      if packet.Flags.TopicIdType == 2:
        topic = MQTTSN.writeInt16(packet.TopicId).decode()
        logger.debug("topic %s", topic)
      self.broker.publish("QoS -1", # no clientid for QoS -1
             topic, packet.Data, 0, packet.Flags.RETAIN)

  def publish(self, sock, packet, callback):
    if packet.Flags.QoS == 0:
      if packet.Flags.TopicType == packet.Flags.TOPIC_TYPE_NAME:
        topicName = packet.TopicName.decode(encoding="utf-8")
      else:
        topic = MQTTSN.writeInt16(packet.TopicId).decode()
        logger.debug("topic %s", topic)
      self.broker.publish(self.clients[sock].id,
             topicName, packet.Data, packet.Flags.QoS, packet.Flags.RETAIN)
    elif packet.Flags.QoS == 1:
      if packet.Flags.DUP:
        logger.info("[MQTT-3.3.1-3] Incoming publish DUP 1 ==> outgoing publish with DUP 0")
        logger.info("[MQTT-4.3.2-2] server must store message in accordance with QoS 1")
      topic = MQTTSN.writeInt16(packet.TopicId).decode()
      self.broker.publish(self.clients[sock].id,
            topic, packet.Data, packet.Flags.QoS, packet.Flags.RETAIN)
      resp = MQTTSN.Pubacks()
      logger.info("[MQTT-2.3.1-6] puback messge id same as publish")
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, callback, resp)
    elif packet.fh.QoS == 2:
      myclient = self.clients[sock]
      if self.publish_on_pubrel:
        if packet.messageIdentifier in myclient.inbound.keys():
          if packet.fh.DUP == 0:
            logger.error("[MQTT-3.3.1-2] duplicate QoS 2 message id %d found with DUP 0", packet.messageIdentifier)
          else:
            logger.info("[MQTT-3.3.1-2] DUP flag is 1 on redelivery")
        else:
          myclient.inbound[packet.messageIdentifier] = packet
      else:
        if packet.messageIdentifier in myclient.inbound:
          if packet.fh.DUP == 0:
            logger.error("[MQTT-3.3.1-2] duplicate QoS 2 message id %d found with DUP 0", packet.messageIdentifier)
          else:
            logger.info("[MQTT-3.3.1-2] DUP flag is 1 on redelivery")
        else:
          myclient.inbound.append(packet.messageIdentifier)
          logger.info("[MQTT-4.3.3-2] server must store message in accordance with QoS 2")
          self.broker.publish(myclient, packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTSN.Pubrecs()
      logger.info("[MQTT-2.3.1-6] pubrec messge id same as publish")
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, callback, resp)

  def pubrel(self, sock, packet):
    myclient = self.clients[sock]
    pub = myclient.pubrel(packet.messageIdentifier)
    if pub:
      if self.publish_on_pubrel:
        self.broker.publish(myclient.id, pub.topicName, pub.data, pub.fh.QoS, pub.fh.RETAIN)
        del myclient.inbound[packet.messageIdentifier]
      else:
        myclient.inbound.remove(packet.messageIdentifier)
    resp = MQTTSN.Pubcomps()
    logger.info("[MQTT-2.3.1-6] pubcomp messge id same as publish")
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def pingreq(self, sock, packet):
    resp = MQTTSN.Pingresps()
    logger.info("[MQTT-3.12.4-1] sending pingresp in response to pingreq")
    respond(sock, resp)

  def puback(self, sock, packet):
    "confirmed reception of qos 1"
    self.clients[sock].puback(packet.messageIdentifier)

  def pubrec(self, sock, packet):
    "confirmed reception of qos 2"
    myclient = self.clients[sock]
    if myclient.pubrec(packet.messageIdentifier):
      logger.info("[MQTT-3.5.4-1] must reply with pubrel in response to pubrec")
      resp = MQTTSN.Pubrels()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)

  def pubcomp(self, sock, packet):
    "confirmed reception of qos 2"
    self.clients[sock].pubcomp(packet.messageIdentifier)

  def keepalive(self, sock):
    if sock in self.clients.keys():
      client = self.clients[sock]
      if client.keepalive > 0 and time.time() - client.lastPacket > client.keepalive * 1.5:
        # keep alive timeout
        logger.info("[MQTT-3.1.2-22] keepalive timeout for client %s", client.id)
        self.disconnect(sock, None, terminate=True)
