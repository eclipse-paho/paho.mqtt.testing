"""
*******************************************************************
  Copyright (c) 2013, 2023 IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
     Ian Craggs - add sessionPresent connack flag
*******************************************************************
"""

import traceback, random, sys, string, copy, threading, logging, socket, time, uuid, json

from mqtt.formats import MQTTV5

from .Brokers import Brokers

logger = logging.getLogger('MQTT broker')

mybroker = None

def respond(sock, packet, maximumPacketSize=500):
  # deal with expiry
  if packet.fh.PacketType == MQTTV5.PacketTypes.PUBLISH:
    if hasattr(packet.properties, "MessageExpiryInterval"):
      timespent = int(time.monotonic() - packet.receivedTime)
      if timespent >= packet.properties.MessageExpiryInterval:
        logger.info("[MQTT-3.3.2-5] Delete expired message")
        return
      else:
        try:
          logger.info("[MQTT-3.3.2-6] Message Expiry Interval set to received value minus time waiting in the server")
          packet.properties.MessageExpiryInterval -= timespent
        except:
          traceback.print_exc()
  packed = packet.pack()
  # deal with packet size
  packlen = len(packed)
  if packlen > maximumPacketSize:
    logger.error("[MQTT5-3.1.2-24] Packet too big to send to client packet size %d max packet size %d" % (packlen, maximumPacketSize))
    logger.info("[MQTT5-3.1.2-25] message must be discarded and behave as if it had been sent")
    return
  if hasattr(sock, "fileno"):
    packet_string = str(packet)
    if len(packet_string) > 256:
      packet_string = packet_string[:255] + '...' + (' payload length:' + str(len(packet.data)) if hasattr(packet, "data") else "")
    logger.debug("out: (%d) %s", sock.fileno(), packet_string)
  if mybroker.mscfile != None:
    mybroker.mscfile.write("broker=>client%d[label=%s];\n" % (sock.fileno(), str(packet).split("(")[0]))
  if hasattr(sock, "handlePacket"):
    sock.handlePacket(packet)
  else:
    if mybroker.options["visual"]:
      try:
        data = {"direction" : "StoC", "socket" : sock.fileno(), 
            "clientid":  mybroker.clients[sock].id if sock in mybroker.clients.keys() else "", 
            "packet" : packet.json() }
        # for any byte arrays, use base64 in json
        databytes = bytes(json.dumps(data), 'utf-8')
        mybroker.broker.publish('$internal', '$SYS/clients-packets', databytes, 
                0, 0, None, time.monotonic())
      except:
        traceback.print_exc()
    try:
      bytes_sent = sock.send(packed) # Could get socket error on send
      if sock.websockets:
        assert bytes_sent >= len(packed)
      else:
        assert bytes_sent == len(packed)
    except:
      traceback.print_exc()

class MQTTClients:

  def __init__(self, anId, cleanStart, sessionExpiryInterval, willDelayInterval, keepalive, socket, broker):
    self.id = anId # required
    self.cleanStart = cleanStart
    self.sessionExpiryInterval = sessionExpiryInterval
    self.sessionEndedTime = 0
    self.maximumPacketSize = MQTTV5.MAX_PACKET_SIZE
    self.receiveMaximum = MQTTV5.MAX_PACKETID
    self.connected = False
    self.will = None
    self.willDelayInterval = willDelayInterval
    self.delayedWillTime = None
    self.socket = socket
    self.broker = broker
    # outbound messages
    self.msgid = 1 # outbound message ids
    self.queued = [] # queued message objects
    self.outbound = [] # message objects - for ordering
    self.outmsgs = {} # msgids to message objects
    # inbound messages
    if broker.options["publish_on_pubrel"]:
      self.inbound = {} # stored inbound QoS 2 publications
    else:
      self.inbound = []
    # Keep alive
    self.keepalive = keepalive
    self.lastPacket = None # time of last packet
    # Topic aliases
    self.clearTopicAliases()

  def clearTopicAliases(self):
    self.topicAliasToNames = {} # int -> string, incoming
    self.topicAliasMaximum = 0 # for server topic aliases
    self.outgoingTopicNamesToAliases = []

  def resendPub(self, pub):
    logger.debug("resending %s", str(pub))
    logger.info("[MQTT-4.4.0-2] dup flag must be set on in re-publish")
    if pub.fh.QoS == 0:
      respond(self.socket, pub, self.maximumPacketSize)
    elif pub.fh.QoS == 1:
      logger.info("[MQTT-2.1.2-3] Dup when resending QoS 1 publish id %d", pub.packetIdentifier)
      logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
      logger.info("[MQTT-4.3.2-1] Resending QoS 1 with DUP flag")
      respond(self.socket, pub, self.maximumPacketSize)
      pub.fh.DUP = 1
    elif pub.fh.QoS == 2:
      if pub.qos2state == "PUBREC":
        logger.info("[MQTT-2.1.2-3] Dup when resending QoS 2 publish id %d", pub.packetIdentifier)
        logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
        logger.info("[MQTT-4.3.3-1] Resending QoS 2 with DUP flag")
        respond(self.socket, pub, self.maximumPacketSize)
        pub.fh.DUP = 1
      else:
        resp = MQTTV5.Pubrels()
        logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
        resp.packetIdentifier = pub.packetIdentifier
        respond(self.socket, resp, self.maximumPacketSize)

  def resend(self):
    logger.debug("resending unfinished publications %s", str(self.outbound))
    if len(self.outbound) > 0:
      logger.info("[MQTT-4.4.0-1] resending inflight QoS 1 and 2 messages")
    for pub in self.outbound:
      self.resendPub(pub)
    self.sendQueued()

  def sendFirst(self, pub):
    if pub.fh.QoS in [1, 2]:
      pub.packetIdentifier = self.msgid
      logger.debug("client id: %s msgid: %d", self.id, self.msgid)
      if self.msgid == MQTTV5.MAX_PACKETID:
        self.msgid = 1
      else:
        self.msgid += 1
      self.outbound.append(pub)
      self.outmsgs[pub.packetIdentifier] = pub
      logger.info("[MQTT-4.6.0-6] publish packets must be sent in order of receipt from any given client")
    respond(self.socket, pub, self.maximumPacketSize)
    if pub.fh.QoS > 0:
      pub.fh.DUP = 1

  def sendQueued(self):
    while len(self.queued) > 0 and len(self.outbound) < self.receiveMaximum:
      self.outbound.append(self.queued.pop(0))
      self.sendFirst(self.outbound[-1])

  def publishArrived(self, topic, msg, qos, properties, receivedTime, retained=False):
    pub = MQTTV5.Publishes()
    if properties:
      if hasattr(properties, 'TopicAlias'):
        del properties.TopicAlias
      pub.properties = properties
    logger.info("[MQTT-3.2.3-3] topic name must match the subscription's topic filter")
    # Topic alias
    if self.topicAliasMaximum == 0:
      logger.info("[MQTT5-3.1.2-27] if topic alias is 0, no topic aliases must be sent") 
    if len(self.outgoingTopicNamesToAliases) < self.topicAliasMaximum and not topic in self.outgoingTopicNamesToAliases:
      logger.info("[MQTT5-3.1.2-26] Server must not send topic alias > max") 
      self.outgoingTopicNamesToAliases.append(topic)       # add alias
      pub.topicName = topic # include topic name as well as alias first time
    if topic in self.outgoingTopicNamesToAliases:
      pub.properties.TopicAlias = self.outgoingTopicNamesToAliases.index(topic) + 1 # Topic aliases start at 1
    else:
      pub.topicName = topic
    pub.data = msg
    pub.fh.QoS = qos
    pub.fh.RETAIN = retained
    pub.receivedTime = receivedTime
    if retained:
      logger.info("[MQTT-2.1.2-7] Last retained message on matching topics sent on subscribe")
    if pub.fh.RETAIN:
      logger.info("[MQTT-2.1.2-9] Set retained flag on retained messages")
    if qos == 2:
      pub.qos2state = "PUBREC"
    if len(self.outbound) >= self.receiveMaximum or not self.connected:
      if qos > 0 or not self.broker.options["dropQoS0"]:
        self.queued.append(pub) # this should never be infinite in reality
      if qos > 0 and not self.connected:
        logger.info("[MQTT-3.1.2-5] storing of QoS 1 and 2 messages for disconnected client %s", self.id)
    else:
      self.sendFirst(pub)

  def puback(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 1:
        self.outbound.remove(pub)
        del self.outmsgs[msgid]
        self.sendQueued()
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
          self.sendQueued()
        else:
          logger.error("Pubcomp received for msgid %d, but message in wrong state", msgid)
      else:
        logger.error("Pubcomp received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:
      logger.error("Pubcomp received for msgid %d, but no message found", msgid)

  def pubrel(self, msgid):
    rc = None
    if self.broker.options["publish_on_pubrel"]:
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

class cleanupThreads(threading.Thread):
  """
  Most of the actions of the broker can be taken when provoked by an external stimulus,
  which is generally a client taking some action.  A few actions need to be assessed
  asynchronously, such as the will delay.
  """

  def __init__(self, broker, lock=None):
    threading.Thread.__init__(self)
    self.broker = broker
    self.lock = lock
    self.running = False
    self.start()

  def run(self):
    self.running = True
    while self.running:
      time.sleep(1)
      # will delay
      if self.lock:
        self.lock.acquire()
      for clientid in self.broker.willMessageClients.copy():
        client = self.broker.getClient(clientid)
        if client and time.monotonic() >= client.delayedWillTime:
          self.broker.sendWillMessage(clientid)
      if self.lock:
        self.lock.release()

  def stop(self):
    self.running = False


class MQTTBrokers:

  def __init__(self, options={}, lock=None, sharedData={}):

    global mybroker
    mybroker = self
    self.options = options

    self.broker = Brokers(self.options["overlapping_single"], self.options["topicAliasMaximum"], sharedData=sharedData)
    self.clients = {}   # socket -> clients
    if lock:
      logger.info("Using shared lock %d", id(lock))
      self.lock = lock
    else:
      self.lock = threading.RLock()

    self.cleanupThread = cleanupThreads(self.broker)

    logger.info("MQTT 5.0 Paho Test Broker")
    logger.info("Options %s", self.options)

    self.mscfile = None
    if "mscfile" in self.options.keys():
      self.mscfile = open(self.options["mscfile"], "w")
      self.mscfile.write("msc {\n broker;\n")

  def shutdown(self):
    self.disconnectAll(reasonCode="Server shutting down")
    self.cleanupThread.stop()

  def setBroker3(self, broker3):
    self.broker.setBroker3(broker3.broker)

  def reinitialize(self):
    logger.info("Reinitializing broker")
    self.clients = {}
    self.broker.reinitialize()

  def handleRequest(self, sock):
    "this is going to be called from multiple threads, so synchronize"
    self.lock.acquire()
    raw_packet = None
    try:
      try:
        raw_packet = MQTTV5.getPacket(sock)
      except:
        pass # handled by raw_packet == None
      if raw_packet == None:
        logger.info("[MQTT-4.8.0-1] 'transient error' reading packet, closing connection")
        # will message
        if sock in self.clients.keys():
          self.disconnect(sock, None, sendWillMessage=True)
        terminate = True
      else:
        try:
          packet = MQTTV5.unpackPacket(raw_packet, self.options["maximumPacketSize"])
          if self.options["visual"]:
            clientid = self.clients[sock].id if sock in self.clients.keys() else ""
            if clientid == "" and hasattr(packet, "ClientIdentifier"):
              clientid = packet.ClientIdentifier
            try:
              data = {"direction" : "CtoS", "socket" : sock.fileno(), 
                    "clientid":  clientid, "packet" : packet.json() }
              databytes = bytes(json.dumps(data), 'utf-8')
              self.broker.publish('$internal', '$SYS/clients-packets', databytes,
                   0, 0, None, time.monotonic())
            except:
              traceback.print_exc()
          if packet:
            terminate = self.handlePacket(packet, sock)
          else:
            self.disconnect(sock, reasonCode="Malformed packet", sendWillMessage=True)
            terminate = True
        except MQTTV5.MalformedPacket as error:
          traceback.print_exc()
          disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
          disconnect_properties.ReasonString = error.args[0]
          self.disconnect(sock, reasonCode="Malformed packet", sendWillMessage=True)
          terminate = True
        except MQTTV5.ProtocolError as error:
          disconnect_properties = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
          disconnect_properties.ReasonString = error.args[0]
          self.disconnect(sock, reasonCode=error.args[0], properties=disconnect_properties,
                          sendWillMessage=True)
          terminate = True
    finally:
      self.lock.release()
    return terminate

  def handlePacket(self, packet, sock):
    terminate = False
    if hasattr(sock, "fileno"):
      packet_string = str(packet)
      if len(packet_string) > 256:
        packet_string = packet_string[0:256] + '...' + (' payload length:' + str(len(packet.data)) if hasattr(packet, "data") else "")
      logger.debug("in: (%d) %s", sock.fileno(), packet_string)
    if self.mscfile != None:
      self.mscfile.write("client%d=>broker[label=%s];\n" % (sock.fileno(), str(packet).split("(")[0]))
    if sock not in self.clients.keys() and packet.fh.PacketType != MQTTV5.PacketTypes.CONNECT:
      self.disconnect(sock, packet)
      raise MQTTV5.MQTTException("[MQTT5-3.1.0-1-error] Connect was not first packet on socket")
    else:
      if packet.fh.PacketType == MQTTV5.PacketTypes.CONNECT:
        logger.info("[MQTT5-3.1.0-1] Connect must be first packet on socket")
      getattr(self, MQTTV5.Packets.Names[packet.fh.PacketType].lower())(sock, packet)
      if sock in self.clients.keys():
        self.clients[sock].lastPacket = time.monotonic()
    if packet.fh.PacketType == MQTTV5.PacketTypes.DISCONNECT:
      terminate = True
    return terminate

  def connect(self, sock, packet):
    resp = MQTTV5.Connacks()
    if packet.ProtocolName != "MQTT":
      self.disconnect(sock, None)
      raise MQTTV5.MQTTException("[MQTT5-3.1.2-1-error] Wrong protocol name %s" % packet.ProtocolName)
    logger.info("[MQTT5-3.1.2-1] Protocol name must be MQTT")
    if packet.ProtocolVersion != 5:
      logger.error("[MQTT5-3.1.2-2-error] Wrong protocol version %d", packet.ProtocolVersion)
      resp.reasonCode.set("Unsupported protocol version")
      respond(sock, resp)
      logger.info("[MQTT5-3.2.2-6] must set session present to 0 with non-zero connack")
      logger.info("[MQTT5-3.2.2-7] must close connection after connack reason >= 0x80")
      self.disconnect(sock, None)
      logger.info("[MQTT5-3.1.4-6] When rejecting connect, no more data must be processed")
      return
    logger.info("[MQTT5-3.1.2-2] Protocol version must be 5")
    if sock in self.clients.keys():    # is socket is already connected?
      self.disconnect(sock, None)
      logger.info("[MQTT5-3.1.4-6] When rejecting connect, no more data must be processed")
      raise MQTTV5.MQTTException("[MQTT5-3.1.0-2] Second connect packet")
    if len(packet.ClientIdentifier) == 0:
      packet.ClientIdentifier = str(uuid.uuid4()) # give the client a unique clientid
      logger.info("[MQTT5-3.1.3-6] 0-length clientid must be assigned a unique id %s", packet.ClientIdentifier)
      resp.properties.AssignedClientIdentifier = packet.ClientIdentifier # returns the assigned client id
      logger.info("[MQTT5-3.1.3-7] must return the assigned client id")
    else:
      logger.info("[MQTT5-3.1.3-5] Clientids of 1 to 23 chars and ascii alphanumeric must be allowed")
      if False: # reject clientid test
        logger.info("[MQTT5-3.1.3-8] server rejects clientid - may return connack")
    if packet.ClientIdentifier in [client.id for client in self.clients.values()]: # is this client already connected on a different socket?
      for cursock in self.clients.keys():
        if self.clients[cursock].id == packet.ClientIdentifier:
          logger.info("[MQTT5-3.1.4-3] Disconnecting old client %s", packet.ClientIdentifier)
          self.disconnect(cursock, reasonCode="Session taken over", sendWillMessage=True)
          break
    me = None
    clean = False
    if packet.CleanStart:
      logger.info("[MQTT5-3.1.2-4] discard existing session when cleanstart set to 1")
      logger.info("[MQTT5-3.1.4-4] server must perform clean start processing")
      clean = True
      logger.info("[MQTT5-3.2.2-2] session present must be set to 0 if cleanstart is 1")
    else:
      me = self.broker.getClient(packet.ClientIdentifier) # find existing state, if there is any
      if not me:
        logger.info("[MQTT5-3.1.2-6] no existing session and cleanstart set to 0")
      # has that state expired?
      if me and me.sessionExpiryInterval >= 0 and time.monotonic() - me.sessionEndedTime > me.sessionExpiryInterval:
        me = None
        clean = True
      else:
        logger.info("[MQTT5-3.1.2-5] resume an existing session when cleanstart set to 0")
      if me:
        logger.info("[MQTT5-3.1.3-2] clientid used to retrieve client state")
        logger.info("[MQTT5-3.2.2-3] session present must be set to 1")
    resp.sessionPresent = True if me else False
    # Connack topic alias maximum for incoming client created topic aliases
    if self.options["topicAliasMaximum"] > 0:
      resp.properties.TopicAliasMaximum = self.options["topicAliasMaximum"]
    if self.options["maximumPacketSize"] < MQTTV5.MAX_PACKET_SIZE:
      resp.properties.MaximumPacketSize = self.options["maximumPacketSize"]
    if self.options["receiveMaximum"] < MQTTV5.MAX_PACKETID:
      resp.properties.ReceiveMaximum = self.options["receiveMaximum"]
    keepalive = packet.KeepAliveTimer
    if packet.KeepAliveTimer > 0 and self.options["serverKeepAlive"] < packet.KeepAliveTimer:
      keepalive = self.options["serverKeepAlive"]
      resp.properties.ServerKeepAlive = keepalive
      logger.info("[MQTT5-3.1.2-21] client must use server keep alive if returned on connack")
    # Session expiry
    if hasattr(packet.properties, "SessionExpiryInterval"):
      sessionExpiryInterval = packet.properties.SessionExpiryInterval
    else:
      sessionExpiryInterval = 0 # immediate expiry - change to spec
    # will delay
    willDelayInterval = 0
    if hasattr(packet.WillProperties, "WillDelayInterval"):
      willDelayInterval = packet.WillProperties.WillDelayInterval
      delattr(packet.WillProperties, "WillDelayInterval") # must not be sent with will message
    if willDelayInterval > sessionExpiryInterval:
      willDelayInterval = sessionExpiryInterval
    if me == None:
      me = MQTTClients(packet.ClientIdentifier, packet.CleanStart, sessionExpiryInterval, willDelayInterval, keepalive, sock, self)
    else:
      me.socket = sock # set existing client state to new socket
      me.cleanStart = packet.CleanStart
      me.keepalive = keepalive
      me.sessionExpiryInterval = sessionExpiryInterval
      me.willDelayInterval = willDelayInterval
    if me.delayedWillTime:
      me.delayedWillTime = None
      logger.info("[MQTT5-3.1.3-9] don't send delayed will if client connects in time")
    if me.id in self.broker.willMessageClients:
      self.broker.willMessageClients.remove(me.id)
    # the topic alias maximum in the connect properties sets the maximum outgoing topic aliases for a client
    me.topicAliasMaximum = packet.properties.TopicAliasMaximum if hasattr(packet.properties, "TopicAliasMaximum") else 0
    me.maximumPacketSize = packet.properties.MaximumPacketSize if hasattr(packet.properties, "MaximumPacketSize") else MQTTV5.MAX_PACKET_SIZE
    assert me.maximumPacketSize <= MQTTV5.MAX_PACKET_SIZE # is this the correct value?
    me.receiveMaximum = packet.properties.ReceiveMaximum if hasattr(packet.properties, "ReceiveMaximum") else MQTTV5.MAX_PACKETID
    assert me.receiveMaximum <= MQTTV5.MAX_PACKETID
    logger.info("[MQTT-4.1.0-1] server must store data for at least as long as the network connection lasts")
    self.clients[sock] = me
    me.will = (packet.WillTopic, packet.WillQoS, packet.WillMessage, packet.WillRETAIN, packet.WillProperties) if packet.WillFlag else None
    if me.will != None:
      logger.info("[MQTT5-3.1.2-7] the will message must be stored if the WillFlag is set")
    self.broker.connect(me, clean)
    logger.info("[MQTT5-3.2.0-1] the first response to a client must be a connack")
    logger.info("[MQTT5-3.1.4-5] the server must acknowledge the connect with a connack success")
    resp.reasonCode.set("Success")
    respond(sock, resp)
    me.resend()

  def disconnect(self, sock, packet=None, sendWillMessage=False, reasonCode=None, properties=None):
    logger.info("[MQTT-3.14.4-2] Client must not send any more packets after disconnect")
    me = self.clients[sock]
    me.clearTopicAliases()
    # Session expiry
    if packet and hasattr(packet.properties, "SessionExpiryInterval"):
      if me.sessionExpiryInterval == 0 and packet.properties.SessionExpiryInterval > 0:
        raise MQTTV5.ProtocolError("[MQTT-3.1.0-2] Can't reset SessionExpiryInterval from 0")
      else:
        me.sessionExpiryInterval = packet.properties.SessionExpiryInterval
    if reasonCode:
      resp = MQTTV5.Disconnects(reasonCode=reasonCode) # reasonCode is text
      if properties:
        resp.properties = properties
      respond(sock, resp)
    if sock in self.clients.keys():
      self.broker.disconnect(me.id, willMessage=sendWillMessage,
          sessionExpiryInterval=me.sessionExpiryInterval)
      del self.clients[sock]
    try:
      sock.shutdown(socket.SHUT_RDWR) # must call shutdown to close socket immediately
    except:
      pass # doesn't matter if the socket has been closed at the other end already
    try:
      sock.close()
    except:
      pass # doesn't matter if the socket has been closed at the other end already

  def disconnectAll(self, reasonCode=None):
    for sock in list(self.clients.keys())[:]:
      self.disconnect(sock, None, reasonCode=reasonCode)

  def subscribe(self, sock, packet):
    topics = []
    optionss = []
    respqoss = []
    for topicFilter, subsoption in packet.data:
      QoS = subsoption.QoS
      if topicFilter == "test/nosubscribe":
        respqoss.append(MQTTV5.ReasonCodes(MQTTV5.PacketTypes.SUBACK, "Unspecified error"))
      else:
        if topicFilter == "test/QoS 1 only":
          respqoss.append(MQTTV5.ReasonCodes(MQTTV5.PacketTypes.SUBACK, identifier=min(1, QoS)))
        elif topicFilter == "test/QoS 0 only":
          respqoss.append(MQTTV5.ReasonCodes(MQTTV5.PacketTypes.SUBACK, identifier=min(0, QoS)))
        else:
          respqoss.append(MQTTV5.ReasonCodes(MQTTV5.PacketTypes.SUBACK, identifier=QoS))
        topics.append(topicFilter)
        subsoption.QoS = respqoss[-1].value # might have been downgraded
        optionss.append((subsoption, packet.properties))
    if len(topics) > 0:
      self.broker.subscribe(self.clients[sock].id, topics, optionss)
    resp = MQTTV5.Subacks()
    logger.info("[MQTT5-2.2.1-6-suback] Suback has same message id as subscribe")
    logger.info("[MQTT-3.8.4-1] Must respond with suback")
    resp.packetIdentifier = packet.packetIdentifier
    logger.info("[MQTT-3.8.4-5] return code must be returned for each topic in subscribe")
    logger.info("[MQTT-3.9.3-1] the order of return codes must match order of topics in subscribe")
    resp.reasonCodes = respqoss
    # propagating user property is broker specific behaviour, to aid testing
    if hasattr(packet.properties, "UserProperty"):
      resp.properties.UserProperty = packet.properties.UserProperty
    respond(sock, resp)

  def unsubscribe(self, sock, packet):
    reasonCodes = self.broker.unsubscribe(self.clients[sock].id, packet.topicFilters)
    resp = MQTTV5.Unsubacks()
    logger.info("[MQTT5-2.2.1-6-unsuback] Unsuback has same message id as unsubscribe")
    logger.info("[MQTT-3.10.4-4] Unsuback must be sent - same message id as unsubscribe")
    me = self.clients[sock]
    if len(me.outbound) > 0:
      logger.info("[MQTT-3.10.4-3] sending unsuback has no effect on outward inflight messages")
    # propagating user property is broker specific behaviour, to aid testing
    if hasattr(packet.properties, "UserProperty"):
      resp.properties.UserProperty = packet.properties.UserProperty
    resp.packetIdentifier = packet.packetIdentifier
    resp.reasonCodes = reasonCodes
    respond(sock, resp)

  def publish(self, sock, packet):
    packet.receivedTime = time.monotonic()
    if packet.topicName.find("+") != -1 or packet.topicName.find("#") != -1:
      raise MQTTV5.AcksProtocolError("Topic name invalid %s" % packet.topicName)
    # Test Topic to disconnect the client
    if packet.topicName.startswith("cmd/"):
        self.handleBehaviourPublish(sock, packet.topicName, packet.data)
    else:
        if packet.fh.QoS > 0 and len(self.clients[sock].inbound) >= self.options["receiveMaximum"]:
          self.disconnect(sock, reasonCode="Receive maximum exceeded", sendWillMessage=True)
          return
        if hasattr(packet.properties, "UserProperty") and len(packet.properties.UserProperty) > 1:
          logger.info("[MQTT-3.1.3-10] Must maintain order of user properties")
        if packet.fh.QoS == 0:
          self.broker.publish(self.clients[sock].id, packet.topicName,
                 packet.data, packet.fh.QoS, packet.fh.RETAIN, packet.properties,
                 packet.receivedTime)
        elif packet.fh.QoS == 1:
          if packet.fh.DUP:
            logger.info("[MQTT-3.3.1-3] Incoming publish DUP 1 ==> outgoing publish with DUP 0")
            logger.info("[MQTT-4.3.2-2] server must store message in accordance with QoS 1")
          subscribers = self.broker.publish(self.clients[sock].id, packet.topicName,
                packet.data, packet.fh.QoS, packet.fh.RETAIN, packet.properties,
                packet.receivedTime)
          resp = MQTTV5.Pubacks()
          logger.info("[MQTT5-2.2.1-5-puback] puback message id same as publish")
          resp.packetIdentifier = packet.packetIdentifier
          if subscribers == None:
            resp.reasonCode.set("No matching subscribers")
          if packet.topicName == "test_qos_1_2_errors": # specific error behaviour for testing
            resp.reasonCode.set("Not authorized")
            if hasattr(packet.properties, "UserProperty"):
              resp.properties.UserProperty = packet.properties.UserProperty
          respond(sock, resp)
        elif packet.fh.QoS == 2:
          myclient = self.clients[sock]
          subscribers = None
          if self.options["publish_on_pubrel"]:
            if packet.packetIdentifier in myclient.inbound.keys():
              if packet.fh.DUP == 0:
                logger.error("[MQTT-3.3.1-2] duplicate QoS 2 message id %d found with DUP 0", packet.packetIdentifier)
              else:
                logger.info("[MQTT-3.3.1-2] DUP flag is 1 on redelivery")
            else:
              myclient.inbound[packet.packetIdentifier] = packet
              if len(packet.topicName) == 0 and hasattr(packet.properties, "TopicAlias"):
                packet.topicName = self.broker.getAliasTopic(self.clients[sock].id, packet.properties.TopicAlias)
              subscribers = self.broker.se.getSubscriptions(packet.topicName)
          else:
            if packet.packetIdentifier in myclient.inbound:
              if packet.fh.DUP == 0:
                logger.error("[MQTT-3.3.1-2] duplicate QoS 2 message id %d found with DUP 0", packet.packetIdentifier)
              else:
                logger.info("[MQTT-3.3.1-2] DUP flag is 1 on redelivery")
            else:
              myclient.inbound.append(packet.packetIdentifier)
              logger.info("[MQTT-4.3.3-2] server must store message in accordance with QoS 2")
              if len(packet.topicName) == 0 and hasattr(packet.properties, "TopicAlias"):
                packet.topicName = self.broker.getAliasTopic(self.clients[sock].id, packet.properties.TopicAlias)
              subscribers = self.broker.publish(self.clients[sock].id, packet.topicName,
                   packet.data, packet.fh.QoS, packet.fh.RETAIN, packet.properties,
                   packet.receivedTime)
              if packet.topicName == "test_qos_1_2_errors_pubcomp":
                myclient.pubcomp_error = packet.packetIdentifier
          resp = MQTTV5.Pubrecs()
          logger.info("[MQTT5-2.2.1-5-pubrec] pubrec message id same as publish")
          resp.packetIdentifier = packet.packetIdentifier
          if subscribers == None:
            resp.reasonCode.set("No matching subscribers")
          if hasattr(packet, "topicName") and packet.topicName == "test_qos_1_2_errors":
            resp.reasonCode.set("Not authorized")
            if self.options["publish_on_pubrel"]:
              del myclient.inbound[packet.packetIdentifier]
            else:
              myclient.inbound.remove(packet.packetIdentifier)
            if hasattr(packet.properties, "UserProperty"):
              resp.properties.UserProperty = packet.properties.UserProperty
          respond(sock, resp)

  def handleBehaviourPublish(self,sock, topic, data):
    """Handle behaviour packet.

    Options:
    Topic: 'cmd/disconnectWithRC', Payload: A Disconnect Return code
            - Disconnects with the specified return code and sample properties.
    """
    logger.info("Command Mode: Topic: %s, Payload: %s" % (topic, int(data)))
    if topic == "cmd/disconnectWithRC":
        returnCode = int(data)
        props = MQTTV5.Properties(MQTTV5.PacketTypes.DISCONNECT)
        props.ReasonString = "This is a custom Reason String"
        props.ServerReference = "tcp://localhost:1883"
        props.UserPropertyList = [("key", "value")]
        self.disconnect(sock,
                        None,
                        sendWillMessage=False,
                        reasonCode=returnCode,
                        properties=props)


  def pubrel(self, sock, packet):
    myclient = self.clients[sock]
    pub = myclient.pubrel(packet.packetIdentifier)
    if pub:
      if self.options["publish_on_pubrel"]:
        self.broker.publish(myclient.id, pub.topicName, pub.data, pub.fh.QoS, pub.fh.RETAIN, pub.properties,
                pub.receivedTime)
        del myclient.inbound[packet.packetIdentifier]
      else:
        myclient.inbound.remove(packet.packetIdentifier)
    resp = MQTTV5.Pubcomps()
    logger.info("[MQTT5-2.2.1-5-pubcomp] pubcomp message id same as publish")
    resp.packetIdentifier = packet.packetIdentifier
    if not pub:
      resp.reasonCode.set("Packet identifier not found")
      resp.properties.ReasonString = "Looking for packet id "+str(packet.packetIdentifier)
    elif (hasattr(pub, "topicName") and pub.topicName == "test_qos_1_2_errors_pubcomp") or \
         (hasattr(myclient, "pubcomp_error") and myclient.pubcomp_error == packet.packetIdentifier):
      resp.reasonCode.set("Packet identifier not found")
      if hasattr(packet.properties, "UserProperty"):
        resp.properties.UserProperty = packet.properties.UserProperty
      if hasattr(myclient, "pubcomp_error"):
        del myclient.pubcomp_error
    respond(sock, resp)

  def pingreq(self, sock, packet):
    logger.info("[MQTT5-3.1.2-20] client must send ping in the absence of other packets")
    resp = MQTTV5.Pingresps()
    logger.info("[MQTT-3.12.4-1] sending pingresp in response to pingreq")
    respond(sock, resp)

  def puback(self, sock, packet):
    "confirmed reception of qos 1"
    self.clients[sock].puback(packet.packetIdentifier)

  def pubrec(self, sock, packet):
    "confirmed reception of qos 2"
    myclient = self.clients[sock]
    if myclient.pubrec(packet.packetIdentifier):
      logger.info("[MQTT-3.5.4-1] must reply with pubrel in response to pubrec")
      resp = MQTTV5.Pubrels()
      logger.info("[MQTT5-2.2.1-5-pubrel] pubrel message id same as publish")
      resp.packetIdentifier = packet.packetIdentifier
      respond(sock, resp)

  def pubcomp(self, sock, packet):
    "confirmed reception of qos 2"
    self.clients[sock].pubcomp(packet.packetIdentifier)

  def keepalive(self, sock):
    if sock in self.clients.keys():
      client = self.clients[sock]
      if client.keepalive > 0 and time.monotonic() - client.lastPacket > client.keepalive * 1.5:
        # keep alive timeout
        logger.info("[MQTT5-3.1.2-22] keepalive timeout for client %s", client.id)
        self.disconnect(sock, None, sendWillMessage=True)
