"""
*******************************************************************
  Copyright (c) 2013, 2017 IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

import sys, traceback, socket, logging, getopt, hashlib, base64
import threading, ssl, time

import mqtt.clients.V5
from mqtt.brokers.V5 import MQTTBrokers as MQTTV5Brokers
from mqtt.formats import MQTTV311, MQTTV5

server = None
logger = logging.getLogger('MQTT broker')


class Callbacks(mqtt.clients.V5.Callback):

  def __init__(self, broker):
    self.messages = []
    self.messagedicts = []
    self.publisheds = []
    self.subscribeds = []
    self.unsubscribeds = []
    self.disconnects = []
    self.broker = broker

  def __str__(self):
     return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
        str(self.subscribeds) + str(self.unsubscribeds) + str(self.disconnects)

  def clear(self):
    self.__init__()

  def disconnected(self, reasoncode, properties):
    logger.info("Bridge: disconnected %s %s", str(reasoncode), str(properties))
    self.disconnects.append({"reasonCode" : reasoncode, "properties" : properties})

  def connectionLost(self, cause):
    logger.info("Bridge: connectionLost %s" % str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid, properties=None):
    logger.info("Bridge: publishArrived %s %s %d %s %d %s", topicName, payload, qos, retained, msgid, str(properties))
    self.messages.append((topicName, payload, qos, retained, msgid, properties))
    self.messagedicts.append({"topicname" : topicName, "payload" : payload,
        "qos" : qos, "retained" : retained, "msgid" : msgid, "properties" : properties})

    # add to local broker
    self.broker.broker.publish("bridge", topicName, payload, qos, retained, properties, time.monotonic())
    return True

  def published(self, msgid):
    logger.info("Bridge: published %d", msgid)
    self.publisheds.append(msgid)

  def subscribed(self, msgid, data):
    logger.info("Bridge: subscribed %d", msgid)
    self.subscribeds.append((msgid, data))

  def unsubscribed(self, msgid):
    logger.info("Bridge: unsubscribed %d", msgid)
    self.unsubscribeds.append(msgid)


class Bridges:

  def __init__(self, name="local", host="localhost", port=1883, topic="+", direction="both", localprefix="", remoteprefix=""):
    self.name = name
    self.host = host
    self.port = int(port)
    self.topic = topic
    self.direction = direction
    self.localprefix = localprefix.strip('\"')
    self.remoteprefix = remoteprefix.strip('\"')
    self.client = mqtt.clients.V5.Client(name)
    self.callback = Callbacks(broker5)
    self.client.registerCallback(self.callback)
    self.local_connect()

  def local_connect(self):
    # connect locally with V5, so we get noLocal and retainAsPublished
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = self.name
    logger.debug("Bridge: local_connect:"+connect.ClientIdentifier)
    broker5.connect(self, connect)
    subscribe = MQTTV5.Subscribes()
    options = MQTTV5.SubscribeOptions()
    options.noLocal = options.retainAsPublished = True
    subscribe.data = [(self.topic, options)]
    broker5.subscribe(self, subscribe)

  def connect(self):
    logger.info("Bridge: connect: connecting to %s:%d"%(self.host, self.port))
    connected = False
    retry=2
    while not connected:
      try:
        self.client.connect(host=self.host, port=self.port, cleanstart=True)
        connected = True
      except OSError as e:
        #try again with a small amount of backoff, could ake this configurable
        logger.debug("Bridge: failed to connect to remote end due to an OS Error (%s), retrying...",str(e))
        time.sleep(retry)
        retry *= 2
      except MQTTV5.MQTTException as e:
        #I think we'll retry this one too, the other end probably wasn't ready
        logger.debug("Bridge: failed to connect to remote end due to an MQTT Error (%s), retrying...",str(e))
        time.sleep(retry)
        retry *= 2
        
        
    # subscribe if necessary
    options = MQTTV5.SubscribeOptions()
    options.noLocal = options.retainAsPublished = True
    if self.direction == "both" or self.direction == "in":
      self.client.subscribe([self.topic], [options])
    else:
      logger.info("Bridge: not subscribing to remote")

  def getPacket(self):
    # get packet from remote 
    pass

  def handlePacket(self, packet):
    # response from local broker
    logger.info("Bridge: from local broker %s", str(packet))
    if packet.fh.PacketType == MQTTV5.PacketTypes.PUBLISH:
      logger.info("Bridge: sending on %s", self.remoteprefix+packet.topicName)
      self.client.publish(self.remoteprefix+packet.topicName, packet.data, packet.fh.QoS) #retained=False, properties=None)

  def run(self):
    while True:
      logger.info("Bridge: initiating connect logic")
      self.connect()
      time.sleep(300)
    self.shutdown()

def setBroker5(aBroker5):
  global broker5
  broker5 = aBroker5

def create(name="local", host="localhost", port=1883, topic="+", direction="both", localprefix="", remoteprefix="", TLS=False, 
    cert_reqs=ssl.CERT_REQUIRED,
    ca_certs=None, certfile=None, keyfile=None):

  if host == "":
    host = "localhost"
  logger.info("Bridge: Starting TCP bridge '%s' for address '%s' port %d %s", name, host, int(port), "with TLS support" if TLS else "")
  bridge = Bridges(name, host, port, topic, direction, localprefix, remoteprefix)
  thread = threading.Thread(target=bridge.run)
  thread.start()
  return bridge





