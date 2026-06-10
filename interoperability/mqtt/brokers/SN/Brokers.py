"""
*******************************************************************
  Copyright (c) 2013, 2026 IBM Corp., Ian Craggs
 
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

import types, time, logging

from ..V311 import Topics
from ..V311.SubscriptionEngines import SubscriptionEngines

logger = logging.getLogger('MQTT-SN broker')
 
class Brokers:

  def __init__(self, overlapping_single=True, sharedData={}):
    self.sharedData = sharedData
    self.se = SubscriptionEngines(self.sharedData)
    self.__clients = {} # clientid -> client
    self.overlapping_single = overlapping_single
    self.__broker3 = None
    self.__broker5 = None

  def setBroker3(self, broker3):
    self.__broker3 = broker3

  def setBroker5(self, broker5):
    self.__broker5 = broker5

  def reinitialize(self):
    self.__clients = {}
    self.se.reinitialize()

  def getClients(self):
    return list(self.__clients.keys())

  def getClient(self, clientid):
    return self.__clients[clientid] if (clientid in self.__clients.keys()) else None

  def cleanSession(self, aClientid):
    "clear any outstanding subscriptions and publications"
    if len(self.se.getRetainedTopics("#")) > 0:
      logger.info("[MQTT-3.1.2-7] retained messages not cleaned up as part of session state for client %s", aClientid)
    self.se.clearSubscriptions(aClientid)

  def connect(self, aClient):
    aClient.connected = True
    try:
      aClient.timestamp = time.clock() # time.clock is deprecated
    except:
      aClient.timestamp = time.process_time()
    self.__clients[aClient.id] = aClient
    if aClient.cleansession:
      self.cleanSession(aClient.id)

  def terminate(self, aClientid):
    "Abrupt disconnect which also causes a will msg to be sent out"
    if aClientid in self.__clients.keys() and self.__clients[aClientid].connected:
      if self.__clients[aClientid].will != None:
        logger.info("[MQTT-3.1.2-8] sending will message for client %s", aClientid)
        willtopic, willQoS, willmsg, willRetain = self.__clients[aClientid].will
        if willRetain:
          logger.info("[MQTT-3.1.2-17] sending will message retained for client %s", aClientid)
        else:
          logger.info("[MQTT-3.1.2-16] sending will message non-retained for client %s", aClientid)
        self.publish(aClientid, willtopic, willmsg, willQoS, willRetain)
      self.disconnect(aClientid)

  def disconnect(self, aClientid):
    if aClientid in self.__clients.keys():
      self.__clients[aClientid].connected = False
      if self.__clients[aClientid].cleansession:
        logger.info("[MQTT-3.1.2-6] broker must discard the session data for client %s", aClientid)
        self.cleanSession(aClientid)
        del self.__clients[aClientid]
      else:
        logger.info("[MQTT-3.1.2-4] broker must store the session data for client %s", aClientid)
        self.__clients[aClientid].timestamp = time.clock()
        self.__clients[aClientid].connected = False 
        logger.info("[MQTT-3.1.2-10] will message is deleted after use or disconnect, for client %s", aClientid)
        logger.info("[MQTT-3.14.4-3] on receipt of disconnect, will message is deleted")
        self.__clients[aClientid].will = None

  def disconnectAll(self):
    for c in self.__clients.keys()[:]: # copy the array because disconnect will remove an element
      self.disconnect(c)

  def publish(self, aClientid, topic, message, qos, retained=False):
    """publish to all subscribed connected clients
       also to any disconnected non-cleansession clients with qos in [1,2]
    """
    if retained:
      logger.info("[MQTT-2.1.2-6] store retained message and QoS")
      self.se.setRetained(topic, message, qos)
    else:
      logger.info("[MQTT-2.1.2-12] non-retained message - do not store")

    for subscriber in self.se.subscribers(topic):  # all subscribed clients
      # qos is lower of publication and subscription
      if len(self.se.getSubscriptions(topic, subscriber)) > 1:
        logger.info("[MQTT-3.3.5-1] overlapping subscriptions")
      if retained:
        logger.info("[MQTT-2.1.2-10] outgoing publish does not have retained flag set")
      if self.overlapping_single:   
        out_qos = min(self.se.qosOf(subscriber, topic), qos)
        if subscriber in self.__clients.keys(): 
          self.__clients[subscriber].publishArrived(topic, message, out_qos)
        elif self.__broker3.getClient(subscriber):
          self.__broker3.getClient(subscriber).publishArrived(topic, message, out_qos)
        elif self.__broker5.getClient(subscriber):
          self.__broker5.getClient(subscriber).publishArrived(topic, message, out_qos, None, None)
      else:
        for subscription in self.se.getSubscriptions(topic, subscriber):
          out_qos = min(subscription.getQoS(), qos)
          if subscriber in self.__clients.keys():         
            self.__clients[subscriber].publishArrived(topic, message, out_qos)
          elif self.__broker3.getClient(subscriber):
            self.__broker3.getClient(subscriber).publishArrived(topic, message, out_qos)
          elif self.__broker5.getClient(subscriber):
            self.__broker5.getClient(subscriber).publishArrived(topic, message, out_qos, None, None)

  def __doRetained__(self, aClientid, topic, qos):
    # topic can be single, or a list
    if type(topic) != type([]):
      topic = [topic]
      qos = [qos]
    i = 0
    for t in topic: # t is a wildcard subscription topic
      topicsUsed = []
      for s in self.se.getRetainedTopics(t): # s is a non-wildcard retained topic
        if s not in topicsUsed and Topics.topicMatches(t, s):
          # topic has retained publication
          topicsUsed.append(s)
          (ret_msg, ret_qos) = self.se.getRetained(s)
          thisqos = min(ret_qos, qos[i])
          self.__clients[aClientid].publishArrived(s, ret_msg, thisqos, True)
      i += 1

  def subscribe(self, aClientid, topic, qos):
    rc = self.se.subscribe(aClientid, topic, qos)
    self.__doRetained__(aClientid, topic, qos)
    return rc

  def unsubscribe(self, aClientid, topic):
    self.se.unsubscribe(aClientid, topic)

  def getSubscriptions(self, aClientid=None):
    return self.se.getSubscriptions(aClientid)
 
def unit_tests():
  bn = Brokers()

  class Clients:

    def __init__(self, anId):
     self.id = anId # required
     self.msgqueue = []
     self.cleansession = True

    def publishArrived(self, topic, msg, qos, retained=False):
      "required by broker node class"
      logger.debug(self.id, "publishArrived", repr((topic, msg, qos, retained)))
      self.msgqueue.append((topic, msg, qos))

  Client1 = Clients("Client1")
  Client1.cleansession = False

  bn.connect(Client1)
  bn.subscribe(Client1.id, "topic1", 1)
  bn.publish(Client1.id, "topic1", "message 1", 1)

  assert Client1.msgqueue.pop(0) == ("topic1", "message 1", 1)

  bn.publish(Client1.id, "topic2", "message 2", 1, retained=True)
  bn.subscribe(Client1.id, "topic2", 2)

  assert Client1.msgqueue.pop(0) == ("topic2", "message 2", 1)

  bn.subscribe(Client1.id, "#", 2)
  assert Client1.msgqueue.pop(0) == ("topic2", "message 2", 1)

  bn.subscribe(Client1.id, "#", 0)
  assert Client1.msgqueue.pop(0) == ("topic2", "message 2", 0)
  bn.unsubscribe(Client1.id, "#")

  bn.publish(Client1.id, "topic2/next", "message 3", 2, retained=True)
  bn.publish(Client1.id, "topic2/blah", "message 4", 1, retained=True)
  bn.subscribe(Client1.id, "topic2/+", 2)
  logger.debug(Client1.msgqueue)
  msg1 = Client1.msgqueue.pop(0)
  msg2 = Client1.msgqueue.pop(0)

  assert (msg1 == ("topic2/next", "message 3", 2) and \
          msg2 == ("topic2/blah", "message 4", 1)) or \
         (msg2 == ("topic2/next", "message 3", 2) and \
          msg1 == ("topic2/blah", "message 4", 1))

  assert Client1.msgqueue == []

  bn.disconnect(Client1.id)

  Client2 = Clients("Client2")
  Client2.cleansession = False
  bn.connect(Client2)
  bn.publish(Client2.id, "topic2/a", "queued message 0", 0)
  bn.publish(Client2.id, "topic2/a", "queued message 1", 1)
  bn.publish(Client2.id, "topic2/a", "queued message 2", 2)

  bn.connect(Client1)
  Client1.cleansession = False
  print(Client1.msgqueue)
  assert Client1.msgqueue.pop(0) == ("topic2/a", "queued message 0", 0)
  assert Client1.msgqueue.pop(0) == ("topic2/a", "queued message 1", 1)
  assert Client1.msgqueue.pop(0) == ("topic2/a", "queued message 2", 2)

  bn.disconnect(Client1.id)
  bn.disconnect(Client2.id)

 
