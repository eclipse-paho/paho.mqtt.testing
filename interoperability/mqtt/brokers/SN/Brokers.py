"""
*******************************************************************
  Copyright (c) 2013, 2026 IBM Corp.

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
"""

"""
  MQTT-SN 2.0 broker node functionality.

  Modelled on BrokersV5.py (MQTT 5.0) but adapted for MQTT-SN 2.0 semantics:
    - Session expiry uses SessionExpiryInterval (absent == 0, i.e. clean on disconnect)
    - Will delay is not supported in MQTT-SN 2.0 (will is sent immediately on abnormal disconnect)
    - No shared subscriptions in MQTT-SN 2.0
    - No topic aliases on the broker→client direction (topic aliases are client-registered via REGISTER)
"""

import time, logging

from . import Topics
from .SubscriptionEngines import SubscriptionEngines
import mqtt.formats.MQTTSN2 as MQTTSN

logger = logging.getLogger('MQTT-SN broker')


class Brokers:

  def __init__(self, overlapping_single=True, sharedData={}):
    self.sharedData = sharedData
    self.se = SubscriptionEngines(self.sharedData)
    self.__clients = {}  # clientid -> client object
    self.overlapping_single = overlapping_single

  def setBroker3(self, broker3):
    self.__broker3 = broker3

  def setBroker5(self, broker5):
    self.__broker5 = broker5

  def reinitialize(self):
    self.__clients = {}
    self.se.reinitialize()

  def getClients(self):
    return self.__clients

  def getClient(self, clientid):
    return self.__clients.get(clientid)

  def cleanSession(self, aClientid):
    "Clear any outstanding subscriptions for this client."
    if len(self.se.getRetainedTopics("#")) > 0:
      logger.info("[MQTT-SN-4.1-1] retained messages not cleaned up as part of session state for client %s", aClientid)
    self.se.clearSubscriptions(aClientid)

  def connect(self, aClient, clean=False):
    """Register a client as connected.

    clean corresponds to the MQTT-SN 2.0 CleanStart flag: when True, any
    existing session state (subscriptions, queued messages) for this client
    is discarded before the new session begins, matching MQTT 5.0 semantics.
    The caller passes clean=True when the CONNECT packet had CleanStart set.
    """
    aClient.connected = True
    try:
      aClient.timestamp = time.clock()
    except AttributeError:
      aClient.timestamp = time.process_time()
    self.__clients[aClient.id] = aClient
    if clean:
      self.cleanSession(aClient.id)

  def terminate(self, aClientid):
    """Abrupt disconnect — sends the will message (if any) then disconnects."""
    if aClientid in self.__clients and self.__clients[aClientid].connected:
      if self.__clients[aClientid].will is not None:
        logger.info("[MQTT-SN-3.1.2-8] sending will message for client %s", aClientid)
        willtopic, willQoS, willmsg, willRetain = self.__clients[aClientid].will
        if willRetain:
          logger.info("[MQTT-SN-3.1.2-17] sending will message retained for client %s", aClientid)
        else:
          logger.info("[MQTT-SN-3.1.2-16] sending will message non-retained for client %s", aClientid)
        self.publish(aClientid, willtopic, willmsg, willQoS, willRetain, time.monotonic())
      self.disconnect(aClientid, sessionExpiryInterval=0)

  def disconnect(self, aClientid, sessionExpiryInterval=-1):
    """Normal disconnect.

    sessionExpiryInterval:
      -1  → use the value negotiated at connect time (aClient.sessionExpiryInterval)
       0  → end session immediately (clean up state and remove client record)
      >0  → persist session state; client record is kept but marked disconnected

    Per MQTT-SN 2.0 §3.1.9: an absent SessionExpiryInterval in CONNECT means 0
    (session ends on disconnect), so aClient.sessionExpiryInterval defaults to 0.
    """
    if aClientid not in self.__clients:
      return
    client = self.__clients[aClientid]
    client.connected = False

    # Resolve effective session expiry
    if sessionExpiryInterval == -1:
      # Fall back to what was agreed at connect time
      effective_sei = getattr(client, 'sessionExpiryInterval', 0)
      if effective_sei is None:
        effective_sei = 0
    else:
      effective_sei = sessionExpiryInterval

    if effective_sei == 0:
      logger.info("[MQTT-SN-3.1.2-6] broker must discard the session data for client %s", aClientid)
      self.cleanSession(aClientid)
      del self.__clients[aClientid]
    else:
      logger.info("[MQTT-SN-3.1.2-4] broker must store the session data for client %s", aClientid)
      try:
        client.timestamp = time.clock()
      except AttributeError:
        client.timestamp = time.process_time()
      client.connected = False
      logger.info("[MQTT-SN-3.1.2-10] will message is deleted after use or disconnect, for client %s", aClientid)
      logger.info("[MQTT-SN-3.14.4-3] on receipt of disconnect, will message is deleted")
      client.will = None

  def disconnectAll(self):
    for c in list(self.__clients.keys()):
      self.disconnect(c)

  def publish(self, aClientid, topic, message, qos, retained, receivedTime):
    """Publish to all subscribed connected clients.

    Also delivers to disconnected clients with a persistent session (sessionExpiryInterval > 0)
    for QoS 1 and 2, provided their publishArrived() implementation queues the message.

    No shared subscriptions in MQTT-SN 2.0.
    """
    if retained:
      logger.info("[MQTT-SN-2.1.2-6] store retained message and QoS")
      self.se.setRetained(topic, message, qos, receivedTime, {})
    else:
      logger.info("[MQTT-SN-2.1.2-12] non-retained message - do not store")

    subscriptions = self.se.subscriptions(topic)
    # Deduplicate: one delivery per client (overlapping_single mode picks max QoS via optionsOf)
    client_ids = {s.getClientid() for s in subscriptions}

    for subscriber in client_ids:
      subs_for_client = self.se.getSubscriptions(topic, subscriber)
      if len(subs_for_client) > 1:
        logger.info("[MQTT-SN-3.3.5-1] overlapping subscriptions for client %s", subscriber)

      if retained:
        logger.info("[MQTT-SN-2.1.2-10] outgoing publish does not have retained flag set")

      client = self.__clients.get(subscriber)
      if client is None:
        continue  # client record gone; nothing to do

      if self.overlapping_single:
        options = self.se.optionsOf(subscriber, topic)
        if options is not None:
          out_qos = min(options.QoS, qos)
          client.publishArrived(topic, message, out_qos)
      else:
        for subscription in subs_for_client:
          out_qos = min(subscription.getQoS(), qos)
          client.publishArrived(topic, message, out_qos)

    return list(client_ids) if client_ids else None

  def __doRetained__(self, aClientid, topic, options, resubscribed):
    """Deliver retained messages to a newly subscribed client.

    topic is always a single topic filter string in MQTT-SN 2.0.

    Retain-handling logic (from SubscribeFlags.RetainHandling):
      0 → always send retained messages
      1 → send only on a new (not re-) subscription
      2 → never send
    """
    client = self.__clients.get(aClientid)
    if client is None:
      return

    retain_handling = getattr(options, 'RetainHandling', 0)
    if retain_handling == 2:
      return
    if retain_handling == 1 and resubscribed:
      return

    topics_used = []
    for s in self.se.getRetainedTopics(topic):
      if s not in topics_used and Topics.topicMatches(topic, s):
        topics_used.append(s)
        retained_msg = self.se.getRetained(s)
        if retained_msg is None:
          continue
        if len(retained_msg) == 4:
          (ret_msg, ret_qos, receivedTime, _props) = retained_msg
        else:
          (ret_msg, ret_qos, receivedTime) = retained_msg
        thisqos = min(ret_qos, options.QoS)
        client.publishArrived(s, ret_msg, thisqos, retained=True)

  def subscribe(self, aClientid, topic, options):
    """Subscribe aClientid to topic with the given SubscribeFlags options.

    Returns the (subscription, resubscribed) tuple from SubscriptionEngines.subscribe().
    Also delivers any retained messages matching the topic filter.
    """
    rc, resubscribed = self.se.subscribe(aClientid, topic, options)
    self.__doRetained__(aClientid, topic, options, resubscribed)
    return rc, resubscribed

  def unsubscribe(self, aClientid, topic):
    """Unsubscribe aClientid from topic.

    Returns a ReasonCode (Success or No subscription existed).
    """
    return self.se.unsubscribe(aClientid, topic)

  def getSubscriptions(self, aClientid=None):
    return self.se.getSubscriptions(aClientid)


def unit_tests():
  bn = Brokers()

  class MockClient:

    def __init__(self, anId, sessionExpiryInterval=0):
      self.id = anId
      self.msgqueue = []
      self.sessionExpiryInterval = sessionExpiryInterval
      self.will = None
      self.connected = False

    def publishArrived(self, topic, msg, qos, retained=False):
      logger.debug("%s publishArrived %s", self.id, repr((topic, msg, qos, retained)))
      self.msgqueue.append((topic, msg, qos))

  Client1 = MockClient("Client1", sessionExpiryInterval=3600)
  bn.connect(Client1, clean=False)
  bn.subscribe(Client1.id, "topic1", MQTTSN.SubscribeFlags(QoS=1))
  bn.publish(Client1.id, "topic1", b"message 1", 1, False, time.monotonic())
  assert Client1.msgqueue.pop(0) == ("topic1", b"message 1", 1)

  bn.publish(Client1.id, "topic2", b"message 2", 1, True, time.monotonic())
  bn.subscribe(Client1.id, "topic2", MQTTSN.SubscribeFlags(QoS=2))
  assert Client1.msgqueue.pop(0) == ("topic2", b"message 2", 1)

  bn.subscribe(Client1.id, "#", MQTTSN.SubscribeFlags(QoS=2))
  assert Client1.msgqueue.pop(0) == ("topic2", b"message 2", 1)

  bn.subscribe(Client1.id, "#", MQTTSN.SubscribeFlags(QoS=0))
  assert Client1.msgqueue.pop(0) == ("topic2", b"message 2", 0)
  bn.unsubscribe(Client1.id, "#")

  bn.publish(Client1.id, "topic2/next", b"message 3", 2, True, time.monotonic())
  bn.publish(Client1.id, "topic2/blah", b"message 4", 1, True, time.monotonic())
  bn.subscribe(Client1.id, "topic2/+", MQTTSN.SubscribeFlags(QoS=2))
  msg1 = Client1.msgqueue.pop(0)
  msg2 = Client1.msgqueue.pop(0)
  assert (msg1 == ("topic2/next", b"message 3", 2) and
          msg2 == ("topic2/blah", b"message 4", 1)) or \
         (msg2 == ("topic2/next", b"message 3", 2) and
          msg1 == ("topic2/blah", b"message 4", 1))
  assert Client1.msgqueue == []

  # Test persistent session: disconnect Client1, publish while away, reconnect
  bn.disconnect(Client1.id, sessionExpiryInterval=3600)
  assert Client1.id in bn.getClients(), "Persistent session should be retained"

  Client2 = MockClient("Client2", sessionExpiryInterval=3600)
  bn.connect(Client2, clean=False)
  bn.publish(Client2.id, "topic2/a", b"queued message 1", 1, False, time.monotonic())
  bn.publish(Client2.id, "topic2/a", b"queued message 2", 2, False, time.monotonic())

  # Reconnect Client1 without CleanStart: subscriptions and queued messages survive
  bn.connect(Client1, clean=False)
  assert Client1.msgqueue.pop(0) == ("topic2/a", b"queued message 1", 1)
  assert Client1.msgqueue.pop(0) == ("topic2/a", b"queued message 2", 2)

  # Reconnect Client1 with CleanStart=True: subscriptions wiped
  bn.connect(Client1, clean=True)
  assert len(bn.se.getClientSubscriptions(Client1.id)) == 0, \
    "CleanStart must clear subscriptions"

  # Test will message delivery on abrupt disconnect
  Client3 = MockClient("Client3")
  Client3.will = ("willtopic", 0, b"will payload", False)
  bn.connect(Client3)
  bn.subscribe(Client1.id, "willtopic", MQTTSN.SubscribeFlags(QoS=0))
  bn.terminate(Client3.id)
  assert Client1.msgqueue.pop(0) == ("willtopic", b"will payload", 0), \
    "Will message should be delivered on terminate()"

  bn.disconnect(Client1.id, sessionExpiryInterval=0)
  bn.disconnect(Client2.id, sessionExpiryInterval=0)

  logger.info("unit_tests passed")