"""
*******************************************************************
  Copyright (c) 2013, 2026 Ian Craggs, IBM Corp.

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

""" 
  MQTT-SN 2.0 subscription engine funcionality
"""

import types, logging

from . import Topics, Subscriptions
import mqtt.formats.MQTTSN2 as MQTTSN

from .Subscriptions import *

logger = logging.getLogger('MQTT broker')

def isDollarTopic(name):
  return name[0] == '$' # and not name.startswith('$share/')

class SubscriptionEngines:

   def __init__(self, sharedData={}):
     self.sharedData = sharedData
     if "subscriptions" not in self.sharedData:
       self.sharedData["subscriptions"] = []  # list of subscriptions
     else:
       logger.info("Sharing subscription data")
     if "dollar_subscriptions" not in self.sharedData:
       self.sharedData["dollar_subscriptions"] = []  # list of subscriptions
     self.__subscriptions = self.sharedData["subscriptions"] 
     self.__dollar_subscriptions = self.sharedData["dollar_subscriptions"] 
     if "retained" not in self.sharedData:
       self.sharedData["retained"] = {}  # map of topics to retained msg+qos
     self.__retained = self.sharedData["retained"]
     if "dollar_retained" not in self.sharedData:
       self.sharedData["dollar_retained"] = {}  # map of topics to retained msg+qos
     self.__dollar_retained = self.sharedData["dollar_retained"] 

   def reinitialize(self):
     self.__init__()

   def subscribe(self, aClientid, aTopic, options):
     "in MQTT-SN we can subscribe to one topic at a time only"
     if type(aTopic) == type([]):
       raise Exception("MQTT-SN - only one subscription per subscribe request")
     rc = None
     resubscribed = False
     if Topics.isValidTopicName(aTopic):
       subscriptions = self.__subscriptions if not isDollarTopic(aTopic) else self.__dollar_subscriptions
       for s in subscriptions:
         if s.getClientid() == aClientid and s.getTopic() == aTopic:
           s.resubscribe(options)
           resubscribed = True
       if not resubscribed:
         rc = Subscriptions(aClientid, aTopic, options)
         subscriptions.append(rc)
     return rc, resubscribed

   def unsubscribe(self, aClientid, aTopic):
     rc = []
     matched = self.__unsubscribe(aClientid, aTopic)
     rc = MQTTSN.ReasonCodes(MQTTSN.PacketTypes.UNSUBACK, "Success") if matched else \
          MQTTSN.ReasonCodes(MQTTSN.PacketTypes.UNSUBACK, "No subscription existed")
     if not matched:
       logger.info("[MQTT-SN-3.9.6-6] Even where no Topic Subscriptions are deleted, the Server MUST respond with an UNSUBACK")
     return rc

   def __unsubscribe(self, aClientid, aTopic):
     "unsubscribe to one topic"
     matched = False
     if Topics.isValidTopicName(aTopic):
       subscriptions = self.__subscriptions if not isDollarTopic(aTopic) else self.__dollar_subscriptions
       for s in subscriptions:
         if s.getClientid() == aClientid and s.getTopic() == aTopic:
           logger.info("[MQTT-SN-3.9.6-2] topic filters must be compared byte for byte")
           logger.info("[MQTT-SN-3.9.6-3] no more messages must be added after unsubscribe is complete")
           subscriptions.remove(s)
           matched = True
           break # once we've hit one, that's us done
     return matched

   def clearSubscriptions(self, aClientid):
     for subscriptions in [self.__subscriptions, self.__dollar_subscriptions]:
       for s in subscriptions[:]:
         if s.getClientid() == aClientid:
           subscriptions.remove(s)

   def getSubscriptions(self, aTopic, aClientid=None):
     "return a list of subscriptions for this topic, optionally for one client"
     rc = None
     if Topics.isValidTopicName(aTopic):
       subscriptions = self.__subscriptions if not isDollarTopic(aTopic) else self.__dollar_subscriptions
       if aClientid == None:
         rc = [sub for sub in subscriptions if Topics.topicMatches(sub.getTopic(), aTopic)]
       else:
         rc = [sub for sub in subscriptions if sub.getClientid() == aClientid and Topics.topicMatches(sub.getTopic(), aTopic)]
     return rc
   
   def getClientSubscriptions(self, aClientid):
     "return a list of subscriptions for one client"
     rc = {sub for sub in self.__subscriptions if sub.getClientid() == aClientid}
     rc1 = {sub for sub in self.__dollar_subscriptions if sub.getClientid() == aClientid}
     return rc.union(rc1)

   def optionsOf(self, clientid, topic):
     # if there are overlapping subscriptions, choose maximum QoS
     chosen = None
     for sub in self.getSubscriptions(topic, clientid):
       if chosen == None:
         if hasattr(sub, "getOptions"):
           chosen = sub.getOptions() #V5 or SN
         else: # MQTT V3 case
           chosen = MQTTSN.SubscribeFlags(QoS=sub.getQoS())
       else:
         logger.info("[MQTT-SN-3.6.3.7-2] Overlapping subscriptions max QoS")
         if sub.getQoS() > chosen[0].QoS:
           if hasattr(sub, "getOptions"):
             chosen = sub.getOptions()  #V5 or SN
           else: # MQTT V3 case
             chosen = MQTTSN.SubscribeFlags(QoS=sub.getQoS())
       # Omit the following optimization because we want to check for condition [MQTT-3.3.5-1]
       #if chosen == 2:
       #  break
     return chosen

   def subscriptions(self, aTopic):
     "list all clients subscribed to this (non-wildcard) topic"
     result = set()
     if Topics.isValidTopicName(aTopic):
       subscriptions = self.__subscriptions if not isDollarTopic(aTopic) else self.__dollar_subscriptions
       for s in subscriptions:
         if Topics.topicMatches(s.getTopic(), aTopic):
           result.add(s) # don't add a subscription twice
     return result

   def setRetained(self, aTopic, aMessage, aQoS, receivedTime, properties):
     "set a retained message on a non-wildcard topic"
     if Topics.isValidTopicName(aTopic):
       retained = self.__retained if not isDollarTopic(aTopic) else self.__dollar_retained
       if len(aMessage) == 0:
         if aTopic in retained.keys():
           logger.info("[MQTT-3.3.1-11] Deleting zero byte retained message")
           del retained[aTopic]
       else:
         retained[aTopic] = (aMessage, aQoS, receivedTime, properties)

   def getRetained(self, aTopic):
     "returns (msg, QoS, properties) for a topic"
     result = None
     if Topics.isValidTopicName(aTopic):
       retained = self.__retained if not isDollarTopic(aTopic) else self.__dollar_retained
       if aTopic in retained.keys():
         result = retained[aTopic]
     return result

   def getRetainedTopics(self, aTopic):
     "returns a list of topics for which retained publications exist"
     if Topics.isValidTopicName(aTopic):
       retained = self.__retained if not isDollarTopic(aTopic) else self.__dollar_retained
       return retained.keys()
     else:
       return None


def unit_tests():
  se = SubscriptionEngines()
  se.subscribe("Client1", "topic1", MQTTSN.SubscribeFlags(2))
  se.subscribe("Client1", "topic2", MQTTSN.SubscribeFlags(1))
  assert [s.getClientid() for s in se.subscriptions("topic1")] == ["Client1"]
  assert [s.getQoS() for s in se.subscriptions("topic1")] == [2]

  se.subscribe("Client2", "topic2", MQTTSN.SubscribeFlags(2))
  assert [s.getClientid() for s in se.subscriptions("topic1")] == ["Client1"]
  assert {s.getClientid() for s in se.subscriptions("topic2")} == {"Client1", "Client2"}

  se.subscribe("Client2", "#", MQTTSN.SubscribeFlags(0))
  assert {s.getClientid() for s in se.subscriptions("topic1")}  == {"Client1", "Client2"}
  assert {s.getClientid() for s in se.subscriptions("topic2")}  == {"Client1", "Client2"}
  assert [s.getClientid() for s in se.subscriptions("topic3")]  == ["Client2"]
  assert {s.getTopic() for s in se.getClientSubscriptions("Client2")} == {"#", "topic2"}

  print("Before clear: %s", se.getClientSubscriptions("Client2"))
  se.clearSubscriptions("Client2")
  print("After clear, client2: %s", se.getClientSubscriptions("Client2"))
  assert len(se.getClientSubscriptions("Client2")) == 0
  logger.info("After clear, client1: %s", se.getClientSubscriptions("Client1"))
  assert len(se.getClientSubscriptions("Client1")) > 0
 
 
