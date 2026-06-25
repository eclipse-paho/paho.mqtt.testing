"""
*******************************************************************
  Copyright (c) 2013, 2026 IBM Corp.

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

import time, logging

logger = logging.getLogger('MQTT-SN broker')

class Subscriptions:

  def __init__(self, aClientid, aTopic, options):
    self.__clientid = aClientid
    self.__topic = aTopic
    self.__options = options

  def getClientid(self):
    return self.__clientid

  def getTopic(self):
    return self.__topic

  def getQoS(self):
    return self.__options.QoS

  def getOptions(self):
    return self.__options

  def resubscribe(self, options):
    logger.info("[MQTT-SN-3.7.6-3] resubscription for client %s on topic %s", self.__clientid, self.__topic)
    self.__options = options

  def __repr__(self):
    return repr({"clientid":self.__clientid, "topic":self.__topic, "options":str(self.__options)})
