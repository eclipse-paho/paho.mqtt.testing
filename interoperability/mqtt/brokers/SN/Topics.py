"""
*******************************************************************
  Copyright (c) 2013, 2026 IBM Corp.
 
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


import re, logging
from mqtt.formats import MQTTSN2 as MQTTSN

logger = logging.getLogger('MQTT broker')

 
def isValidTopicName(aName):
  logger.info("[MQTT-SN-4.7.1.3-1] all topic names and filters must be at least 1 char")
  if len(aName) < 1:
    raise MQTTSN.MQTTSNException("[MQTT-SN-4.7.1.3-1] all topic names and filters must be at least 1 char")
    return False
  logger.info("[MQTT-SN-4.7.1.3-3] all topic names and filters must be <= 65535 bytes long")
  if len(aName) > 65535:
    raise MQTTSN.MQTTSNException("[MQTT-SN-4.7.1.3-3] all topic names and filters must be <= 65535 bytes long")
    return False
  rc = True

  # '#' wildcard can be only at the end of a topic (used to be beginning as well)
  logger.info("[MQTT-SN-4.7.1.1.2-1] # must be last, and next to /")
  if aName[0:-1].find('#') != -1:
    raise MQTTSN.MQTTSNException("[MQTT-SN-4.7.1.1.2-1] # must be last, and next to /")
    rc = False

  logger.info("[MQTT-SN-4.7.1.1.3-1] + can be used at any complete level")
  # '#' or '+' only next to a slash separator or end of name
  wilds = '#+'
  for c in wilds:
    pos = 0
    pos = aName.find(c, pos)
    while pos != -1:
      if pos > 0: # check previous char is '/'
        if aName[pos-1] != '/':
          raise MQTTSN.MQTTSNException("[MQTT-SN-4.7.1.1.3-1] + can be used at any complete level")
          rc = False
      if pos < len(aName)-1: # check that subsequent char is '/'
        if aName[pos+1] != '/':
          raise MQTTSN.MQTTSNException("[MQTT-SN-4.7.1.1.3-1] + can be used at any complete level")
          rc = False
      pos = aName.find(c, pos+1)
  return rc
 

def topicMatches(wild, nonwild, wildCheck=True):
  if wildCheck:
    assert nonwild.find('+') == nonwild.find('#') == -1
  assert isValidTopicName(wild) and isValidTopicName(nonwild)

  """
  # no shared subscriptions in MQTT-SN
  if wild.startswith('$share'):
    # strip shared prefix $share/sharename/
    assert wild.count('/') >= 2
    wild = wild.split('/', 2)[2]
  """

  rc = False
  if wild.find('+') == wild.find('#') == -1:
    # no wildcards, so check is simple
    rc = (wild == nonwild)
  else:
    # we have wildcards. Escape metacharacters, except +
    metachars = '\\.^$*?{[]|()' # make sure \\ is at beginning of this string
    for s in metachars:
      wild = wild.replace(s, '\\'+s)

    # now replace wildcards with regular expressions and match
    if wild == '#':
      "# on its own matches everything"
      wild = '.*'
    elif wild == '/#':
      "/# matches everything starting with a slash"
      wild = '/.*'
    else:
      "all other instances subsume the preceding or following slash"
      wild = wild.replace('#/', '(.*?/|^)').replace('/#', '(/.*?|$)')
    wild = wild.replace('+', '[^/#]+?') # + does not match an empty level
    if re.compile(wild+'$').match(nonwild) != None:
      rc = True
  return rc


""" 
topicCoverage = dataclasses("string", (1, 64),
                ["TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA", "MQTTSAS topic"],
                static=True)
wildTopicCoverage = dataclasses("string", (1, 64),
                    ["TopicA/+", "+/C", "#", "/#", "/+", "+/+"],
                    static=True)
""" 

def unit_tests():
  topics = \
  ['level1', 'level1/level2', 'level1/level2/level3',
   'le(el1/le?el2', '/level1a']

  tests = \
  [('level1/+/level3', [False, False, True, False, False]),
   ('level1/#', [True, True, True, False, False]),
   ('level1/level2', [False, True, False, False, False]),
   ('le(el1/le)el2', [False, False, False, False, False]),
   ('+/le?el2', [False, False, False, True, False]),
   ('/le?el2', [False, False, False, False, False]),
   ('/+', [False, False, False, False, True]),
   ('level3/#', [False, False, False, False, False]),
   ('level1/#', [True, True, True, False, False]),
   ('/#', [False, False, False, False, True]),
   ('#', [True, True, True, True, True])]

  for t in tests:
    wildtopic, results = t
    try:
      assert isValidTopicName(wildtopic), \
               "Topic validation failed for: "+wildtopic
    except:
      print("Topic validation failed for", wildtopic)
    for i in range(len(results)):
      assert topicMatches(wildtopic, topics[i]) == results[i], \
        "Failed: "+ wildtopic+" "+topics[i]+" "+repr(results[i])
      logger.info("Worked: %s %s %s", wildtopic, topics[i], results[i])

  tests = \
  [('level1/+/level3', True),
   ('level1//level3', True),
   ('level1/#/level3', False),
   ('level1/+/level4/#', True),
   ('##/level2', False),
   ('level1#', False)]

  for topic in topics:
    assert isValidTopicName(topic), "Topic validation failed for: "+topic
  for t in tests:
    topic, result = t
    try:
      assert isValidTopicName(topic) == result, "Topic validation failed for: "+topic
    except:
      assert result == False, "Topic validation failed for: "+topic
      
  logger.info(not topicMatches("1/2/#", "1/2s/s"))
  assert not topicMatches("1/2/#", "1/2s/s")
  assert topicMatches("1/2/#", "1/2/s")

  assert not topicMatches("+/1/2", "1/2")
  assert not topicMatches("+/1/2", "o/s1/2")

 
