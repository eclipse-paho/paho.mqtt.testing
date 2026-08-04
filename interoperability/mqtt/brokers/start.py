"""
*******************************************************************
  Copyright (c) 2013, 2021 IBM Corp. and Ian Craggs

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
     Ian Craggs - add websockets support
     Ian Craggs - add TLS support
*******************************************************************
"""

import sys, traceback, logging, getopt, threading, ssl, signal

from .V311 import MQTTBrokers as MQTTV3Brokers
from .V5 import MQTTBrokers as MQTTV5Brokers
from .SN import MQTTSNBrokers
from .coverage import filter, measure
from mqtt.formats.MQTTV311 import MQTTException as MQTTV3Exception
from mqtt.formats.MQTTV5 import MQTTException as MQTTV5Exception
from mqtt.formats.MQTTSN import MQTTSNException
from mqtt.brokers.listeners import TCPListeners, UDPListeners, HTTPListeners
from mqtt.brokers.bridges import TCPBridges

logger = None

def handler(signum, frame):
    logger.info('Signal handler called with signal %d', signum)
    raise OSError("Signal interrupt")

def setup_persistence(persistence_filename):
  import ZODB, ZODB.FileStorage, BTrees.OOBTree, transaction, persistent
  storage = ZODB.FileStorage.FileStorage(persistence_filename)
  db = ZODB.DB(storage)
  connection = db.open()
  root = connection.root

  if not hasattr(root, 'mqtt'):
    root.mqtt = BTrees.OOBTree.BTree()
    transaction.commit()

  if not root.mqtt.has_key("sharedData"):
    root.mqtt["sharedData"] = persistent.mapping.PersistentMapping()
    transaction.commit()

  sharedData = root.mqtt["sharedData"]
  return connection, sharedData

def process_config(config, options):
    servers_to_create = []
    lineno = 0
    while lineno < len(config):
      curline = config[lineno].strip()
      lineno += 1
      if curline.startswith('#') or len(curline) == 0:
        continue
      words = curline.split()
      if words[0] == "loglevel":
        if len(words) > 1:
          if words[1].upper() == "DEBUG":
            logger.setLevel(logging.DEBUG)
      elif words[0] == "receive_maximum":
        options["receiveMaximum"] = int(words[1])
      elif words[0] == "topic_alias_maximum":
        options["topicAliasMaximum"] = int(words[1])
      elif words[0] in ["maximum_packet_size", "message_size_limit"]:
        options["maximumPacketSize"] = int(words[1])
      elif words[0] == "persistence" and words[1] == "true":
        options["persistence"] = True
      elif words[0] in ["maximum_qos", "retain_available", "subscription_identifier_available",
              "shared_subscription_available", "server_keep_alive", "visual", "mscfile"]:
        bools = {"true":True,'false':False}
        result = words[1]
        if words[1] in bools.keys():
          result = bools[words[1]]
        else:
          try:
            result = int(words[1])
          except:
            pass
        options[words[0]] = result
      elif words[0] == "listener":
        ca_certs = certfile = keyfile = None
        cert_reqs=ssl.CERT_REQUIRED
        bind_address = ""
        port = 1883; TLS=False; allow_non_sni_connections=True;
        if len(words) > 1:
          port = int(words[1])
        protocol = "mqtt"
        if len(words) >= 3:
          bind_address = words[2]
        if len(words) >= 4:
          if words[3] in ["mqttsn", "http"]:
            protocol = words[3]
        while lineno < len(config) and not config[lineno].strip().startswith("listener"):
          curline = config[lineno].strip()
          lineno += 1
          if curline.startswith('#') or len(curline) == 0:
            continue
          words = curline.split()
          if words[0] == "require_certificate":
            if words[1] == "false":
              cert_reqs=ssl.CERT_OPTIONAL
          elif words[0] == "cafile":
            ca_certs = words[1]; TLS=True
          elif words[0] == "certfile":
            certfile = words[1]; TLS=True
          elif words[0] == "keyfile":
            keyfile = words[1]; TLS=True
          elif words[0] == "allow_non_sni_connections":
            if words[1] == "false":
              allow_non_sni_connections = False
        if protocol == "mqtt":
          servers_to_create.append((TCPListeners, {"host":bind_address, "port":port, "TLS":TLS, "cert_reqs":cert_reqs,
                      "ca_certs":ca_certs, "certfile":certfile, "keyfile":keyfile, 
                      "allow_non_sni_connections":allow_non_sni_connections}))
        elif protocol == "mqttsn":
          servers_to_create.append((UDPListeners, {"host":bind_address, "port":port}))
        elif protocol == "http":
          servers_to_create.append((HTTPListeners, {"host":bind_address, "port":port, "TLS":TLS, "cert_reqs":cert_reqs,
              "ca_certs":ca_certs, "certfile":certfile, "keyfile":keyfile}))
    servers_to_create[-1][1]["serve_forever"] = True
    return servers_to_create, options

def run(config=None):
  global logger, broker3, broker5, brokerSN, server
  logger = logging.getLogger('MQTT broker')
  logger.setLevel(logging.INFO)
  logger.addFilter(filter)

  logger.info("Python version "+sys.version)

  signal.signal(signal.SIGTERM, handler)

  lock = threading.RLock() # shared lock

  options = {
    "visual":False,
    "persistence": False,
    "overlapping_single": True,
    "dropQoS0": True, 
    "zero_length_clientids":True, 
    "publish_on_pubrel":False,
    "topicAliasMaximum":2,
    "maximumPacketSize":256,
    "receiveMaximum":2,
    "serverKeepAlive":60,
    "maximum_qos":2,
    "retain_available":True,
    "subscription_identifier_available":True,
    "shared_subscription_available":True,
    "server_keep_alive":None,
  }

  if config != None:
    servers_to_create, options = process_config(config, options)

  if options["persistence"]:
    logger.info("Using persistence")    
    connection, sharedData = setup_persistence("sharedData") # location for data shared between brokers - subscriptions for example
  else:
    sharedData = {}
  logger.debug("Starting sharedData %s", sharedData)

  broker3 = MQTTV3Brokers(options=options.copy(), lock=lock, sharedData=sharedData)

  broker5 = MQTTV5Brokers(options=options.copy(), lock=lock, sharedData=sharedData)

  brokerSN = MQTTSNBrokers(lock=lock, sharedData=sharedData)

  brokers = [broker3, broker5, brokerSN]

  broker3.setBroker5(broker5)
  broker5.setBroker3(broker3)

  brokerSN.setBroker3(broker3)
  brokerSN.setBroker5(broker5)

  servers = []
  UDPListeners.setBroker(brokerSN)
  TCPListeners.setBrokers(broker3, broker5)
  HTTPListeners.setBrokers(broker3, broker5, brokerSN)
  HTTPListeners.setSharedData(lock, sharedData)

  try:
    if config == None:
      #TCPBridges.setBroker5(broker5)
      #TCPBridges.create(1886)
      servers.append(TCPListeners.create(1883, serve_forever=True))
    else:
      for server in servers_to_create:
        servers.append(server[0].create(**server[1]))
  except (KeyboardInterrupt, OSError):
    pass
  except:
    logger.exception("startBroker")

  # Stop incoming communications
  import socket
  for server in servers:
    try:
      logger.info("Stopping listener %s", str(server))
      server.shutdown()
    except:
      traceback.print_exc()

  logger.info("Shutdown brokers")
  for broker in brokers:
    try:
      logger.info("Stopping broker %s", str(broker))
      broker.shutdown()
    except:
      traceback.print_exc()
  filter.measure()

  logger.debug("Ending sharedData %s", sharedData)
  if options["persistence"]:
    sharedData._p_changed = True
    import transaction
    transaction.commit()
    connection.close()

def measure():
  return filter.getmeasures()

def stop():
  global server
  server.shutdown()

def reinitialize():
  global broker3, broker5, brokerSN
  broker3.reinitialize()
  broker5.reinitialize()
  brokerSN.reinitialize()

def read_config(filename):
  infile = open(filename)
  lines = infile.readlines()
  infile.close()
  return lines

def main(argv):
  try:
    opts, args = getopt.gnu_getopt(argv[1:], "hp:o:d:z:c:", ["help", "publish_on_pubrel=", "overlapping_single=",
        "dropQoS0=", "port=", "zero_length_clientids=", "config-file="])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)
  overlapping_single = dropQoS0 = zero_length_clientids = True
  publish_on_pubrel = False
  port = 1883
  cfg = None
  for o, a in opts:
    if o in ("-h", "--help"):
      usage()
      sys.exit()
    elif o in ("--port"):
      port = int(a)
    elif o in ("-c", "--config-file"):
      cfg = read_config(a)
    else:
      assert False, "unhandled option"

  run(config=cfg)

def usage():
  print(
"""
Eclipse Paho combined MQTT V311 and MQTT V5 broker

 -h --help: print this message
 -c --confile-file: the name of a configuration file
 --port= port number to listen to

""")

if __name__ == "__main__":
  main(sys.argv)
