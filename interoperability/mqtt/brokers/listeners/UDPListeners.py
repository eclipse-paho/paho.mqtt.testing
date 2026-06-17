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
     Ian Craggs - add websockets support
     Ian Craggs - add TLS support
*******************************************************************
"""

import socketserver, select, sys, traceback, socket, logging, getopt, hashlib, base64
import threading, ssl

from mqtt.brokers.SN import MQTTSNBrokers
from mqtt.formats.MQTTSN import MQTTSNException

logger = logging.getLogger('MQTT broker')

def respond(handler, data):
  socket = handler.request[1]
  socket.sendto(data, handler.client_address)

class UDPHandler(socketserver.BaseRequestHandler):
  """
    This class works similar to the TCP handler class, except that
    self.request consists of a pair of data and client socket, and since
    there is no connection the client address must be given explicitly
    when sending data back via sendto().
  """

  def handle(self):
    try:
      terminate = brokerSN.handleRequest(self.request[0], self.client_address, (respond, self))
    except MQTTSNException as exc:
      logger.error(exc.args[0])


class ThreadingUDPServer(socketserver.ThreadingMixIn,
                           socketserver.UDPServer):
  pass


def setBroker(aBrokerSN):
  global brokerSN
  brokerSN = aBrokerSN

def create(port, host="", serve_forever=False):
  logger.info("Starting UDP listener on address '%s' port %d", host, port)
  bind_address = ""
  if host not in ["", "INADDR_ANY"]:
    bind_address = host
  server = ThreadingUDPServer((bind_address, port), UDPHandler, False)
  server.terminate = False
  server.allow_reuse_address = True
  server.server_bind()
  server.server_activate()
  if serve_forever:
    server.serve_forever()
  else:
    thread = threading.Thread(target = server.serve_forever)
    thread.daemon = True
    thread.start()
  return server

