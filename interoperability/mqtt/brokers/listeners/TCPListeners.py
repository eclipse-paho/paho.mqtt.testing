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
     Ian Craggs - add websockets support
     Ian Craggs - add TLS support
*******************************************************************
"""

import socketserver, select, sys, traceback, socket, logging, getopt, hashlib, base64
import threading, ssl

from mqtt.brokers.V311 import MQTTBrokers as MQTTV3Brokers
from mqtt.brokers.V5 import MQTTBrokers as MQTTV5Brokers
from mqtt.formats.MQTTV311 import MQTTException as MQTTV3Exception
from mqtt.formats.MQTTV5 import MQTTException as MQTTV5Exception

server = None
logger = logging.getLogger('MQTT broker')

class BufferedSockets:

  def __init__(self, socket):
    self.socket = socket
    self.buffer = bytearray()
    self.websockets = False

  def rebuffer(self, data):
    self.buffer = data + self.buffer

  def wsrecv(self):
    header1 = ord(self.socket.recv(1))
    header2 = ord(self.socket.recv(1))

    opcode = (header1 & 0x0f)
    maskbit = (header2 & 0x80) == 0x80
    length = (header2 & 0x7f) # works for 0 to 125 inclusive
    if length == 126: # for 126 to 65535 inclusive
      lb1 = ord(self.socket.recv(1))
      lb2 = ord(self.socket.recv(1))
      length = lb1*256 + lb2
    elif length == 127:
      length = 0
      for i in range(0, 8):
        length += ord(self.socket.recv(1)) * 2**((7 - i)*8)
    assert maskbit == True
    if maskbit:
      mask = self.socket.recv(4)
    mpayload = bytearray()
    while len(mpayload) < length:
      mpayload += self.socket.recv(length - len(mpayload))
    buffer = bytearray()
    if maskbit:
      mi = 0
      for i in mpayload:
        buffer.append(i ^ mask[mi])
        mi = (mi+1)%4
    else:
      buffer = mpayload
    self.buffer += buffer

  def recv(self, bufsize):
    if self.websockets:
      while len(self.buffer) < bufsize:
        self.wsrecv()
      out = self.buffer[:bufsize]
      self.buffer = self.buffer[bufsize:]
    else:
      if bufsize <= len(self.buffer):
        out = self.buffer[:bufsize]
        self.buffer = self.buffer[bufsize:]
      else:
        out = self.buffer + self.socket.recv(bufsize - len(self.buffer))
        self.buffer = bytes()
    return out

  def __getattr__(self, name):
    return getattr(self.socket, name)

  def send(self, data):
    header = bytearray()
    if self.websockets:
      header.append(0x82) # opcode
      l = len(data)
      if l < 126:
        header.append(l)
      elif l < 65536:
        """ If 126, the following 2 bytes interpreted as a 16-bit unsigned integer are
            the payload length.
        """
        header += bytearray([126, l // 256, l % 256])
      elif l < 2**64:
        """ If 127, the following 8 bytes interpreted as a 64-bit unsigned integer (the
            most significant bit MUST be 0) are the payload length.
        """
        mybytes = [127]
        for i in range(0, 7):
          divisor = 2**((7 - i)*8)
          mybytes.append(l // divisor)
          l %= divisor
        mybytes.append(l) # units
        header += bytearray(mybytes)
    totaldata = header + data
    # Ensure the entire packet is sent by calling send again if necessary
    sent = self.socket.send(totaldata)
    while sent < len(totaldata):
      sent += self.socket.send(totaldata[sent:])
    return sent


class WebSocketTCPHandler(socketserver.StreamRequestHandler):

  def getheaders(self, data):
    "return headers: keys are converted to upper case so that checks are case insensitive"
    headers = {}
    lines = data.splitlines()
    for curline in lines[1:]:
      if curline.find(":") != -1:
        key, value = curline.split(": ", 1)
        headers[key.upper()] = value     # headers are case insensitive
    return headers

  def handshake(self, client):
    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    data = client.recv(1024).decode('utf-8')
    headers = self.getheaders(data)
    digest = base64.b64encode(hashlib.sha1((headers['SEC-WEBSOCKET-KEY'] + GUID).encode("utf-8")).digest())
    resp = b"HTTP/1.1 101 Switching Protocols\r\n" +\
           b"Upgrade: websocket\r\n" +\
           b"Connection: Upgrade\r\n" +\
           b"Sec-WebSocket-Protocol: mqtt\r\n" +\
           b"Sec-WebSocket-Accept: " + digest +b"\r\n\r\n"
    return client.send(resp)

  def handle(self):
    global server
    first = True
    broker = None
    sock = BufferedSockets(self.request)
    sock_no = sock.fileno()
    terminate = keptalive = False
    logger.info("Starting communications for socket %d", sock_no)
    while not terminate and server and not server.terminate:
      try:
        if not keptalive:
          logger.debug("Waiting for request")
        (i, o, e) = select.select([sock], [], [], 1)
        if i == [sock]:
          if first:
            char = sock.recv(1)
            sock.rebuffer(char)
            if char == b"G":    # should be websocket connection
              self.handshake(sock)
              sock.websockets = True
              logger.info("Switching to websockets for socket %d", sock_no)
          if sock.websockets and first:
            pass
          else:
            if broker == None:
              connbuf = sock.recv(1)
              if connbuf == b'\x10': # connect packet
                while connbuf[-4:] != b"MQTT" and len(connbuf) < 10:
                  connbuf += sock.recv(1)
                connbuf += sock.recv(1)
                version = connbuf[-1]
                if version == 4:
                  broker = broker3
                elif version == 5:
                  broker = broker5
              sock.rebuffer(connbuf)
            if broker == None:
              terminate = True
            else:
              terminate = broker.handleRequest(sock)
          keptalive = False
          first = False
        elif (i, o, e) == ([], [], []):
          broker3.keepalive(sock)
          broker5.keepalive(sock)
          keptalive = True
        else:
          break
      except UnicodeDecodeError:
        logger.error("[MQTT-1.4.0-1] Unicode field encoding error")
        break
      except MQTTV3Exception as exc:
        logger.error(exc.args[0])
        break
      except MQTTV5Exception as exc:
        logger.error(exc.args[0])
        break
      except AssertionError as exc:
        if (len(exc.args) > 0):
          logger.error(exc.args[0])
        else:
          logger.error("")
        break
      except:
        logger.exception("WebSocketTCPHandler")
        break
    logger.info("Finishing communications for socket %d", sock_no)
    try:
      sock.shutdown(socket.SHUT_RDWR)
    except:
      pass
    sock.close()


class ThreadingTCPServer(socketserver.ThreadingMixIn,
                           socketserver.TCPServer):
  pass


def setBrokers(aBroker3, aBroker5):
  global broker3, broker5
  broker3 = aBroker3
  broker5 = aBroker5



def create(port, host="", TLS=False, serve_forever=False,
    cert_reqs=ssl.CERT_REQUIRED,
    ca_certs=None, certfile=None, keyfile=None, allow_non_sni_connections=True):
  global server
  logger.info("Starting TCP listener on address '%s' port %d %s", host, port, "with TLS support" if TLS else "")

  def snicallback(socket, text, context):
    rc = None # success
    if text:
      logger.debug("Connection SNI info "+text)
    elif allow_non_sni_connections == False:
      logger.info("Denying TLS connection because of no SNI information")
      rc = ssl.ALERT_DESCRIPTION_INTERNAL_ERROR # stop connection
    return rc

  bind_address = ""
  if host not in ["", "INADDR_ANY"]:
    bind_address = host
  server = ThreadingTCPServer((bind_address, port), WebSocketTCPHandler, False)
  if TLS:
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_SERVER)
    #try:
    #  context.set_ciphers('ALL:@SECLEVEL=1') # until we have seclevel 2 TLS config
    #except:
    #  pass # set_ciphers doesn't work on older Python versions
    try:
      context.sni_callback = snicallback
    except:
      logger.error("SNI callback not supported")
    if certfile:
      context.load_cert_chain(certfile, keyfile)
    if ca_certs:
      context.load_verify_locations(ca_certs)
    context.verify_mode = cert_reqs
    server.socket = context.wrap_socket(server.socket, server_side=True)
  server.request_queue_size = 50
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





