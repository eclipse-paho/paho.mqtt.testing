"""
*******************************************************************
  Copyright (c) 2013, 2014 IBM Corp.
 
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

# Trace MQTT traffic

from ..formats import MQTTV5 as MQTTV5

import socket, sys, select, socketserver, traceback, datetime, os

logging = True
myWindow = None


def timestamp():
  now = datetime.datetime.now()
  return now.strftime('%Y%m%d %H%M%S')+str(float("."+str(now.microsecond)))[1:]


class MyHandler(socketserver.StreamRequestHandler):

  def handle(self):
    if not hasattr(self, "ids"):
      self.ids = {}
    if not hasattr(self, "versions"):
      self.versions = {}
    inbuf = True
    i = o = e = None
    try:
      clients = self.request
      brokers = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      brokers.connect((brokerhost, brokerport))
      while inbuf is not None:
        (i, o, e) = select.select([clients, brokers], [], [])
        for s in i:
          if s == clients:
            inbuf = MQTTV5.getPacket(clients) # get one packet
            if inbuf is None:
              break
            try:
              packet = MQTTV5.unpackPacket(inbuf)
              if packet.fh.PacketType == MQTTV5.PacketTypes.PUBLISH and \
                  packet.topicName == "MQTTSAS topic" and \
                  packet.data == b"TERMINATE":
                print("Terminating client", self.ids[id(clients)])
                brokers.close()
                clients.close()
                break
              elif packet.fh.PacketType == MQTTV5.PacketTypes.CONNECT:
                self.ids[id(clients)] = packet.ClientIdentifier
                self.versions[id(clients)] = 3
              print(timestamp() , "C to S", self.ids[id(clients)], packet.json())
              print([hex(b) for b in inbuf])
              print(inbuf)
            except:
              traceback.print_exc()
            brokers.sendall(inbuf)       # pass it on
          elif s == brokers:
            inbuf = MQTTV5.getPacket(brokers) # get one packet
            if inbuf is None:
              break
            try:
              print(timestamp(), "S to C", self.ids[id(clients)], MQTTV5.unpackPacket(inbuf).json())
            except:
              traceback.print_exc()
            clients.sendall(inbuf)
      print(timestamp()+" client "+self.ids[id(clients)]+" connection closing")
    except:
      print(repr((i, o, e)), repr(inbuf))
      traceback.print_exc()
    if id(clients) in self.ids.keys():
      del self.ids[id(clients)]
    elif id(clients) in self.versions.keys():
      del self.versions[id(clients)]

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass

def run():
  global brokerhost, brokerport
  myhost = 'localhost'
  if len(sys.argv) > 1:
    brokerhost = sys.argv[1]
  else:
    brokerhost = 'localhost'

  if len(sys.argv) > 2:
    brokerport = int(sys.argv[2])
  else:
    brokerport = 1883

  if brokerhost == myhost:
    myport = brokerport + 1
  else:
    myport = 1883

  print("Listening on port", str(myport)+", broker on port", brokerport)
  s = ThreadingTCPServer(("", myport), MyHandler)
  s.serve_forever()

if __name__ == "__main__":
  run()
