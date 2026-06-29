This is a Python MQTT and MQTT-SB server (broker) which implements 
MQTT 3.1.1, 5.0 and MQTT-SN 2.0.

It is used for testing some Paho MQTT and MQTT-SN clients:
  - https://github.com/eclipse-paho/paho.mqtt.c
  - https://github.com/eclipse-paho/paho.mqtt-sn.embedded-c

One of the reasons for using it in Paho Client testing is that it
is just as easy to use on Windows as MacOS and Linux, whereas Mosquitto
Windows installation is a bit awkward (certainly when I last looked).

To start the broker for Paho MQTT C client testing, use the following command:

  python startbroker.py -c localhost_testing.conf

This configuration supports MQTT 3.1.1 and 5.0 on the same ports, with some
configured for TLS. See the configuration file for details.

To start the broker for Paho MQTT-SN Embedded C client testing, use:

  python startbroker.py -c udp.conf

Source
------

The project is structured into sub-projects. The code to serialize and deserialize packets is:
  - mqtt/formats/MQTTV311, mqtt/formats/MQTTV5, mqtt/formats/MQTTSN2
The listeners communicate on the respective underlying transport protocols:
  - mqtt/brokers/listeners/ - TCP/IP, UDP and HTTP
And then the actual broker implementations themselves:
  - mqtt/brokers - V311, V5 and SN


TLS
---

A configuration file similar to that of Eclipse Mosquitto can be passed to startbroker:

  python3 startbroker.py -c client_testing.conf

so for example, a TCP listener with TLS support can be configured like this:

  listener 18884
  cafile tls_testing/keys/all-ca.crt
  certfile tls_testing/keys/server/server.crt
  keyfile tls_testing/keys/server/server.key
  require_certificate true

Client Tests
------------

Some old tests exist but haven't been updated for some time - see following.

MQTT Version 5
--------------

Start a broker:

  python3 startbroker.py

Run client tests:

  python3 client_test5.py

various options are available, individual tests can be run with:

  python3 client_test5.py Test.test_name  

As yet unimplemented features:

  https://github.com/eclipse/paho.mqtt.testing/issues

Sub-packages:

  mqtt/formats/MQTTV5 - packet serialization and deserialization
  mqtt/clients/V5 - test client implementation
  mqtt/brokers/V5 - test broker implementation

MQTT Version 3
--------------

Start a broker:

  python3 startbroker.py

Run client tests:

  python3 client_test.py
