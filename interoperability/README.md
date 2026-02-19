

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

