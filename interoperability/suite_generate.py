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

"""

1. Start the model broker.  Clean state.

2. Run the spec, one step at a time until conformance statement hit.

3. Reduce sequence to shortest.

4. Store.


Repeat until all conformance statements hit.

Do store tests that reach a conformance statement in a different way.

"""

import os, shutil, threading, time, logging, logging.handlers, queue, sys, traceback

import mqtt, MQTTV311_spec

class Brokers(threading.Thread):

  def __init__(self):
    threading.Thread.__init__(self)

  def run(self):
    mqtt.broker.run()
    while not mqtt.broker.server:
      time.sleep(.1)
    time.sleep(1)

  def stop(self):
    mqtt.broker.stop()
    while mqtt.broker.server:
      time.sleep(.1) 
    time.sleep(1)

  def reinitialize(self):
    mqtt.broker.reinitialize()

  def measure(self):
    return mqtt.broker.measure()

# Attach to the broker log, so we can get its messages
broker_log = queue.Queue()
qh = logging.handlers.QueueHandler(broker_log)
formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
qh.setFormatter(formatter)
qh.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.ERROR)

broker_logger = logging.getLogger('MQTT broker')
broker_logger.addHandler(qh)
broker_logger.propagate = False
#broker_logger.addHandler(ch)  # prints broker log messages to stdout, for debugging or interest

# Attach to the mbt log, so we can get its messages
mbt_log = queue.Queue()
qh = logging.handlers.QueueHandler(mbt_log)
formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
qh.setFormatter(formatter)
qh.setLevel(logging.INFO)
mbt_logger = logging.getLogger('mbt')
mbt_logger.addHandler(qh)
mbt_logger.propagate = False

logger = logging.getLogger('suite_generate')
logger.setLevel(logging.INFO)
#formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
logger.addHandler(ch)
logger.propagate = False


def create():
	global logger, broker_log
	conformances = set([])
	restart = False
	file_lines = []
	while not restart:
		logger.debug("stepping")
		restart = MQTTV311_spec.mbt.step()
		logger.debug("stepped")
		try:
			while (True):
				data = mbt_log.get_nowait().getMessage() # throws exception when no message
				file_lines.append(data + "\n" if data[-1] != "\n" else data) 
		except:
			pass	
		data = broker_log.get().getMessage()
		logger.debug("data %s", data)
		while data and data.find("Waiting for request") == -1 and data.find("Finishing communications") == -1:
			if data.find("[MQTT") != -1:
				logger.debug("Conformance statement %s", data)
				if data[-1] != "\n":
					data += "\n"
				if data not in conformances:
					file_lines.append(data)
					conformances.add(data)
			data = broker_log.get().getMessage()
			logger.debug("data %s", data)
		#if input("--->") == "q":
		#		return
	return conformances, file_lines


if __name__ == "__main__":
	#try:
	os.system("rm -rf tests")
	#except:
	#	pass
	os.mkdir("tests")
	test_no = 0
	logger.info("Generation starting")

	broker = Brokers() 
	broker.start()
	
	coverage = 0
	while test_no < 20:
		test_no += 1
		conformance_statements, file_lines = create()
		logger.info("Test %d created", test_no)

		# now tests/test.%d has the test
		filename = "tests/test.log.%d" % (test_no,)
		#infile = open(filename)
		#lines = infile.readlines()
		#infile.close()
		outfile = open(filename, "w")
		#outfile.writelines(list(conformance_statements) + lines)
		outfile.writelines(file_lines)
		outfile.close()
	
		#shorten()
		#store()
		results = broker.measure()
		last_coverage = coverage
		coverage = int(results[0].split()[-1][:-1])
		print("Test", test_no, results[0], results[1])
		broker.reinitialize()

	final_results = broker.measure()
	broker.stop()

	logger.info("Generation complete")
	for curline in final_results:
		logger.info(curline)

	# Without the following, background threads cause the process not to stop
	for t in threading.enumerate():
		t._stop()

