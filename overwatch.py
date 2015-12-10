#!/usr/bin/python

# manager for entire project or system
# manages subprocess and their dependencies
# monitors their health and availability
# can be controlled remotely
#

import subprocess
import threading
import Queue

import logging
import time

import os, urlparse
import paho.mqtt.client as paho

import json
from pprint import pprint

import re
from fnmatch import fnmatch, fnmatchcase

logging.basicConfig(level = logging.DEBUG,
	format = '(%(threadName)-15s) %(message)s',
		)

# Load JSON configuration from disk
with open('config.json') as data_file:
    config = json.load(data_file)

services = config["services"]
print ("Loaded " + str(len(services)) + " services from config")


# Service Handler
# Thread with associated process
class Service(threading.Thread):

	# careful, name is an actual thread class member
	
	def __init__(self, config):
		self.msgQ = Queue.Queue()
		self.config = config
		self._stop = threading.Event()
		self.processRunning = False
		self.processStarted = False
		threading.Thread.__init__(self)
		# keep the noise down
		self.process = None 
		self.watchmtime = None

	def startProcess(self):

		# log the start time of the process
		# Time.time() returns elapsed time in seconds as a float
		self.lastStartTime = time.time()

		# manage auto reload on file change
		if "reload_watch" in self.config:
			self.watchmtime = os.stat(self.config["reload_watch"]).st_mtime

		# launch process and pass handle
		self.process = subprocess.Popen(self.config["cmd"], shell=True)
		
		self.processRunning = True
		self.processStarted = True
		self.msgQ.put("running " + self.config["name"])

	def stopProcess(self):
		# we are abruptly terminating the process
		# we could come up with a better way to politely ask first
		self.processRunning = False
		self.processStarted = False
		
		# terminate process only if it has not already returned
		# especially if this is being called as part of a restart
		try:
			if self.process and not self.process.poll():
				self.process.terminate()
		except OSError:
			pass

	def run(self):

		# calling this seperately now
		#self.startProcess()

		# detect that we have began execution of the process
		# but differentiate that it has not yet started
		# .. we could certainly block until it has began
		# or set a timer for some time

		while not self.stopped():

			# process.poll() returns None (False) when not exited
			# returns error code elsewise
			# since scripts ofter return (int)0, we must be specific
			# to look for anything BUT (NoneType)None
			if self.process and not self.process.poll() == None:
				# we have an exit
				self.processRunning = False
				#elf.msgQ.put(self.config["name"] + " exited unexpectedly")

			# waste cycles until the next loop
			time.sleep(0.05)

		return

	# stop thread, not process directly
	def stop(self):
		self.stopProcess()
		self._stop.set()

	# is the thread intended to be stopped?
	def stopped(self):
		return self._stop.isSet()

	def restartProcess(self):
		self.msgQ.put("restarting " + self.config["name"])
		self.stopProcess()
		self.startProcess()

## MQTT

# Define event callbacks
def on_connect(mosq, obj, rc):
	#mqttc.publish("logs/" + clientName + "/notice", "started overwatch")
	pass

def on_message(mosq, obj, msg):

	if (fnmatch(msg.topic, 'clients/+/status') == True):
		print("brief status")
		# client status received
		pass

	if (fnmatch(msg.topic, 'clients/+/fullstatus') == True):
		print("full status")
		pass

	print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_publish(mosq, obj, mid):
    pass
    #print("mid: " + str(mid))

def on_subscribe(mosq, obj, mid, granted_qos):
    pass
    #print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mosq, obj, level, string):
    print(string)

def mqttcLog(message, level = "debug"):
	mqttc.publish("logs/" + config["mqttClientName"] + "/" + level.lower(), message)

mqttc = paho.Client(config["mqttClientName"])
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

# Parse CLOUDMQTT_URL (or fallback to localhost)
url_str = os.environ.get('CLOUDMQTT_URL', 'mqtt://' + config["mqttRemoteHost"] + ":" + str(config["mqttRemotePort"]))
url = urlparse.urlparse(url_str)

# Initialize and begin MQTT
#mqttc.username_pw_set(url.username, url.password)
mqttc.will_set('clients/' + config["mqttClientName"], 'offline', 0, False)
mqttc.connect(url.hostname, url.port)

# Start up modules
serviceThreads = [] # for tracking
for service in services:
	t = Service(service)
	serviceThreads.append(t)
	t.start()

	# start associated process if service is enabled
	# per config file
	if service["enabled"].lower() == "true":
		t.startProcess()

# we are now healthy
mqttc.publish("clients/" + config["mqttClientName"] + "/status", 'healthy')

mqttc.subscribe("clients/#") # what qos do we want?

# Update retained status json topic in MQTT
#
status = {}
status["services"] = {}
def updateStatus():
	for service in serviceThreads:
		pass

def checkServices():
	# remember, some services may be disabled and not
	# intended to be running
	for service in serviceThreads:
	
		# process thread born messages to 
		# pass to logging facility
		if not service.msgQ.empty():
			msg = service.msgQ.get(False)
			service.msgQ.task_done()
			mqttcLog(msg)

		# if process should be running but is not
		# lets restart it while being careful not to flap
		if service.processStarted and not service.processRunning:
			if ((time.time() - service.lastStartTime) > 2):
				service.restartProcess()

		# manage auto reload on file change
		# we need to watch this rather infrequently
		if not service.watchmtime == None:
			if os.stat(service.config["reload_watch"]).st_mtime > service.watchmtime:
				#mqttcLog("Detected modification in watched file " + service.config["reload_watch"] + ".  Restarting " + service.config["name"], "notice")
				service.restartProcess()


# blocking loop with a KeyboardInterrupt exit
# not sure why we need to maintain a variable to kill the while loop
exiting = False
try:
	while not exiting:

		checkServices()

		# run mqttc loop
		mqttc.loop()

		# sleep for 50ms
		# sleeping for 200ms until we can come up with dividers
		time.sleep(0.1)

except (KeyboardInterrupt, SystemExit):

	exiting = True
	print("Received keyboard interrupt.  Shutting down..")

	for thread in serviceThreads:
		thread.stop()

