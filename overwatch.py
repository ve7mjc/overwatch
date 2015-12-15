#!/usr/bin/python

# manager for entire project or system
# manages subprocess and their dependencies
# monitors their health and availability
# can be controlled remotely
#

import sys, os
import time
import urlparse
import paho.mqtt.client as paho
import json
from fnmatch import fnmatch
import re

import service

class Overwatch(object):
	
	def __init__(self):

		self.config = {}
		self.services = []
		self.configured = False
		self.status = {}
		self.lastStatus = None
		
	def loadConfig(self, config_file="config.json"):

		# Load JSON configuration from disk
		with open(config_file) as data_file:
		    self.config = json.load(data_file)
		self.services = self.config["services"]
		
		self.configured = True
		
		self.connectToBroker()

	def connectToBroker(self):
		
		# Create and initialize MQTT Client
		self.mqttc = paho.Client(self.config["mqttClientName"])
		self.mqttc.on_message = self.on_MqttMessage
		self.mqttc.on_connect = self.on_MqttConnect
		self.mqttc.on_publish = self.on_MqttPublish
		self.mqttc.on_subscribe = self.on_MqttSubscribe

		if self.configured:
			# Parse CLOUDMQTT_URL (or fallback to localhost)
			url_str = os.environ.get('CLOUDMQTT_URL', 'mqtt://' + self.config["mqttRemoteHost"] + ":" + str(self.config["mqttRemotePort"]))
			url = urlparse.urlparse(url_str)
			
			# Initialize and begin MQTT
			#mqttc.username_pw_set(url.username, url.password)
			self.mqttc.will_set('clients/' + self.config["mqttClientName"], 'offline', 0, False)
			self.mqttc.connect(url.hostname, url.port)
			return False
		else:
			print("MQTT settings are not configured; unable to connect")
			sys.exit()

	# Define event callbacks
	def on_MqttConnect(self, something, mosq, obj, rc):
		self.mqttc.publish("clients/" + self.config["mqttClientName"] + "/status", 'healthy')
		self.mqttc.subscribe("clients/#") # what qos do we want?
	
	def on_MqttMessage(self, mosq, obj, msg):
	
		if (fnmatch(msg.topic, 'clients/+/status') == True):
			print("brief status")
			# client status received
			pass
	
		if (fnmatch(msg.topic, 'clients/+/fullstatus') == True):
			print("full status")
			pass
	
		#print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
	
	def on_MqttPublish(self, mosq, obj, mid):
	    pass
	
	def on_MqttSubscribe(self, mosq, obj, mid, granted_qos):
	    pass

	def mqttcLog(self, message, level = "debug"):
		self.mqttc.publish("logs/" + self.config["mqttClientName"] + "/" + level.lower(), message)
	
	def startServices(self):
		
		print("Overwatch: Starting {0} services".format(len(self.services)))

		# Start up modules
		self.serviceThreads = [] # for tracking
		for s in self.services:
			t = service.Service(s)
			t.start()
			self.serviceThreads.append(t)

	def shutdown(self):
		self.stopServices()

	def stopServices(self):
		for thread in self.serviceThreads:
			thread.stop()
			
	def checkServices(self):
		# remember, some services may be disabled and not
		# intended to be running
		for s in self.serviceThreads:
		
			# process thread born messages to 
			# pass to logging facility
			if not s.msgQ.empty():
				msg = s.msgQ.get(False)
				s.msgQ.task_done()
				self.mqttcLog(msg)
	
			# if process should be running but is not
			# lets restart it while being careful not to flap
			if s.processStarted and not s.processRunning:
				if ((time.time() - s.lastStartTime) > 2):
					s.restartProcess()
	
			# manage auto reload on file change
			# we need to watch this rather infrequently
			if not s.watchmtime == None:
				if os.stat(s.config["reload_watch"]).st_mtime > s.watchmtime:
					self.mqttcLog("{0}: detected modification in watched file ({1}); Restarting".format(s.config["name"],s.config["reload_watch"]), "notice")
					s.restartProcess()
	
	def publishServiceStatus(self):
		
		serviceStatuses = []
		for ser in self.serviceThreads:
			serviceStatus = {}
			serviceStatus["name"] = ser.config["name"]
			serviceStatus["enabled"] = ser.config["enabled"]
			serviceStatuses.append(serviceStatus)
		
		if (self.lastStatus is None) or self.lastStatus["services"] != serviceStatuses:
			self.status["services"] = serviceStatuses
			self.lastStatus = self.status
			self.mqttc.publish("overwatch/status/services", json.dumps(serviceStatuses))
		

	def run(self):
		try:
			while True:
		
				# check status of services
				# todo, these are already threads
				# they could be checking themselves
				self.checkServices()
				
				self.publishServiceStatus()
		
				# service mqttc network loop
				# timeout 10 mS as it is blocking
				self.mqttc.loop(0.01)
		
		except (KeyboardInterrupt, SystemExit):
		
			print("Received keyboard interrupt in Overwatch::run()\r\nShutting down..")
			self.shutdown()
			return False

def main():
	
	overwatch = Overwatch()
	overwatch.loadConfig()
	overwatch.startServices()
	overwatch.run()
	
	return False

if __name__ == "__main__":
    sys.exit(main())