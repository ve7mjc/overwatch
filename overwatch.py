#!/usr/bin/python3

# manager for entire project or system
# manages subprocess and their dependencies
# monitors their health and availability
# can be controlled remotely
#

import sys, os
import time
import paho.mqtt.client as paho
import json
from fnmatch import fnmatch
import re
import queue

import service

class Overwatch(object):
	
	def __init__(self):
		self.mqttMessageQueue = queue.Queue()
		self.config = {}
		self.services = []
		self.configured = False
		self.status = {}
		self.lastStatus = None
		
		self.loadConfig()
		self.connectToBroker()
		self.startServices()
		self.run()
		
	def loadConfig(self, config_file="config.json"):
		# Load JSON configuration from disk
		with open(config_file) as data_file:
		    self.config = json.load(data_file)
		
		self.configured = True

	def connectToBroker(self):
		# Create and initialize MQTT Client
		self.mqttc = paho.Client(self.config["mqttClientName"])
		self.mqttc.on_message = self.on_MqttMessage
		self.mqttc.on_connect = self.on_MqttConnect
		self.mqttc.on_publish = self.on_MqttPublish
		self.mqttc.on_subscribe = self.on_MqttSubscribe

		if self.configured:	
			# Initialize and begin MQTT
			#mqttc.username_pw_set(url.username, url.password)
			self.mqttc.will_set('clients/' + self.config["mqttClientName"], 'offline', 0, False)
			self.mqttc.connect(self.config["mqttRemoteHost"], self.config["mqttRemotePort"])
			self.mqttc.loop_start()
			return False
		else:
			print("MQTT settings are not configured; unable to connect")
			sys.exit()

	# MQTT Client has successfully connected
	# do testing to ensure that this cross-thread call
	# is safe
	def on_MqttConnect(self, something, mosq, obj, rc):
		self.mqttc.publish("clients/" + self.config["mqttClientName"] + "/status", 'healthy')
		self.mqttc.subscribe("clients/#")
		self.mqttc.subscribe("clients/{0}/cmd/#".format(self.config["mqttClientName"]))
	
	# MQTT Client has received a message
	def on_MqttMessage(self, mosq, obj, msg):
		topic = msg.topic
		payload = msg.payload.decode('utf-8')
		
		# place message as tuple in a thread-safe queue 
		# as this was called from the mqtt loop thread
		self.mqttMessageQueue.put((topic,payload))
		
	def processMqttMessages(self):

		while not self.mqttMessageQueue.empty():			
			(topic,payload) = self.mqttMessageQueue.get()

			if topic == "clients/{0}/cmd".format(self.config["mqttClientName"]):
				try: command = json.loads(payload)
				except: self.writeLog("unable to process json from command: {0}".format(payload))
					
				if command['command'] == 'service-restart':
					pass
					
				if command['command'] == 'shutdown':
					print("Received request to shutdown from MQTT")
					self.shutdown()
					
				if command['command'] == 'restart':
					pass # stub
					
					
			if (fnmatch(topic, 'clients/+/status') == True):
				print("brief status")
				# client status received
				pass
		
			if (fnmatch(topic, 'clients/+/fullstatus') == True):
				print("full status")
				pass
	
	def on_MqttPublish(self, mosq, obj, mid):
	    pass
	
	def on_MqttSubscribe(self, mosq, obj, mid, granted_qos):
	    pass

	def writeLog(self, message, level = "debug"):
		self.mqttc.publish("logs/" + self.config["mqttClientName"] + "/" + level.lower(), message)
	
	def startService(self, name):
		found = False
		for s in self.services:
			if s.name == name:
				found = True
				self.writeLog("starting service {0}".format(name))
				s.startProcess()
		
		if not found:
			self.writeLog("requested service does not exist to start {0}".format(name))
		
	def stopService(self, name):
		found = False
		for s in self.services:
			if s.name == name:
				found = True
				self.writeLog("stopping service {0}".format(name))
				s.stopProcess()
		
		if not found:
			self.writeLog("requested service does not exist to stop {0}".format(name))
		
	def restartService(self, name):
		self.stopService(name)
		self.startService(name)
	
	def startServices(self):
		
		print("Starting {0} services".format(len(self.config["services"])))

		# Start up modules
		self.services.clear()
		for s in self.config["services"]:
			# Create new service and pass config dictionary
			t = service.Service(s)
			self.services.append(t)
			t.start()

	# Shut down entire application
	def shutdown(self):
		print("Shutting down..")
		self.stopServices()
		self.mqttc.loop_stop()
		sys.exit(True)

	def stopServices(self):
		for s in self.services:
			s.stop()
			
	def checkServices(self):
		# remember, some services may be disabled and not
		# intended to be running
		for s in self.services:

			# process thread born messages to 
			# pass to logging facility
			if not s.msgQ.empty():
				msg = s.msgQ.get(False)
				s.msgQ.task_done()
				self.writeLog(msg)
	
			# if process should be running but is not
			# lets restart it while being careful not to flap
			if s.processStarted and not s.processRunning:
				if ((time.time() - s.lastStartTime) > 2):
					s.restartProcess()
	
			# manage auto reload on file change
			# we need to watch this rather infrequently
			if not s.watchmtime == None:
				if os.stat(s.config["reload_watch"]).st_mtime > s.watchmtime:
					self.writeLog("{0}: detected modification in watched file ({1}); Restarting".format(s.config["name"],s.config["reload_watch"]), "notice")
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
		
				self.processMqttMessages()
				# check status of services
				# todo, these are already threads
				# they could be checking themselves
				self.checkServices()
				
				#self.publishServiceStatus()
		
				# not sure we need to do anything
				# faster than 10 Hz
				time.sleep(0.1)
		
		except (KeyboardInterrupt, SystemExit):
			print("Received keyboard interrupt in Overwatch")
			self.shutdown()

overwatch = Overwatch()