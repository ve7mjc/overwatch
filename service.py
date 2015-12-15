import subprocess
import threading
import Queue
import time
import os.path

# Service Handler
# Thread with associated process
class Service(threading.Thread):

	# careful, name is an actual thread class member
	
	def __init__(self, config):
		
		self.msgQ = Queue.Queue()
		self._stop = threading.Event()
		self.processRunning = False
		self.processStarted = False
		self.processTerminated = False
		threading.Thread.__init__(self)

		self.config = config
		
		# parse cmd to pull args delimited by spaces
		self.config["args"] = self.config["cmd"].split(" ")	
		
		# keep the noise down
		self.process = None
		self.watchmtime = None

	def startProcess(self):

		# check if service is enabled, otherwise
		# refuse to execute process
		if self.config["enabled"].lower() != "true":
			self.msgQ.put("{0}: service is disabled; refusing to run".format(self.config["name"]))
			return True

		# check that supplied cmd exists
		# isfile() does not return True on directories
		# like other methods
		if not os.path.isfile(self.config["args"][0]):
			self.msgQ.put("unable to launch {0}; {1} does not exist".format(self.config["name"], self.config["args"][0]))
			return True

		# log the start time of the process
		# Time.time() returns elapsed time in seconds as a float
		self.lastStartTime = time.time()

		# manage auto reload on file change
		try:
			if "reload_watch" in self.config:
				self.watchmtime = os.stat(self.config["reload_watch"]).st_mtime
		except:
			pass

		# launch process and pass handle
		try:
			self.process = subprocess.Popen(self.config["args"]) # , shell=True
			self.processRunning = True
			self.processStarted = True
			self.msgQ.put("{0}: running".format(self.config["name"]))
			return False
		except OSError:
			self.msgQ.put("unable to launch {0}; unknown error".format(self.config["name"]))
			return True
		except ValueError:
			self.msgQ.put("unable to launch {0}; invalid arguments supplied".format(self.config["name"]))
			return True
		except:
			self.msgQ.put("unable to launch {0}; unknown error".format(self.config["name"]))
			return True

	def stopProcess(self, force=False):
		# we are abruptly terminating the process
		# we could come up with a better way to politely ask first
		self.processStarted = False
		self.processTerminated = True
		
		# terminate process only if it has not already returned
		# especially if this is being called as part of a restart
		try:
			if self.process and (self.process.poll() is None):
				self.process.terminate()
		except OSError:
			pass

	def run(self):

		# calling this seperately now
		self.startProcess()

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