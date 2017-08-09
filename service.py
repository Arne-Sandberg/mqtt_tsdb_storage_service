#!/usr/bin/python3 


################################################################################
#
#  Copyright (C) 2017, Carnegie Mellon University
#  All rights reserved.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, version 2.0 of the License.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#  Contributing Authors (specific to this file):
#
#  Khushboo Bhatia               khush[at]cmu[dot]edu
#
################################################################################

import logging
import sys
import signal

import common
from influxdb_mqtt_client import InfluxdbMqttClient
from collections import namedtuple

from twisted.internet import task
from twisted.internet import reactor

#Data Holders
Server = namedtuple('Server', ['host', 'port','user','password'])

#Define stop action
def signal_handler(signal, frame):
    logging.info('Received kill signal ..Stopping service daemon')
    if "service" in globals():
        reactor.stop()
        service.stop()

signal.signal(signal.SIGINT, signal_handler)

class ServiceDaemon():
    def run(self):
        conf = common.parse_arguments()
        logging.basicConfig(filename=conf['log_file'],level=logging.INFO)
        mqtt_server = Server(conf['mqtt_broker'], '1883', conf['service_id'], conf['password'])
        influxdb_server = Server(conf['influxdb_host'], conf['influxdb_port'], conf['influxdb_user'], conf['influxdb_password'])
        rest_server = Server(conf['rest_url'], '', conf['service_id'], conf['password'])
        global service
        service =  InfluxdbMqttClient(conf['service_id'], mqtt_server, rest_server, influxdb_server)

        loopTask1 = task.LoopingCall(service.process_messages)
        loopDeferred1 = loopTask1.start(1.0) #process messages every second
        loopDeferred1.addErrback(self.handle_error)

        loopTask2 = task.LoopingCall(service.publish_status)
        loopDeferred2 = loopTask2.start(600.0) #Publish status every 10 minutes 
        loopDeferred2.addErrback(self.handle_error)

        reactor.run() #Keeps the process running forever

    def handle_error(self, failure):
        logging.error(failure.getBriefTraceback())
        reactor.stop()
        service.stop()

if __name__ == '__main__':
    daemon = ServiceDaemon()
    daemon.run()
