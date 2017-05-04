#!/usr/bin/env python


################################################################################
#
#  Copyright (C) 2016, Carnegie Mellon University
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

from mqtt_client import MqttClient
from influxdb import InfluxDBClient
import logging
import requests
from requests.auth import HTTPBasicAuth
import json

#Constants
INFLUX_DATABASE = 'openchirp'

class InfluxdbMqttClient(MqttClient):

    def __init__(self, mqtt_server, rest_server, influx_server):
        self.influx_client = InfluxDBClient(influx_server.host, influx_server.port, influx_server.user, influx_server.password, INFLUX_DATABASE)
        self.rest_server = rest_server
        self.auth = HTTPBasicAuth(rest_server.user, rest_server.password)
        self.devices = dict()
        MqttClient.__init__(self, mqtt_server)

    def get_topics(self):
        topics = list()
        topics.append('/devices/+/transducer/#')
        return topics

    def process_messages(self):
        messages = self.grab_cached_messages()
        points = list()
        for msg in messages:
            words = msg.topic.split('/')
            if(len(words) < 4):
                logging.info("Skipping message: " + str(msg))
                continue

            # TODO add check to see if the payload is json ..
            device_id = words[2]
            transducer_name = words[4]
            device = self.get_device(device_id)
            self.get_or_create_transducer(device, transducer_name)
            if device is None:
                logging.info("Skipping message "+ str(msg)+" because could not find a device with id :" + device_id)
                continue
            point = {
            "measurement":  device_id+"_"+transducer_name,
            "fields": {
                "value": msg.payload
                }
            }
            points.append(point)

        self.influx_client.write_points( points, time_precision='s', batch_size = 5000)

    def get_device(self, device_id):
        if device_id not in self.devices:
            ##TODO: Catch exceptions and log them
            url = self.rest_server.host + "/device/"+device_id
            try:
                res = requests.get(url, self.auth)
                if(res.ok):
                    device = res.json()
                    self.devices[device_id] = device
                    return device
            except ConnectionError as ce:
                logging.warn("Connection error " + str(ce))
                return 
            except requests.exceptions.RequestException as re:
                logging.warn("RequestException " + str(re))
                return

        else:
            return self.devices[device_id]


    # This method creates the transducer if it does not exist
    def get_or_create_transducer(self, device, transducer_name):
        transducers = device["transducers"]
        for item in transducers:
            if(item["name"].lower() == transducer_name.lower()):
                return item

        logging.info("About to create transducer "+transducer_name + " on device "+device["id"])        
        url = self.rest_server.host + "/device/"+device["id"]+"/transducer"
        data = json.dumps({"name" : transducer_name})
        #TODO: catch exceptions and log them
        response = requests.post(url, data, self.auth)
        if(response.ok):
            logging.info("Transducer created ")
        else:
            logging.info("Error in creating transducer : HTTP code : "+ str(response.status_code) + " Content : "+ str(response.json()))



