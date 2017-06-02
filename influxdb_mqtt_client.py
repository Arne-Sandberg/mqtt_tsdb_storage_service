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
        topics.append('openchirp/devices/+/transducer/#')
        return topics

    def process_messages(self):
        messages = self.grab_cached_messages()
        points = list()
        for msg in messages:
            words = msg.topic.split('/')
            if(len(words) < 5):
                logging.info("Skipping message: " + str(msg.topic) +" : "+str(msg.payload))
                continue

            # TODO add check to see if the payload is json ..
            device_id = words[2]
            transducer_name = words[4]
            device = self.get_device(device_id)
            if device is None:
                logging.info("Skipping message "+ str(msg)+" because could not find a device with id :" + device_id)
                continue
            transducer_name = transducer_name.lower()
            self.get_or_create_transducer(device, transducer_name)
            point = {
            "measurement":  device_id+"_"+transducer_name,
            "fields": {
                "value": msg.payload
                }
            }
            points.append(point)
        try:
            self.influx_client.write_points( points, time_precision='s', batch_size = 5000)
        except influxdb.exceptions.InfluxDBClientError as ie:
            logging.exception("Error in writing to influxdb")
            logging.exception(ie)

    def get_device(self, device_id):
        if device_id not in self.devices:
            url = str(self.rest_server.host + "/device/"+ device_id)
            logging.info("Connecting  to "+url)
            try:
                res = requests.get(url, auth = self.auth)
                if(res.ok):
                    device = res.json()
                    self.devices[device_id] = device
                    return device
                else:
                    logging.info("Error response "+ str(res.status_code))
            except ConnectionError as ce:
                logging.exception("Connection error : " + url)
                logging.exception(ce)
                return 
            except requests.exceptions.RequestException as re:
                logging.exception("RequestException :" + url)
                logging.exception(re)
                return

        else:
            return self.devices[device_id]


    # This method creates the transducer if it does not exist
    def get_or_create_transducer(self, device, transducer_name):
        transducers = device["transducers"]
        for item in transducers:
            if(item["name"].lower() == transducer_name):
                return item
        # Reload transducers before creating one

        url = self.rest_server.host + "/device/"+device["id"]+"/transducer"
        try:
            response = requests.get(url, auth = self.auth)
            if(response.ok):
                transducers = response.json()
                for item in transducers:
                    if(item["name"].lower() == transducer_name):
                        return item

            else:
                logging.info("Error in getting transducers "+ str(response.status_code))
                return
        except ConnectionError as ce:
            logging.exception("Connection error :" +url)
            logging.exception(ce)
            return
        except requests.exceptions.RequestException as re:
            logging.exception("RequestsException :" +url)
            logging.exception(re)
            return

        logging.info("About to create transducer "+transducer_name +" on device "+ device["id"])    
        data = {}
        data['name'] = transducer_name;
        data['properties'] = {"created_by":"OpenChirp Influxdb Storage service"} 
        try:
            response = requests.post(url, data = data, auth = self.auth)
            if(response.ok):
                logging.info("Transducer created ")
            else:
                logging.info("Error in creating transducer : HTTP code : "+ str(response.status_code) + " Content : "+ str(response.json()))
        except ConnectionError as ce:
                logging.exception("Connection error : " + url)
                logging.exception(ce)
                return 
        except requests.exceptions.RequestException as re:
                logging.exception("RequestException : "+ url)
                logging.exception(re)
                return


