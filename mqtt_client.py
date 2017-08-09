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

from threading import Lock
import ssl
import paho.mqtt.client as mqtt

import logging

class MqttClient():

    mqttMessageBuffer = list()
    mqtt_message_buffer_lock = Lock()

    def on_connect(self, client, userdata, flags, rc):
        logging.info('Connected to mqtt broker with result code '+str(rc))
        topics = self.get_topics()
        for topic in topics:
            logging.info('Subscibing to mqtt topic ' + topic)
            client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        self.mqttMessageBuffer.append(msg)

    def __init__(self, mqtt_server):
        logging.info('MqttClient : Init')
        self.client = mqtt.Client(client_id="mtss_service")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.username_pw_set(mqtt_server.user, mqtt_server.password)
        self.client.tls_set('/etc/ssl/certs/ca-certificates.crt', tls_version = ssl.PROTOCOL_TLSv1)
        self.client.connect(mqtt_server.host, int(mqtt_server.port), 60)
        self.client.loop_start()

    def grab_cached_messages(self):
        self.mqtt_message_buffer_lock.acquire()
        messages = self.mqttMessageBuffer
        self.mqttMessageBuffer = list()
        self.mqtt_message_buffer_lock.release()
        return messages


    def process_messages(self):
        # The derived class should override this method.
        pass

    def publish(self, msg):
        logging.info('MqttClient: publishing message '+ str(msg))
        #self.client.publish(msg["topic"], msg["message"], retain = True)
        self.client.publish(msg["topic"], msg["message"])

    def stop(self):
        logging.info('Disconnecting MqttClient')
        self.client.loop_stop()

    def get_topics(self):
        # The derived class should override this method and return a list of topics
        #that this mqtt client should subscribe to.
        pass
