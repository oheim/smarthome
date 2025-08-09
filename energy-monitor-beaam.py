#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2025 Oliver Heimlich <oheim@posteo.de>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This program has three functions:

  (1) Periodically poll the Neoom Beaam API and publish all values to InfluxDB
  
  (2) Receive information about PV production from a shelly device (Balkonkraftwerk)
      and publish PV production to InfluxDB
  
  (3) Receive events from Shelly BLE devices via MQTT and publish them to InfluxDB

@author: Oliver Heimlich <oheim@posteo.de>
"""

import logging
import dotenv
import timeloop
import json
import datetime
import asyncio
import requests
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from modules import mqttclient

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Beaam.env")

background = timeloop.Timeloop()

#def find_value_by_key(data, target_key):
#    for item in data:
#        if item['key'] == target_key:
#            return item['value']
#    return None

def call_api(path):
    global config
    
    headers = {
        'Authorization': 'Bearer ' + config['BEAAM_API_TOKEN']
    }
    response = requests.get(config['BEAAM_URL'] + path, headers=headers)
    response.raise_for_status()
    return response.json()

def load_site():
    global site
    
    site = call_api('api/v1/site/configuration')
load_site()

average_power = 0.0
def forward_data_points():
    global average_power
    global site
    global config
    global influx_api
    
    site_state = call_api('api/v1/site/state')
    for data_point in site_state['energyFlow']['states']:
        data_point_id = data_point['dataPointId']
        unit_of_measure = site['energyFlow']['dataPoints'][data_point_id]['unitOfMeasure']
        key = data_point['key']
        value = data_point['value']
        if value is not None and site['energyFlow']['dataPoints'][data_point_id]['dataType'] == 'NUMBER':
            value = float(value)
        
        topic = 'beaam/SITE/' + unit_of_measure + '/' + key
        
        point = (
            Point("SITE")
            .tag("unit", unit_of_measure)
            .field(key, value)
        )
        influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)
        
        if key == 'POWER_PRODUCTION':
            smoothing_factor = 0.125
            average_power += smoothing_factor * (value - average_power)
            
            mqttclient.publish(topic + '_AVERAGE_SMOOTH', "%d" % (average_power))
    
    for thing_id in site['things']:
        thing_type = site['things'][thing_id]['type']

        thing_states = call_api('api/v1/things/' + thing_id + '/states')
        for data_point in thing_states['states']:
            data_point_id = data_point['dataPointId']
            unit_of_measure = site['things'][thing_id]['dataPoints'][data_point_id]['unitOfMeasure']
            key = data_point['key']
            value = data_point['value']
            if value is not None and site['things'][thing_id]['dataPoints'][data_point_id]['dataType'] == 'NUMBER':
                value = float(value)
            
            
            if isinstance(value, (str, int, float, bool)):
                point = (
                    Point(thing_type)
                    .tag("unit", unit_of_measure)
                    .tag("thing_id", thing_id)
                    .field(key, value)
                )
                influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

@background.job(interval = datetime.timedelta(seconds=20))
def update_measurement():
    try:
        forward_data_points()
    except Exception as err:
        logging.exception('Failed to update measurement')
        return

def on_message(client, userdata, msg):
    if msg.topic == 'shellies/ble':
        on_ble_event(json.loads(msg.payload))
    else:
        on_bk_power(float(msg.payload))

def on_ble_event(payload):
    global influx_api
    
    if 'addr' in payload and 'Temperature' in payload and 'Humidity' in payload:
        point = (
            Point('SHELLY')
            .tag('addr', payload['addr'])
            .field('Temperature', float(payload['Temperature']))
            .field('Humidity', int(payload['Humidity']))
        )
    
        influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

def on_bk_power(power):
    global influx_api

    power = int(power)
 
    point = (
        Point("BK_POWER")
        .tag("unit", "W")
        .field("AVERAGE_PRODUCED_LAST_MINUTE", power)
    )
    influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

loop = None
influx_api = None
async def main():
    global config
    global loop
    global influx_api

    loop = asyncio.get_running_loop()

    influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
    influx_api = influx_client.write_api(write_options=SYNCHRONOUS)

    mqttclient.connect(server=config['MQTT_SERVER'], user=config['MQTT_USER'], password=config['MQTT_PASSWORD'], message_callback=on_message)
    mqttclient.subscribe('bk-power/status/switch:0/power/average')
    mqttclient.subscribe('shellies/ble')
    
    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()
        mqttclient.disconnect()

asyncio.run(main())
