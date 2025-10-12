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

"""This program has the following functions:

  (1) Receive information about PV production from a shelly device (Balkonkraftwerk)
  
  (2) Receive information about energy consumption from shelly devices
  
  (3) Receive events from Shelly BLE devices
  
  All information is received via MQTT and published via InfluxDB.

@author: Oliver Heimlich <oheim@posteo.de>
"""

import logging
import dotenv
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

config = dotenv.dotenv_values("Shelly.env")

def on_message(client, userdata, msg):
    if msg.topic == 'shellies/ble':
        on_ble_event(json.loads(msg.payload))
    elif msg.topic.startswith('shellies/power/average/'):
        on_power(msg.topic.split('/')[-1], float(msg.payload))
    elif msg.topic.startswith('shellies/energy/total/'):
        on_energy(msg.topic.split('/')[-1], float(msg.payload))

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

def on_power(device, power):
    global influx_api

    point = (
        Point("SHELLY")
        .tag("device", device)
        .tag("unit", "W")
        .field("averagePowerLastMinute", power)
    )
    influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

def on_energy(device, totalEnergy):
    global influx_api
    
    if device == "null":
        return
        
    point = (
        Point('SHELLY')
        .tag('device', device)
        .tag('unit', 'Wh')
        .field('totalEnergy', totalEnergy)
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
    mqttclient.subscribe('shellies/power/average/bk-power')
    mqttclient.subscribe('shellies/energy/total/#')
    mqttclient.subscribe('shellies/ble')
    
    try:
        while True:
            await asyncio.sleep(60)
    finally:
        mqttclient.disconnect()

asyncio.run(main())
