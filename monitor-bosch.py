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

"""Poll the local Bosch Smarthome Controller REST API periodically
and publish temperature levels and boiler state to InfluxDB

@author: Oliver Heimlich <oheim@posteo.de>
"""


import dotenv
import influxdb_client
import requests
import timeloop
import datetime
import asyncio
import logging

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import urllib3

urllib3.disable_warnings()
config = dotenv.dotenv_values("Bosch-Smarthome.env")

background = timeloop.Timeloop()

def call_api(path):
    global config
    
    headers = {
        'api-version': '3.2',
        'accept': 'application/json'
    }

    response = requests.get(config['BOSCH_SMARTHOME_URL'] + path, headers=headers, cert=(config['BOSCH_SMARTHOME_CLIENT_CERT'], config['BOSCH_SMARTHOME_CLIENT_KEY']), verify=False)
    response.raise_for_status()
    return response.json()
    
devices = []
for device in call_api("devices"):
    devices.append(device)

rooms = {}
for room in call_api("rooms"):
    rooms[room['id']] = room

@background.job(interval = datetime.timedelta(minutes = 1))
def update_measurement():
    try:
        forward_data_points()
    except Exception as err:
        logging.exception('Failed to update measurement')
        return

def forward_data_points():
    global devices
    global rooms
    global influx_api
    
    for device in devices:
        if device['deviceModel'] == 'ROOM_CLIMATE_CONTROL':
            room = rooms[device['roomId']]
            temperature_level = call_api('devices/' + device['id'] + '/services/TemperatureLevel/state')
            
            point = (
                Point("BOSCH")
                .tag("unit", "°C")
                .field("Temperaturen/" + room['name'], float(temperature_level['temperature']))
            )
        elif device['deviceModel'] == 'BOILER':
            boiler = call_api('devices/' + device['id'] + '/services/BoilerHeating/state')
            point = (
                Point("BOSCH")
                .field("Boiler", boiler['heatDemand'] == 'HEAT_DEMAND')
            )
        else:
            continue
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

    forward_data_points()
    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
