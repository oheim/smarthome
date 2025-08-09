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

"""Poll the local Novelan WPR-NET Heizungs- und Wärmepumpenregler via Websocket
and publish metrics to InfluxDB

@author: Oliver Heimlich <oheim@posteo.de>
"""

import asyncio
import websockets
import xmltodict
import re
import dotenv
import time
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

config = dotenv.dotenv_values("Heizung.env")

def parse_value_with_unit(s):
    if s == "Ein":
        return True, None
    
    if s == "Aus":
        return False, None
        
    match = re.match(r'^([-+]?\d*\.?\d+)\s*([^\d\s]+)?$', s)
    if match:
        value = float(match.group(1))
        unit = match.group(2)
        return value, unit
    
    match = re.match(r'^(\d{2}):(\d{2}):(\d{2})$', s)
    if match:
        h = int(match.group(1))
        m = int(match.group(2))
        s = int(match.group(3))
        total_minutes = h * 60 + m + s / 60.0
        return total_minutes, "min"

    match = re.match(r'^(\d{2}):(\d{2})$', s)
    if match:
        h = int(match.group(1))
        m = int(match.group(2))
        total_minutes = h * 60 + m
        return total_minutes, "min"
        
    return str(s), None

def publish(point_id, value, unit):
    global names_by_id
    global influx_api
    
    point = (
        Point("HEIZUNG")
        .field(names_by_id[point_id], value)
    )
    if unit is not None:
        point.tag("unit", unit)
    
    influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

async def main():
    global config
    global names_by_id
    
    async with websockets.connect(config['HEIZUNG_URL'], subprotocols = ["Lux_WS"], ping_interval = None) as websocket:
        await websocket.send("LOGIN;" + config['HEIZUNG_PASSWORD'])
        response = await websocket.recv()
        navigation = xmltodict.parse(response)
        information = navigation['Navigation']['item'][0]
        await websocket.send('GET;' + information['@id'])
        response = await websocket.recv()
        content = xmltodict.parse(response)['Content']
        
        names_by_id = {}
        for category in content['item']:
            category_name = category['name']
            if category_name in ('Abschaltungen', 'Fehlerspeicher', 'GLT'):
                continue
            
            for data_point in category['item']:
                point_id = data_point['@id']
                name = data_point['name']
                value = data_point['value']
                value, unit = parse_value_with_unit(value)
                
                if unit == "bar":
                    # workaround for duplicate name
                    name = name + " Druck"
                
                names_by_id[point_id] = category_name + "/" + name
                
                publish(point_id, value, unit)

        while True:
            time.sleep(20)
            await websocket.send('REFRESH')
            response = await websocket.recv()
            values = xmltodict.parse(response)['values']
            for data_point in values['item']:
                point_id = data_point['@id']
                if not (point_id in names_by_id):
                    continue
                value = data_point['value']
                value, unit = parse_value_with_unit(value)

                publish(point_id, value, unit)
            
                    
influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
influx_api = influx_client.write_api(write_options=SYNCHRONOUS)
            
asyncio.run(main())
