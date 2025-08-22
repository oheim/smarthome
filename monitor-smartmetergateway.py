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

"""Poll the local HAN interface of the smart meter gateway periodically
and publish meter values to InfluxDB

@author: Oliver Heimlich <oheim@posteo.de>
"""


import dotenv
import requests, base64
import timeloop
import datetime
import zoneinfo
import asyncio
import sys
import logging
from requests.auth import HTTPDigestAuth
from bs4 import BeautifulSoup
import urllib3
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


urllib3.disable_warnings()
config = dotenv.dotenv_values(sys.argv[1])

background = timeloop.Timeloop()

def read_meter_value():
    global config
    
    s = requests.Session()
    res=s.get(config['METER_URL'], auth=HTTPDigestAuth(config['METER_USERNAME'], config['METER_PASSWORD']), verify=False)
    cookies = { 'Cookie' : res.cookies.get('session')}

    soup = BeautifulSoup(res.content, 'html.parser')
    tags = soup.find_all('input')
    token = tags[0].get('value')
    action = 'meterform'
    post_data = "tkn=" + token + "&action=" + action 

    res = s.post(config['METER_URL'], data=post_data, cookies=cookies, verify=False)

    soup = BeautifulSoup(res.content, 'html.parser')
    sel = soup.find(id='meterform_select_meter')
    meter_val = sel.findChild()
    meter_id = meter_val.attrs.get('value')
    action = 'showMeterProfile'
    post_data = "tkn=" + token + "&action=" + action + "&mid=" + meter_id

    res = s.post(config['METER_URL'], data=post_data, cookies=cookies, verify=False)

    soup = BeautifulSoup(res.content, 'html.parser')
    table_data = soup.find('table', id="metervalue")
    result_data = {
                    'value': table_data.find(id="table_metervalues_col_wert").string,
                    'unit': table_data.find(id="table_metervalues_col_einheit").string,
                    'timestamp': table_data.find(id="table_metervalues_col_timestamp").string,
                    'isvalid': table_data.find(id="table_metervalues_col_istvalide").string,
                    'name': table_data.find(id="table_metervalues_col_name").string,
                    'obis': table_data.find(id="table_metervalues_col_obis").string
                }
    s.close()
    return result_data

def forward_datapoint(meter_value):
    global influx_api
    
    point = (
        Point("SMART_METER_GATEWAY")
        .tag("unit", meter_value['unit'])
        .field("OBIS/" + meter_value['obis'], float(meter_value['value']))
        .time(datetime.datetime.fromisoformat(meter_value['timestamp']).replace(tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")))
    )
   
    influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

last_timestamp = None
@background.job(interval = datetime.timedelta(minutes = 10))
def update_measurement():
    global last_timestamp
    
    try:
        meter_value = read_meter_value()
        if last_timestamp is None or meter_value['timestamp'] != last_timestamp:
            forward_datapoint(meter_value)
    except Exception as err:
        logging.exception('Failed to update measurement')
        return
        
loop = None
influx_api = None
async def main():
    global config
    global loop
    global influx_api

    loop = asyncio.get_running_loop()

    influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
    influx_api = influx_client.write_api(write_options=SYNCHRONOUS)

    update_measurement()
    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
