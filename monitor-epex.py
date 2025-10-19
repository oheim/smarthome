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

"""Collect information from EPEX SPOT market prices

@author: Oliver Heimlich <oheim@posteo.de>
"""

import logging
import dotenv
import timeloop
import json
import pytz
import datetime
import asyncio
import requests
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("EPEX.env")

background = timeloop.Timeloop()

def load_prices(day):
    bidding_zone = "DE-LU"
    timezone = pytz.timezone("Europe/Berlin")
    start = datetime.datetime.combine(day, datetime.time(), timezone)
    start_ts = int(start.timestamp())
    end = start + datetime.timedelta(days=2)
    end_ts = int(end.timestamp())
        
    headers = {
        'Accept': 'application/json'
    }
    url = f"https://api.energy-charts.info/price?bzn={bidding_zone}&start={start_ts}&end={end_ts}"
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    timestamps = [datetime.datetime.fromtimestamp(ts, timezone) for ts in data['unix_seconds']]
    prices = dict(zip(timestamps, data['price']))
    publish_prices(prices)
    
known_price_date = None
def publish_prices(prices):
    global influx_api
    global known_price_date
    global config

    for start_time, price in prices.items():
        point = (
            Point('EPEX')
                .tag('unit', 'EUR/MWh')
                .field('priceInfo', float(price))
                .time(start_time)
        )
        influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)
        known_price_date = start_time.date()

@background.job(interval = datetime.timedelta(minutes=15))
def update_prices():
    global known_price_date
    
    try:
        now = datetime.datetime.now()
        today = now.date()
        if known_price_date is None or known_price_date < today:
            load_prices(today)
        
        if known_price_date == today and now.time().hour >= 13:
            tomorrow = today + datetime.timedelta(days=1)
            load_prices(tomorrow)
        
    except Exception as err:
        logging.exception('Failed to update prices')
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

    update_prices()
    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
