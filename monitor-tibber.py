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

"""Collect information from Tibber API and publish them to InfluxDB

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

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Tibber.env")

background = timeloop.Timeloop()

def call_api(query):
    global config
    
    headers = {
        'Authorization': 'Bearer ' + config['TIBBER_API_TOKEN'],
        'Content-Type': 'application/json'
    }
    request = {
        'query': query
    }
    
    response = requests.post(config['TIBBER_URL'], headers=headers, json=request)
    response.raise_for_status()
    return response.json()

def load_prices(day):
    query = """
        {
          viewer {
            homes {
              currentSubscription{
                priceInfo(resolution: QUARTER_HOURLY) {
                  %day% {
                    total
                    startsAt
                  }
                }
              }
            }
          }
        }
    """
    
    response = call_api(query.replace('%day%', day))
    prices = response['data']['viewer']['homes'][0]['currentSubscription']['priceInfo'][day]
    if prices:
        publish_prices(prices)

known_price_date = None
def publish_prices(prices):
    global influx_api
    global known_price_date
    global config

    for price in prices:
        point = (
            Point('TIBBER')
                .tag('unit', 'EUR/kWh')
                .field('priceInfo', price['total'])
                .time(datetime.datetime.fromisoformat(price['startsAt']))
        )
        influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

    known_price_date = datetime.datetime.fromisoformat(prices[0]['startsAt']).date()

@background.job(interval = datetime.timedelta(minutes=15))
def update_prices():
    global known_price_date
    
    try:
        now = datetime.datetime.now()
        today = now.date()
        if known_price_date is None or known_price_date < today:
            load_prices('today')
        
        if known_price_date == today and now.time().hour >= 13:
            load_prices('tomorrow')
        
    except Exception as err:
        logging.exception('Failed to update prices')
        return

def load_consumption(last_hours):
    query = """
        {
          viewer {
            homes {
              consumption(resolution: HOURLY, last: %last_hours%) {
                nodes {
                  to
                  cost
                  consumption
                  consumptionUnit
                }
              }
            }
          }
        }
    """

    response = call_api(query.replace('%last_hours%', str(last_hours)))
    nodes = response['data']['viewer']['homes'][0]['consumption']['nodes']
    for node in nodes:
        if node['consumption'] is not None:
            publish_consumption(node)

def publish_consumption(node):
    global config
    global influx_api

    point = (
        Point('TIBBER')
            .tag('consumptionUnit', node['consumptionUnit'])
            .field('consumption', float(node['consumption']))
            .field('cost', float(node['cost']))
            .time(datetime.datetime.fromisoformat(node['to']))
    )
    influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)

@background.job(interval = datetime.timedelta(minutes=20))
def update_consumption():
    try:
        # Nodes are listed even if the consumption and cost haven't been collected yet.
        # The smart meter reports its measurements every 24 hours only.
        # We simply query the last 48 hours and store any data that is available.
        load_consumption(48)
            
    except Exception as err:
        logging.exception('Failed to update consumption')
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
    load_consumption(100)
    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
