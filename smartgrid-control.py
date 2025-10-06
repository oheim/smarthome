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

"""Control the SG-Ready interface via Barionet Relay controller (Beaam Extension) based on solar production and power prices

@author: Oliver Heimlich <oheim@posteo.de>
"""

import timeloop
import datetime
import asyncio
import dotenv
import time
import influxdb_client
import enum
import requests
import logging
from influxdb_client import InfluxDBClient, Point, WritePrecision

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("SmartGrid.env")

background = timeloop.Timeloop()
    
class SG_Ready(enum.Enum):
    SHUTDOWN = 1
    LOW = 2
    NORMAL = 3
    HIGH = 4

last_relay_states = { 1: None, 2: None }
def set_sg_ready_relay(relay: int, state: bool):
    global config
    global last_relay_states
    if last_relay_states[relay] is None or last_relay_states[relay] != state:
        response = requests.get(config['BARIONET_URL'] + '?o=' + str(relay) + ',' + str(int(state)))
        response.raise_for_status()
        last_relay_states[relay] = state

last_sg_ready = None
def set_sg_ready(new_state: SG_Ready):
    global last_sg_ready
    if last_sg_ready is None or last_sg_ready != new_state:
        logging.info('SG-Ready: ' + new_state.name)
        set_sg_ready_relay(1, new_state == SG_Ready.SHUTDOWN or new_state == SG_Ready.LOW)
        set_sg_ready_relay(2, new_state == SG_Ready.SHUTDOWN or new_state == SG_Ready.HIGH)
        last_sg_ready = new_state

def influx_query_single_value(query):
    global influx_api
    global config

    result = influx_api.query(org=config['INFLUXDB_ORG'], query=query.replace('%BUCKET%', config['INFLUXDB_BUCKET']))
    for table in result:
        for record in table.records:
            return record.get_value()
    logging.error("Query returned no result: " + query)
    return None

def is_pv_production_high():
    result = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "SITE")
            |> filter(fn: (r) => r._field == "POWER_PRODUCTION")
            |> filter(fn: (r) => r.unit == "W")
            |> min()
    """)
    return result is not None and result > 3500

def is_battery_well_charged():
    result = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "BATTERY")
            |> filter(fn: (r) => r._field == "STATE_OF_CHARGE")
            |> filter(fn: (r) => r.unit == "%")
            |> last()
    """)
    return result is not None and result >= 40

def is_power_cheap():
    current_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: -15m, stop: 15m)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> max()
    """)
    last_week_avg_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: -7d)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> mean()
    """)
    if current_price is None or last_week_avg_price is None or current_price > last_week_avg_price:
        return False

    next_min_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: 0h, stop: 4h)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> min()
    """)
    next_max_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: 0h, stop: 4h)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> max()
    """)
    if next_min_price is None or next_max_price is None or (next_max_price - next_min_price) < 0.1:
        return False
    return current_price <= next_min_price + 0.2 * (next_max_price - next_min_price)

def is_power_expensive():
    current_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: -15m, stop: 15m)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> max()
    """)
    if current_price is None:
        return False

    next_min_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: 0h, stop: 2h)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> min()
    """)
    next_max_price = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: 0h, stop: 2h)
            |> filter(fn: (r) => r._measurement == "TIBBER")
            |> filter(fn: (r) => r._field == "priceInfo")
            |> max()
    """)
    if next_min_price is None or next_max_price is None or (next_max_price - next_min_price) < 0.1:
        return False
    return current_price >= next_min_price + 0.5 * (next_max_price - next_min_price)

def is_heat_pump_running():
    power_consumption = influx_query_single_value("""
        from(bucket: "%BUCKET%")
            |> range(start: -10m)
            |> filter(fn: (r) => r._measurement == "HEAT_PUMP")
            |> filter(fn: (r) => r._field == "ELECTRICAL_POWER")
            |> filter(fn: (r) => r.unit == "W")
            |> last()
    """)
    return power_consumption is not None and power_consumption > 1000

@background.job(interval = datetime.timedelta(minutes=10))
def update_sg_ready():
    try:
        new_sg_ready_state = SG_Ready.NORMAL
        if is_heat_pump_running():
            # delay stop of heat pump while power is cheap
            if is_battery_well_charged():
                if is_pv_production_high() or is_power_cheap():
                    new_sg_ready_state = SG_Ready.HIGH
        else:
            # delay start of heat pump while power is expensive
            if not is_battery_well_charged() and is_power_expensive():
                new_sg_ready_state = SG_Ready.LOW

        set_sg_ready(new_sg_ready_state)
            
    except Exception as err:
        logging.exception('Failed to update SG Ready state')
        return

loop = None
influx_api = None
async def main():
    global config
    global loop
    global influx_api

    loop = asyncio.get_running_loop()

    influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
    influx_api = influx_client.query_api()

    update_sg_ready()
    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
