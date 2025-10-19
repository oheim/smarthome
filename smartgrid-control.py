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

# SG-Ready spec
#   10 = shutdown or limited to 4.2 kW
#   00 = normal operation
#   01 = increased operation
#   11 = maximum operation (only in spec 1.0, not supported by spec 1.1)
#
# My heat pump doesn't follow the specification
#   10 = shutdown
#   00 = (!) reduced operation
#   01 = (!) normal operation
#   11 = (!) increased operation
#
# The relays have been wired such that the two states for normal and increased operation
# are compliant with the specification, but the other two states are not SG Ready compliant.
#   10 = (!) reduced operation
#   00 = normal operation
#   01 = increased operation
#   11 = (!) shutdown
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

        # this is not SG ready compliant!
        # see comment above
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
            |> median()
    """)
    return result is not None and result > 3200

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

def is_not_self_sufficient():
    self_sufficiency = influx_query_single_value("""
        from(bucket: "%BUCKET%")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "SITE")
          |> filter(fn: (r) => r._field == "SELF_SUFFICIENCY")
          |> last()
    """)
    return self_sufficiency is not None and self_sufficiency < 10

def is_self_sufficient():
    self_sufficiency = influx_query_single_value("""
        from(bucket: "%BUCKET%")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "SITE")
          |> filter(fn: (r) => r._field == "SELF_SUFFICIENCY")
          |> last()
    """)
    return self_sufficiency is not None and self_sufficiency > 90

def is_charging_from_grid():
    charge_battery_from_grid = influx_query_single_value("""
        from(bucket: "%BUCKET%")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "SITE")
          |> filter(fn: (r) => r.unit == "W")
          |> last()
          |> keep(columns: ["unit", "_field", "_value"])
          |> pivot(rowKey: ["unit"], columnKey: ["_field"], valueColumn: "_value")
          |> map(fn: (r) => ({_value: r.POWER_STORAGE < -4000 and r.POWER_GRID > 1000}))
    """)
    return charge_battery_from_grid is not None and charge_battery_from_grid

def is_power_expensive():
    # We assume that the heat pump will run for 45 minutes = 3 * 15 minutes.
    # We compute the power price when the heat pump is starting now vs. when it is started 15, 30 or 45 minutes later.
    # If there is going to be a significantly lower price later, we consider the current price as expensive.
    best_later_price_difference = influx_query_single_value("""
        from(bucket: "%BUCKET%")
          |> range(start: -5m, stop: 2h)
          |> filter(fn: (r) => r._measurement == "EPEX" and r._field == "priceInfo" and r.unit == "EUR/MWh")
          |> movingAverage(n: 3)
          |> difference()
          |> limit(n: 3)
          |> cumulativeSum()
          |> min()
    """)
    return best_later_price_difference is not None and best_later_price_difference < -0.05

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
            if is_charging_from_grid() or (is_battery_well_charged() and (is_pv_production_high() or is_not_self_sufficient())):
                new_sg_ready_state = SG_Ready.HIGH
        else:
            # delay start of heat pump while power is expensive
            if is_power_expensive() and not is_pv_production_high() and (not is_self_sufficient() or not is_battery_well_charged()):
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
