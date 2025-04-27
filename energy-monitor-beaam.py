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

"""Periodically poll the Neoom Beaam API and publish the power level of
PV production via MQTT

@author: Oliver Heimlich <oheim@posteo.de>
"""

import logging
import dotenv
import timeloop
import json
import datetime
import asyncio
import requests

from modules import mqttclient

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Beaam.env")

background = timeloop.Timeloop()

def find_value_by_key(data, target_key):
    for item in data:
        if item['key'] == target_key:
            return item['value']
    return None

def get_energy_flow_states():
    global config
    
    headers = {
        'Authorization': 'Bearer ' + config['BEAAM_API_TOKEN']
    }
    response = requests.get(config['BEAAM_API_STATE_URL'], headers=headers)
    response.raise_for_status()
    data = response.json()
    return data['energyFlow']['states']

average_power = 0.0
@background.job(interval = datetime.timedelta(minutes = 1))
def update_measurement():
    global average_power
    global config

    try:
        current_power_production = find_value_by_key(
            get_energy_flow_states(),
            'POWER_PRODUCTION')
    except HTTPError as err:
        logging.error('Failed to update measurement', err)
        return

    smoothing_factor = 0.125
    average_power += smoothing_factor * (current_power_production - average_power)
    mqttclient.publish(config['MQTT_TOPIC'], "%d" % (average_power))

loop = None
async def main():
    global config
    global loop

    loop = asyncio.get_running_loop()

    mqttclient.connect(server=config['MQTT_SERVER'], user=config['MQTT_USER'], password=config['MQTT_PASSWORD'])

    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()
        mqttclient.disconnect()

asyncio.run(main())
