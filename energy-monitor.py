#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2020-2021 Oliver Heimlich <oheim@posteo.de>
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

"""Monitors an electric consumer for the end of operation

This script reads the current operation status of a device over MQTT
(from a script on a Shelly Plus Plug).  If we detect that the consumer, e. g., washing
machine, no longer consumes a lot of power, we send a telegram message.

@author: Oliver Heimlich <oheim@posteo.de>
"""

import sys
import time
import logging
import locale
import dotenv
import asyncio

from modules import telegram, mqttclient

devicename = sys.argv[1]

locale.setlocale(locale.LC_ALL, 'de_DE.utf8')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

cycle_state = None
def on_mqtt_message(client, userdata, msg):
	global cycle_state
	global loop

	cycle_state = msg.payload.decode()
	loop.create_task(notify_on_cycle_change(cycle_state))


started_message_id = None
time_start = None
async def notify_on_cycle_change(new_cycle_state):
	global config
	global started_message_id
	global time_start

	if new_cycle_state == 'start':
		time_start = time.time()
		message = config['STARTED_MESSAGE_TEMPLATE']
		logging.info(message)

		started_message_id = await telegram.bot_send(text=message)

	if new_cycle_state == 'stop':
		if started_message_id is not None:
			await telegram.bot_delete(message_id=started_message_id)
			started_message_id = None

		if time_start is not None:
			time_stop = time.time()
			cycle_duration = time_stop - time_start

			if cycle_duration < 10 * 60: # 10min
				logging.warning("Gerät war weniger als 10 Minuten eingeschaltet")
			else:
				message = config['DONE_MESSAGE_TEMPLATE']
				logging.info(message)

				await telegram.bot_send(text=message)


config = dotenv.dotenv_values(devicename + ".env")

loop = None
async def main():
	global config
	global loop

	loop = asyncio.get_running_loop()

	await telegram.bot_start(token=config['TELEGRAM_BOT_TOKEN'], chat_id=int(config['TELEGRAM_CHAT_ID']))

	mqttclient.connect(server=config['MQTT_SERVER'], user=config['MQTT_USER'], password=config['MQTT_PASSWORD'], message_callback=on_mqtt_message)
	mqttclient.subscribe(config['MQTT_TOPIC'])

	try:
		while True:
    			await asyncio.sleep(1)

	finally:
		mqttclient.disconnect()
		await telegram.bot_stop()

asyncio.run(main())
