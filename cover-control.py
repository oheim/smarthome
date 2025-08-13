#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2021 Oliver Heimlich <oheim@posteo.de>
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

"""Control program for a window sunscreen

This script sends the optimal sunscreen position over network to a
microcontroller.  The microcontroller moves the sunscreen.

The optimal sunscreen position is based on a weather forecast, which we
retrieve for a nearby weather station by DWD (Deutscher Wetterdienst).

@author: Oliver Heimlich <oheim@posteo.de>
"""

import datetime
import time
import timeloop
import logging
import dotenv
import asyncio
import json

from modules import weather, arduinoclient, telegram, mqttclient

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Sunscreen.env")
weather.set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
arduinoclient.set_address(hostname=config['ARDUINO_HOSTNAME'], port=int(config['ARDUINO_PORT']))


background = timeloop.Timeloop()

schedule = None
@background.job(interval = datetime.timedelta(hours = 2))
def update_schedule():
    global schedule
    global config
    global radar_rain
    
    try:
        schedule = weather.get_sunscreen_schedule()
        logging.info('Wettervorhersage aktualisiert')
        
        if radar_rain is None:
            update_radar()
        
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')


radar_rain = None
@background.job(interval = datetime.timedelta(minutes = 5))
def update_radar():
    global radar_rain
    global schedule
    global config
    
    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        soon = now + datetime.timedelta(minutes = 10)
        close_now = schedule[schedule.index.to_pydatetime() > now]['WEATHER_PREDICTION'].iloc[0] != 'bad'
        close_soon = schedule[schedule.index.to_pydatetime() > soon]['WEATHER_PREDICTION'].iloc[0] != 'bad'
        
        if close_now or close_soon:
            radar_rain = weather.get_current_precipitation()
        else:
            # The screen is not closed. No need to query the radar.
            radar_rain = None
        
    except:
        logging.exception('Fehler beim Abruf der Radar-Daten')
        radar_rain = None

@background.job(interval = datetime.timedelta(minutes = 1))
def bg_apply_schedule():
    global loop
    asyncio.run_coroutine_threadsafe(apply_schedule(), loop)

def sun_is_shining():
    global config
    return is_power_above(int(config['PV_PEAK_POWER']) / 4)

def sun_is_not_shining():
    global config
    return is_power_below(int(config['PV_PEAK_POWER']) / 8)

is_closed = None
window_is_closed = None
dont_close_window_until = None
async def apply_schedule():
    global config
    global is_closed
    global window_is_closed
    global schedule
    global radar_rain
    global dont_close_window_until

    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        # The schedule contains predictions for certain timestamps about the wheather within the 'last 1 hour'.
        # Thus the prediction for now is the first schedule entry where the timestamp is greater than 'now'.
        current_schedule = schedule[schedule.index.to_pydatetime() > now]
        reason = current_schedule['REASON'].iloc[0]
        extended_reason = current_schedule['EXTENDED_REASON'].iloc[0]

        if is_closed:
            close_now = current_schedule['WEATHER_PREDICTION'].iloc[0] != 'bad'
        else:
            close_now = current_schedule['WEATHER_PREDICTION'].iloc[0] == 'good' and current_schedule['WEATHER_PREDICTION'].iloc[1] == 'good'

            # To prevent unnecessary movement:
            # If the sunscreen will be opened in the next two time frames, we don't close it.
            if close_now and current_schedule['WEATHER_PREDICTION'].iloc[2] == 'bad':
                close_now = False

        # The forecast might be incorrect or outdated.
        # If the radar detects unexpected precipitation, we must open the suncreen.
        if radar_rain:
            close_now = False
            reason = '🌦'
            extended_reason += '🌦'

        close_window_reason = reason
        if window_is_closed or (dont_close_window_until is not None and dont_close_window_until > now):
            close_window_now = False
        else:
            close_window_now = radar_rain or current_schedule['CLOSE_WINDOW'].iloc[0]

        # The sunscreen should be open during low irradiation.
        # An open window may stay open.
        #
        # TODO: In summer we need to control each direction differently
        # if close_now and not is_closed and not sun_is_shining():
        #     close_now = False
        #     reason = '🌅'
        #     extended_reason += '🌅'
        # if close_now and is_closed and sun_is_not_shining():
        #     close_now = False
        #     reason = '🌄'
        #     extended_reason += '🌅'

        logging.info('Status: {}'.format(extended_reason))

        if not (is_closed == close_now):
            if close_now:
                logging.info('Markise wird ausgefahren %s', reason)
                arduinoclient.close_curtain()
                mqttclient.shelly_command(config['COVER_CONTROL_DEVICE_ID'], config['COVER_CONTROL_COMPONENT_ID'], 'close')
                if is_closed is not None:
                    await telegram.bot_send('Die Markise wird ausgefahren {}'.format(reason))
            else:
                logging.info('Markise wird eingefahren %s', reason)
                if close_window_now:
                    logging.info('Fenster werden geschlossen')
                    arduinoclient.close_window()
                arduinoclient.open_curtain()
                mqttclient.shelly_command(config['COVER_CONTROL_DEVICE_ID'], config['COVER_CONTROL_COMPONENT_ID'], 'open')
                if is_closed is not None:
                    if close_window_now and window_is_closed is not None:
                        await telegram.bot_send('Die Markise wird eingefahren und die Fenster werden geschlossen {}'.format(reason))
                    else:
                        await telegram.bot_send('Die Markise wird eingefahren {}'.format(reason))
                if close_window_now:
                    window_is_closed = None
            is_closed = close_now

        if close_window_now:
            logging.info('Fenster werden automatisch geschlossen {}'.format(close_window_reason))
            arduinoclient.close_window()
            if window_is_closed is not None:
                await telegram.bot_send(text='Die Fenster werden geschlossen {}'.format(close_window_reason))
            window_is_closed = True
            dont_close_window_until = None

    except:
        logging.exception('Fehler beim Anwenden des Plans')


async def on_window_command(command, args):
    global window_is_closed
    global schedule
    global radar_rain
    global dont_close_window_until


    if command == 'fenster_auf':
        logging.info('Fenster werden geöffnet')
        dont_close_window_until = datetime.datetime.now(datetime.timezone.utc).astimezone() + datetime.timedelta(minutes = 10)
        arduinoclient.open_window()
        window_is_closed = False
        await telegram.bot_send(text='Die Fenster werden geöffnet')

    if command == 'fenster_zu':
        logging.info('Fenster werden geschlossen')
        arduinoclient.close_window()
        window_is_closed = True
        dont_close_window_until = None
        await telegram.bot_send(text='Die Fenster werden geschlossen')

# Receive power measurements for a PV device over MQTT.
# see cover-control-shelly.js

power_history = []

def is_power_above(threshold):
        if len(power_history) == 0:
                return False
        else:
                return min(power_history) > threshold

def is_power_below(threshold):
        if len(power_history) == 0:
                return False
        else:
                return max(power_history) < threshold

def on_power_measurement(power_measurement):
        global power_history

        power_history.append(power_measurement)
        if len(power_history) > 10:
                power_history.pop(0)

## Receive BLE button commands

def on_ble_event(payload):
        global is_closed
        global window_is_closed
        global dont_close_window_until

        if payload['addr'] == '7c:c6:b6:64:dc:ee':
                if payload['Button'] == 1024:
                        logging.info('BLE: Fenster auf')
                        dont_close_window_until = datetime.datetime.now(datetime.timezone.utc).astimezone() + datetime.timedelta(minutes = 10)
                        arduinoclient.open_window()
                        window_is_closed = False
                if payload['Button'] == 512:
                        logging.info('BLE: Fenster zu')
                        arduinoclient.close_window()
                        window_is_closed = True
                        dont_close_window_until = None
                if payload['Button'] == 256:
                        logging.info('BLE: Markise öffnen')
                        arduinoclient.open_curtain()
                        is_closed = False
                if payload['Button'] == 128:
                        logging.info('BLE: Markise schließen')
                        arduinoclient.close_curtain()
                        is_closed = True


def on_message(client, userdata, msg):
        if msg.topic == 'shellies/ble':
                on_ble_event(json.loads(msg.payload))
        else:
                on_power_measurement(int(msg.payload))


loop = None
async def main():
    global config
    global loop

    loop = asyncio.get_running_loop()

    mqttclient.connect(server=config['MQTT_SERVER'], user=config['MQTT_USER'], password=config['MQTT_PASSWORD'], message_callback=on_message)
    mqttclient.subscribe(config['MQTT_TOPIC'])
    mqttclient.subscribe('shellies/ble')

    await telegram.bot_start(token=config['TELEGRAM_BOT_TOKEN'], chat_id=config['TELEGRAM_CHAT_ID'], commands=['fenster_auf', 'fenster_zu'], command_callback=on_window_command)

    update_schedule()

    await apply_schedule()

    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()
        mqttclient.disconnect()
        await telegram.bot_stop()

asyncio.run(main())
