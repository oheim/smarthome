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

from modules import weather, arduinoclient, tuyaclient, telegram, mqttclient

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Sunscreen.env")
weather.set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
arduinoclient.set_address(hostname=config['ARDUINO_HOSTNAME'], port=int(config['ARDUINO_PORT']))
tuyaclient.set_cover_device(device_id=config['TUYA_DEVICE_ID'], hostname=config['TUYA_HOSTNAME'], local_key=config['TUYA_LOCAL_KEY'])

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
        close_now = schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[0]
        close_soon = schedule[schedule.index.to_pydatetime() > soon]['CLOSE'].iloc[0]
        
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
    return mqttclient.is_power_above(int(config['PV_PEAK_POWER']) / 4)

def sun_is_not_shining():
    global config
    return mqttclient.is_power_below(int(config['PV_PEAK_POWER']) / 8)

is_closed = None
async def apply_schedule():
    global is_closed
    global schedule
    global radar_rain
    global close_window_at
    
    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        current_schedule = schedule[schedule.index.to_pydatetime() > now]
        close_now = current_schedule['CLOSE'].iloc[0]
        reason = current_schedule['REASON'].iloc[0]

        # To prevent unnecessary movement:
        # If the sunscreen will be opened in the next two time frames, we don't close it.
        if close_now and not is_closed:
            if not (current_schedule['CLOSE'].iloc[1] and
                    current_schedule['CLOSE'].iloc[2]):
                close_now = False
                reason = '⏲'

        # The forecast might be incorrect or outdated.
        # If the radar detects unexpected precipitation, we must open the suncreen.
        if close_now and radar_rain:
            close_now = False
            reason = '🌦'

        close_window_now = not close_now and close_window_at is not None

        # The sunscreen should be open during low irradiation.
        # An open window may stay open.
        if close_now and not is_closed and not sun_is_shining():
            close_now = False
            reason = '🌅'
        if close_now and is_closed and sun_is_not_shining():
            close_now = False
            reason = '🌄'

        if not (is_closed == close_now):
            if close_now:
                logging.info('Markise wird ausgefahren %s', reason)
                arduinoclient.close_curtain()
                tuyaclient.close_curtain()
                if is_closed is not None:
                    await telegram.bot_send('Die Markise wird ausgefahren {}'.format(reason))
            else:
                logging.info('Markise wird eingefahren %s', reason)
                if close_window_now:
                    logging.info('Fenster werden geschlossen')
                    arduinoclient.close_window()
                    close_window_at = None
                arduinoclient.open_curtain()
                tuyaclient.open_curtain()
                if is_closed is not None:
                    if close_window_now:
                        await telegram.bot_send('Die Markise wird eingefahren und die Fenster werden geschlossen {}'.format(reason))
                    else:
                        await telegram.bot_send('Die Markise wird eingefahren {}'.format(reason))
            is_closed = close_now

    except:
        logging.exception('Fehler beim Anwenden des Plans')

@background.job(interval = datetime.timedelta(minutes = 1))
def bg_close_window():
    global loop
    asyncio.run_coroutine_threadsafe(close_window(), loop)


close_window_at = None
async def close_window():
    global close_window_at
    
    if close_window_at is None:
        return
    
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    if now > close_window_at:
        logging.info('Fenster werden automatisch geschlossen')
        arduinoclient.close_window()
        close_window_at = None
        await telegram.bot_send(text='Die Fenster werden geschlossen')
    

async def open_window(args):
    global close_window_at
    
    if len(args) != 1:
        return
    
    if args[0] == 'auf':
        logging.info('Fenster werden geöffnet')
        await telegram.bot_send(text='Die Fenster werden geöffnet')
        close_window_at = weather.get_next_sunset()
        arduinoclient.open_window()
        
    if args[0] == 'zu':
        logging.info('Fenster werden geschlossen')
        await telegram.bot_send(text='Die Fenster werden geschlossen')
        arduinoclient.close_window()
        close_window_at = None
    
    if args[0].isnumeric():
        minutes = float(args[0])
        if minutes < 1:
            minutes = 60
            
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        close_window_at = now + datetime.timedelta(minutes = minutes)
        
        logging.info('Fenster werden geöffnet')
        arduinoclient.open_window()
        await telegram.bot_send(text='Die Fenster werden für {:g} Minuten geöffnet'.format(minutes))

loop = None
async def main():
    global config
    global loop

    loop = asyncio.get_running_loop()

    mqttclient.connect(server=config['MQTT_SERVER'], user=config['MQTT_USER'], password=config['MQTT_PASSWORD'], topic=config['MQTT_TOPIC'])

    await telegram.bot_start(token=config['TELEGRAM_BOT_TOKEN'], chat_id=config['TELEGRAM_CHAT_ID'], command='Fenster', command_callback=open_window)

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
