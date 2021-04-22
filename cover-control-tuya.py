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

This script reads the current sunscreen position over network from a
microcontroller (tuya cover device).  If the current suncreen position differs
from the optimal position, we send a command to move the sunscreen.

The optimal sunscreen position is based on a weather forecast, which we
retrieve for a nearby weather station by DWD (Deutscher Wetterdienst).

@author: Oliver Heimlich <oheim@posteo.de>
"""

import tinytuya
import datetime
import time
import timeloop
import logging
import sys
import dotenv

from modules import telegram, weather

hostname = sys.argv[1]
config = dotenv.dotenv_values("Sunscreen-tuya.env")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

background = timeloop.Timeloop()

is_closed = None
@background.job(interval = datetime.timedelta(seconds = 5))
def update_device_status():
    global is_closed
    global hostname
    global config
    
    device = tinytuya.CoverDevice(config['DEVICE_ID'], hostname, config['LOCAL_KEY'])
    device.set_version(3.3)
    device.set_socketRetryLimit(120)
    
    try:
        status = device.status()
        if status['dps']['1'] == 'close':
            if not is_closed:
                logging.info('Markise wird manuell ausgefahren')
                is_closed = True
        elif status['dps']['1'] == 'open':
            if is_closed:
                logging.info('Markise wird manuell eingefahren')
                is_closed = False
        # else:
            #unknown
    except:
        logging.exception('Fehler beim Abruf des IoT-Device-Status')
            

schedule = None
@background.job(interval = datetime.timedelta(hours = 3))
def update_schedule():
    global schedule
    global config
    
    try:
        schedule = weather.get_sunscreen_schedule(config['STATION_ID'], config['LATITUDE'], config['LONGITUDE'])
        logging.info('Wettervorhersage aktualisiert')
        
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')


@background.job(interval = datetime.timedelta(minutes = 1))
def apply_schedule():
    global is_closed
    global schedule
    global hostname
    global config
    
    device = tinytuya.CoverDevice(config['DEVICE_ID'], hostname, config['LOCAL_KEY'])
    device.set_version(3.3)
    device.set_socketRetryLimit(5)

    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        close_now = schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[0]
        if is_closed == close_now:
            # Nothing to do
            return

        status = device.status()
        if status['dps']['1'] != 'stop':
            # Do nothing if the device is operating right now
            return
    
        if close_now:
            if not (is_closed is None):
                telegram.bot_send('Die Markise wird ausgefahren.')
            device.set_value(1, 'close')
        else:
            if not (is_closed is None):
                telegram.bot_send('Die Markise wird eingefahren.')
            device.set_value(1, 'open')
        is_closed = close_now
        
    except:
        logging.exception('Fehler beim Anwenden des Plans')        

update_schedule()


telegram.bot_start(token=config['BOT_TOKEN'], chat_id=int(config['CHAT_ID']))

background.start()

try:
    while True:
        time.sleep(1)
finally:
    background.stop()
    telegram.bot_stop()
