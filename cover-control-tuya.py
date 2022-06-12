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

from modules import weather

hostname = sys.argv[1]
config = dotenv.dotenv_values("Sunscreen-tuya.env")
weather.set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))

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
        soon = now + datetime.timedelta(minutes = 10)
        close_now = schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[0]
        close_soon = schedule[schedule.index.to_pydatetime() > soon]['CLOSE'].iloc[0]
        reason = schedule[schedule.index.to_pydatetime() > now]['REASON'].iloc[0]

        # To prevent unnecessary movement:
        # If the sunscreen will be opened shortly, we don't close it.
        if close_now and not close_soon and not is_closed:
            close_now = False
            reason = '⏲'

        # The forecast might be incorrect or outdated.
        # If the radar detects unexpected precipitation, we must open the suncreen.
        if close_now and radar_rain:
            close_now = False
            reason = '🌦'

        if is_closed == close_now:
            # Nothing to do
            return

        status = device.status()
        if status['dps']['1'] != 'stop':
            # Do nothing if the device is operating right now
            return
    
        if close_now:
            if not (is_closed is None):
                logging.info('Die Markise wird ausgefahren %s', reason)
            device.set_value(1, 'close')
        else:
            if not (is_closed is None):
                logging.info('Die Markise wird eingefahren %s', reason)
            device.set_value(1, 'open')
        is_closed = close_now
        
    except:
        logging.exception('Fehler beim Anwenden des Plans')        

update_schedule()

update_device_status()
apply_schedule()

background.start()

try:
    while True:
        time.sleep(1)
finally:
    background.stop()
