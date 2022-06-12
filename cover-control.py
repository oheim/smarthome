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

from modules import weather, arduinoclient, tuyaclient

config = dotenv.dotenv_values("Sunscreen.env")
weather.set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
arduinoclient.set_address(hostname=config['ARDUINO_HOSTNAME'], port=int(config['ARDUINO_PORT']))
tuyaclient.set_device(device_id=config['TUYA_DEVICE_ID'], hostname=config['TUYA_HOSTNAME'], local_key=config['TUYA_LOCAL_KEY'])

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

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

is_closed = None
@background.job(interval = datetime.timedelta(minutes = 1))
def apply_schedule():
    global is_closed
    global schedule
    global radar_rain
    
    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        close_now = schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[0]
        reason = schedule[schedule.index.to_pydatetime() > now]['REASON'].iloc[0]

        # To prevent unnecessary movement:
        # If the sunscreen will be opened in the next two time frames, we don't close it.
        if close_now and not is_closed:
            if not (schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[1] and
                    schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[2]):
                close_now = False
                reason = '⏲'

        # The forecast might be incorrect or outdated.
        # If the radar detects unexpected precipitation, we must open the suncreen.
        if close_now and radar_rain:
            close_now = False
            reason = '🌦'

        if close_now:
            if is_closed == False:
                logging.info('Die Markise wird ausgefahren %s', reason)
            arduinoclient.close_curtain()
            tuyaclient.close_curtain()
        else:
            if is_closed == True:
                logging.info('Die Markise wird eingefahren %s', reason)
            arduinoclient.open_curtain()
            tuyaclient.open_curtain()
        is_closed = close_now
        
    except:
        logging.exception('Fehler beim Anwenden des Plans')        

update_schedule()

apply_schedule()

background.start()

try:
    while True:
        time.sleep(1)
finally:
    background.stop()
