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

This script sends the optimal sunscreen position over network (UDP) to a
microcontroller (Arduino with ethernet shield).  The microcontroller supports
manual override with a hardware switch and triggers a remote control to send
radio commands to move the sunscreen.

Find the arduino code in cover-control-arduino.ino.

The optimal sunscreen position is based on a weather forecast, which we
retrieve for a nearby weather station by DWD (Deutscher Wetterdienst).

@author: Oliver Heimlich <oheim@posteo.de>
"""

import socket
import wetterdienst
from wetterdienst.dwd.forecasts import DWDMosmixParameter, DWDMosmixStations
from pandas import DataFrame
import datetime
import time
import timeloop
import logging
import sys
import dotenv
import astral
import astral.sun

from modules import telegram

hostname = sys.argv[1]
config = dotenv.dotenv_values("Sunscreen-arduino.env")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

background = timeloop.Timeloop()

schedule = None
@background.job(interval = datetime.timedelta(hours = 3))
def update_schedule():
    global schedule
    global config
    
    try:
        
        parameters = [
                DWDMosmixParameter.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H,
                DWDMosmixParameter.WIND_GUST_MAX_LAST_1H,
                DWDMosmixParameter.SUNSHINE_DURATION,
                DWDMosmixParameter.TEMPERATURE_DEW_POINT_200,
                DWDMosmixParameter.TEMPERATURE_AIR_200]
        
        mosmix = wetterdienst.dwd.forecasts.DWDMosmixValues(
                station_id = config['STATION_ID'],
                mosmix_type = wetterdienst.dwd.forecasts.DWDMosmixType.LARGE,
                start_issue = wetterdienst.dwd.forecasts.metadata.dates.DWDForecastDate.LATEST,
                parameter = parameters
                )
        
        # Read data
        forecast = mosmix.all().dropna().pivot(
                index = ['DATE', 'STATION_ID'],
                columns = 'PARAMETER',
                values = 'VALUE')
        
        # Aggregate over all stations (use pessimistic values)
        forecast_agg = forecast.groupby('DATE').agg({
                DWDMosmixParameter.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H.value: 'max',
                DWDMosmixParameter.WIND_GUST_MAX_LAST_1H.value: 'max',
                DWDMosmixParameter.SUNSHINE_DURATION.value: 'max',
                DWDMosmixParameter.TEMPERATURE_DEW_POINT_200.value: 'max',
                DWDMosmixParameter.TEMPERATURE_AIR_200.value: 'min'
                })
        
        # Default = leave open
        local_schedule = DataFrame(False, index = forecast_agg.index, columns=['CLOSE'])
        # Close, if more than 5 Minutes sunshine per hour
        local_schedule[forecast_agg[DWDMosmixParameter.SUNSHINE_DURATION.value] > 5 * 60] = True
        # Open, if windy
        local_schedule[forecast_agg[DWDMosmixParameter.WIND_GUST_MAX_LAST_1H.value] > 10] = False
        # Open, if rainy
        local_schedule[forecast_agg[DWDMosmixParameter.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H.value] > 50.0] = False
        # Open, if below 4°C to protect from ice and snow
        local_schedule[forecast_agg[DWDMosmixParameter.TEMPERATURE_AIR_200.value] < 277.15] = False
        # Open, if below dew point to protect from moisture
        local_schedule[forecast_agg[DWDMosmixParameter.TEMPERATURE_DEW_POINT_200.value] > forecast_agg[DWDMosmixParameter.TEMPERATURE_AIR_200.value]] = False
        
        observer = astral.Observer(
                        latitude = config['LATITUDE'],
                        longitude = config['LONGITUDE'])

        # Don't close before sunrise
        sunrise = astral.sun.sunrise(observer)
        local_schedule.loc[sunrise] = False
        
        # Open at sunset
        sunset = astral.sun.sunset(observer)
        index_after_sunset = local_schedule.index.where(local_schedule.index.to_pydatetime() > sunset).min()
        local_schedule.loc[sunset] = local_schedule.loc[index_after_sunset]
        local_schedule.loc[index_after_sunset] = False

        local_schedule = local_schedule.sort_index()
        
        # Update global variable
        schedule = local_schedule
    
        logging.info('Wettervorhersage aktualisiert')
        
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')


udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
def send_command(command):
    global hostname
    global udp
    
    udp.sendto(bytes(command, 'utf-8'), (socket.gethostbyname(hostname), 8888))

is_closed = None
@background.job(interval = datetime.timedelta(minutes = 1))
def apply_schedule():
    global is_closed
    global schedule
    
    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        close_now = schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[0]

        if close_now:
            if is_closed == False:
                telegram.bot_send('Die Markise wird ausgefahren.')
            send_command('close')
        else:
            if is_closed == True:
                telegram.bot_send('Die Markise wird eingefahren.')
            send_command('open')
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
