#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright 2021 Oliver Heimlich <oheim@posteo.de>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
   
@author: Oliver Heimlich <oheim@posteo.de>
"""

import tinytuya
import wetterdienst
from wetterdienst.dwd.forecasts import DWDMosmixParameter, DWDMosmixStations
from pandas import DataFrame
import datetime
import time
import timeloop
import logging
import sys
import dotenv
import telegram.ext
import astral
import astral.sun

hostname = sys.argv[1]
config = dotenv.dotenv_values("Markise.env")

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
        
        # Open at sunset
        sunset = astral.sun.sunset(
                astral.Observer(
                        latitude = config['LATITUDE'],
                        longitude = config['LONGITUDE']))
        local_schedule.loc[sunset] = False
        local_schedule.sort_index()
        
        # Update global variable
        schedule = local_schedule
    
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
                bot_send('Die Markise wird ausgefahren.')
            device.set_value(1, 'close')
        else:
            if not (is_closed is None):
                bot_send('Die Markise wird eingefahren.')
            device.set_value(1, 'open')
        is_closed = close_now
        
    except:
        logging.exception('Fehler beim Anwenden des Plans')        

update_schedule()


updater = telegram.ext.Updater(token=config['BOT_TOKEN'])

def bot_start(update, context):
    logging.info("New message in chat %d", update.effective_chat.id)
    context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")

def bot_send(message):
    global updater
    global config
    
    updater.bot.send_message(chat_id=int(config['CHAT_ID']), text=message)

updater.dispatcher.add_handler(telegram.ext.CommandHandler('start', bot_start))

updater.start_polling()

background.start()

try:
    while True:
        time.sleep(1)
finally:
    background.stop()
    updater.stop()
