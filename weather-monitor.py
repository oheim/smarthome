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

"""Retrieve a weather forecast from nearby weather stations by DWD (Deutscher Wetterdienst).

@author: Oliver Heimlich <oheim@posteo.de>
"""

import datetime
import timeloop
import logging
import dotenv
import asyncio
import pandas as pd

from wetterdienst import Wetterdienst
from wetterdienst.provider.dwd.mosmix import DwdForecastDate

import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Weather.env")
background = timeloop.Timeloop()

def set_location(latitude, longitude):
    global local_stations
    
    # Find 2 local forecast stations
    api = Wetterdienst(provider = 'dwd', network = 'mosmix')
    mosmix_request = api(parameters=("hourly", "large"))
    local_stations = mosmix_request.filter_by_rank(latlon=(latitude, longitude), rank=2).values

def get_forecast():
    global local_stations
    global influx_api
    
    for local_station in local_stations.all().df_stations['station_id']:
        forecast = local_stations.read_mosmix_large(station_id = local_station, date = DwdForecastDate.LATEST).to_pandas()
        
        forecast.index = pd.to_datetime(forecast.date)
        forecast.drop('date', axis=1, inplace=True)
        
        forecast['stationId'] = local_station
        
        influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=forecast, data_frame_measurement_name='MOSMIX', data_frame_tag_columns=['stationId'])

@background.job(interval = datetime.timedelta(hours = 2))
def update_forecast():
    try:
        get_forecast()
        logging.info('Wettervorhersage aktualisiert')
        
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')

loop = None
async def main():
    global config
    global loop
    global influx_api

    loop = asyncio.get_running_loop()

    influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
    influx_api = influx_client.write_api(write_options=SYNCHRONOUS)

    set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
    get_forecast()

    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
