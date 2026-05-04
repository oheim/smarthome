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
    global influx_write_api
    
    for local_station in local_stations.all().df_stations['station_id']:
        forecast = local_stations.read_mosmix_large(station_id = local_station, date = DwdForecastDate.LATEST).to_pandas()
        
        forecast.index = pd.to_datetime(forecast.date)
        forecast.drop('date', axis=1, inplace=True)
        
        forecast['stationId'] = local_station
        
        influx_write_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=forecast, data_frame_measurement_name='MOSMIX', data_frame_tag_columns=['stationId'])

def update_rating():
    global influx_query_api
    
    query = """
        import "experimental"

        // Bewerte das Wetter für die Vorhersage
        PROBABILITY_PRECIPITATION_LAST_1H = "wwp"
        PRECIPITATION_DURATION = "drr1"
        PROBABILITY_DRIZZLE_LAST_1H = "wwz"
        PROBABILITY_FOG_LAST_1H = "wwm"
        WIND_GUST_MAX_LAST_1H = "fx1"
        SUNSHINE_DURATION = "sund1"
        TEMPERATURE_DEW_POINT_MEAN_200 = "td"
        ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_MEAN_200 = "e_td"
        TEMPERATURE_AIR_MEAN_200 = "ttt"
        ERROR_ABSOLUTE_TEMPERATURE_AIR_MEAN_200 = "e_ttt"
        CLOUD_COVER_EFFECTIVE = "neff"

        mosmixLarge = 
            from(bucket: "%BUCKET%")
                |> range(start: now(), stop: 14d)
                |> filter(fn: (r) => r._measurement == "MOSMIX")

        maxValues =
            mosmixLarge
                |> filter(
                    fn: (r) =>
                        r._field == PROBABILITY_PRECIPITATION_LAST_1H or
                        r._field == PRECIPITATION_DURATION or
                        r._field == PROBABILITY_DRIZZLE_LAST_1H or 
                        r._field == PROBABILITY_FOG_LAST_1H or
                        r._field == WIND_GUST_MAX_LAST_1H or 
                        r._field == SUNSHINE_DURATION or 
                        r._field == TEMPERATURE_DEW_POINT_MEAN_200 or 
                        r._field == ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_MEAN_200 or 
                        r._field == ERROR_ABSOLUTE_TEMPERATURE_AIR_MEAN_200,
                )
                |> group(columns: ["_measurement", "_field", "_time"])
                |> max()

        minValues =
            mosmixLarge
                |> filter(
                    fn: (r) => 
                        r._field == TEMPERATURE_AIR_MEAN_200 or 
                        r._field == CLOUD_COVER_EFFECTIVE,
                )
                |> group(columns: ["_measurement", "_field", "_time"])
                |> min()

        // Aggregate over all nearby stations (use pessimistic values)
        forecast =
            union(tables: [minValues, maxValues])
                |> keep(columns: ["_time", "_field", "_value"])
                |> group(columns: ["_field"])
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")

        forecast
            |> map(
                fn: (r) => {
                    sunny = r["sund1"] >= 10 * 60
                    overcast = r["sund1"] < 5 * 60

                    clear = r["neff"] < 6.0 / 8.0 * 100.0
                    cloudy = r["neff"] > 7.0 / 8.0 * 100.0

                    dry = r["wwp"] < 40.0 and r["drr1"] < 120 and r["wwz"] < 40.0
                    rainy = r["wwp"] > 45.0 and r["drr1"] > 600 or r["wwz"] > 45.0

                    arid = r["td"] + r["e_td"] < r["ttt"] - r["e_ttt"] and r["wwm"] < 40.0
                    dewy = r["td"] > r["ttt"] or r["wwm"] > 45.0

                    warm = r["ttt"] >= 285.15 // 12 °C
                    cold = r["ttt"] - r["e_ttt"] < 277.15 // 4 °C
                    
                    calm = r["fx1"] < 10
                    windy = r["fx1"] > 11

                    return {
                        _time: r._time,
                        goodWeather: sunny and clear and dry and arid and warm and calm,
                        badWeather: overcast or cloudy or rainy or dewy or cold or windy,
                        closeWindow: dewy or cold or windy,
                        reason:
                            if windy then
                                "💨"
                            else if cold then
                                "❄️"
                            else if dewy then
                                "🌫"
                            else if rainy then
                                "🌧"
                            else if cloudy then
                                "☁️"
                            else if overcast then
                                "⛅"
                            else
                                " ",
                    }
                },
            )
            |> map(fn: (r) => ({r with reason: if r.goodWeather then "☀️" else r.reason}))
            |> experimental.unpivot()
            |> set(key: "_measurement", value: "MOSMIX_RATING")
            |> to(bucket: "%BUCKET%")
    """
    
    influx_query_api.query(org=config['INFLUXDB_ORG'], query=query.replace('%BUCKET%', config['INFLUXDB_BUCKET']))

@background.job(interval = datetime.timedelta(hours = 2))
def update_forecast():
    try:
        get_forecast()
        update_rating()
        logging.info('Wettervorhersage aktualisiert')
        
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')

loop = None
async def main():
    global config
    global loop
    global influx_write_api
    global influx_query_api

    loop = asyncio.get_running_loop()

    influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
    influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    influx_query_api = influx_client.query_api()

    set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
    get_forecast()
    update_rating()

    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()

asyncio.run(main())
