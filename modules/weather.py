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

"""Module to simplify access to the DWD weather data

@author: Oliver Heimlich <oheim@posteo.de>
"""

from wetterdienst import Wetterdienst

from wetterdienst.provider.dwd.mosmix import DwdMosmixType, DwdForecastDate

from wetterdienst.provider.dwd.radar import DwdRadarValues
from wetterdienst.provider.dwd.radar.metadata import DwdRadarDate, DwdRadarParameter

import astral
import astral.sun

import pandas as pd

import numpy as np

import wradlib as wrl
from osgeo import osr

import datetime

observer = None
proximity_radolan_idx = None
vicinity_radolan_idx = None
local_stations = None

def set_location(latitude, longitude):
    global local_stations
    global proximity_radolan_idx
    global vicinity_radolan_idx
    global observer
    
    # Find 2 local forecast stations
    api = Wetterdienst(provider = 'dwd', network = 'mosmix')
    mosmix_request = api(parameter="large", mosmix_type=DwdMosmixType.LARGE)
    local_stations = mosmix_request.filter_by_rank(latlon=(latitude, longitude), rank=2).values
    
    # Determine local index in the radolan grid
    proj_stereo = wrl.georef.create_osr("dwd-radolan")
    proj_wgs = osr.SpatialReference()
    proj_wgs.ImportFromEPSG(4326)
    radolan_grid_xy = wrl.georef.get_radolan_grid(900, 900)
    coord_xy = wrl.georef.reproject([longitude, latitude], projection_source=proj_wgs, projection_target=proj_stereo)
    distance_xy = np.hypot(radolan_grid_xy[:, :, 0] - coord_xy[0], radolan_grid_xy[:, :, 1] - coord_xy[1])
    proximity_radolan_idx = np.argwhere(distance_xy < 10)
    vicinity_radolan_idx = np.argwhere(distance_xy < 50)
    
    # Define observer for sun position
    observer = astral.Observer(latitude=latitude, longitude=longitude)


def get_sunscreen_schedule():
    global local_stations
    global observer
    
    PROBABILITY_PRECIPITATION_LAST_1H = 'wwp'
    PRECIPITATION_DURATION = 'drr1'
    PROBABILITY_DRIZZLE_LAST_1H = 'wwz'
    PROBABILITY_FOG_LAST_1H = 'wwm'
    PROBABILITY_THUNDER_LAST_1H = 'wwt'
    WIND_GUST_MAX_LAST_1H = 'fx1'
    SUNSHINE_DURATION = 'sund1'
    TEMPERATURE_DEW_POINT_MEAN_200 = 'td'
    ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_MEAN_200 = 'e_td'
    TEMPERATURE_AIR_MEAN_200 = 'ttt'
    ERROR_ABSOLUTE_TEMPERATURE_AIR_MEAN_200 = 'e_ttt'
    CLOUD_COVER_EFFECTIVE = 'neff'
    
    forecast = pd.DataFrame()
    for local_station in local_stations.all().df_stations['station_id']:
        mosmix_forecast = local_stations.read_mosmix_large(station_id = local_station, date = DwdForecastDate.LATEST)
        forecast = pd.concat([forecast, mosmix_forecast.to_pandas()])

    # Aggregate over all stations (use pessimistic values)
    forecast = forecast.groupby('date').agg({
            PROBABILITY_PRECIPITATION_LAST_1H: 'max',
            PRECIPITATION_DURATION: 'max',
            PROBABILITY_DRIZZLE_LAST_1H: 'max',
            PROBABILITY_FOG_LAST_1H: 'max',
            PROBABILITY_THUNDER_LAST_1H: 'max',
            WIND_GUST_MAX_LAST_1H: 'max',
            SUNSHINE_DURATION: 'max',
            TEMPERATURE_DEW_POINT_MEAN_200: 'max',
            ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_MEAN_200: 'max',
            TEMPERATURE_AIR_MEAN_200: 'min',
            ERROR_ABSOLUTE_TEMPERATURE_AIR_MEAN_200: 'max',
            CLOUD_COVER_EFFECTIVE: 'min'
            })

    # Default
    schedule = pd.DataFrame({'WEATHER_PREDICTION': 'ok', 'CLOSE_WINDOW': False, 'REASON': '', 'EXTENDED_REASON': ''}, index = forecast.index, columns=['WEATHER_PREDICTION', 'CLOSE_WINDOW', 'REASON', 'EXTENDED_REASON'])

    not_sunny_idx = forecast[SUNSHINE_DURATION] < 5 * 60
    sunny_idx = forecast[SUNSHINE_DURATION] >= 10 * 60
    schedule.loc[not_sunny_idx, 'REASON'] = '⛅'
    schedule.loc[sunny_idx == False, 'EXTENDED_REASON'] += '⛅'
    good_idx = sunny_idx
    bad_idx = not_sunny_idx

    cloudy_idx = forecast[CLOUD_COVER_EFFECTIVE] > 7/8 * 100.0
    clear_idx = forecast[CLOUD_COVER_EFFECTIVE] < 6/8 * 100.0
    schedule.loc[cloudy_idx, 'REASON'] = '☁️'
    schedule.loc[clear_idx == False, 'EXTENDED_REASON'] += '☁️'
    good_idx &= clear_idx
    bad_idx |= cloudy_idx

    rainy_idx = ((forecast[PROBABILITY_PRECIPITATION_LAST_1H] > 45.0) & (forecast[PRECIPITATION_DURATION] > 600)) | (forecast[PROBABILITY_DRIZZLE_LAST_1H] > 45.0)
    dry_idx = (forecast[PROBABILITY_PRECIPITATION_LAST_1H] < 40.0) & (forecast[PRECIPITATION_DURATION] < 120) & (forecast[PROBABILITY_DRIZZLE_LAST_1H] < 40.0)
    schedule.loc[rainy_idx, 'REASON'] = '🌧'
    schedule.loc[dry_idx == False, 'EXTENDED_REASON'] += '🌧'
    good_idx &= dry_idx
    bad_idx |= rainy_idx

    dewy_idx = (forecast[TEMPERATURE_DEW_POINT_MEAN_200] > forecast[TEMPERATURE_AIR_MEAN_200]) | (forecast[PROBABILITY_FOG_LAST_1H] > 45.0)
    arid_idx = (forecast[TEMPERATURE_DEW_POINT_MEAN_200] + forecast[ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_MEAN_200] < forecast[TEMPERATURE_AIR_MEAN_200] - forecast[ERROR_ABSOLUTE_TEMPERATURE_AIR_MEAN_200]) & (forecast[PROBABILITY_FOG_LAST_1H] < 40.0)
    schedule.loc[dewy_idx, 'REASON'] = '🌫'
    schedule.loc[arid_idx == False, 'EXTENDED_REASON'] += '🌫'
    schedule.loc[dewy_idx, 'CLOSE_WINDOW'] = True
    good_idx &= arid_idx
    bad_idx |= dewy_idx

    cold_idx = forecast[TEMPERATURE_AIR_MEAN_200] - forecast[ERROR_ABSOLUTE_TEMPERATURE_AIR_MEAN_200] < 277.15 # 4 °C
    warm_idx = forecast[TEMPERATURE_AIR_MEAN_200] >= 285.15 # 12 °C
    schedule.loc[cold_idx, 'REASON'] = '❄️'
    schedule.loc[warm_idx == False, 'EXTENDED_REASON'] += '❄️'
    schedule.loc[cold_idx, 'CLOSE_WINDOW'] = True
    good_idx &= warm_idx
    bad_idx |= cold_idx

    windy_idx = forecast[WIND_GUST_MAX_LAST_1H] > 11
    calm_idx = forecast[WIND_GUST_MAX_LAST_1H] < 10
    schedule.loc[windy_idx, 'REASON'] = '💨'
    schedule.loc[calm_idx == False, 'EXTENDED_REASON'] += '💨'
    schedule.loc[windy_idx, 'CLOSE_WINDOW'] = True
    good_idx &= calm_idx
    bad_idx |= windy_idx

    # In summer, there is always a high risk of a thunderstorm, we can't use the forecast
    #
    #thundery_idx = forecast[PROBABILITY_THUNDER_LAST_1H] > 80.0
    #thunderless_idx = forecast[PROBABILITY_THUNDER_LAST_1H] < 70.0
    #schedule.loc[thundery_idx, 'REASON'] = '⛈'
    #schedule.loc[thunderless_idx == False, 'EXTENDED_REASON'] += '⛈'
    #schedule.loc[thundery_idx, 'CLOSE_WINDOW'] = True
    #good_idx &= thunderless_idx
    #bad_idx |= thundery_idx

    schedule[good_idx] = ['good', False, '☀️', '☀️']
    schedule.loc[bad_idx, 'WEATHER_PREDICTION'] = 'bad'

    # Open sunscreen and close window at sunset
    sunset = astral.sun.sunset(observer)
    index_after_sunset = schedule.index.where(schedule.index.to_pydatetime() > sunset).min()
    schedule.loc[sunset] = schedule.loc[index_after_sunset]
    schedule.loc[index_after_sunset] = ['bad', True, '🌙', '🌙']

    schedule = schedule.sort_index()

    return schedule


last_radolan_rain_date = None

def get_current_precipitation():
    global proximity_radolan_idx
    global vicinity_radolan_idx
    global observer
    global last_radolan_rain_date
    
    # RY
    # qualitätsgeprüfte Radardaten nach Abschattungskorrektur
    # und nach Anwendung der verfeinerten Z-R-Beziehungen
    # in Niederschlagshöhen umgerechnet
    #
    # Einheit: 1/100mm
    # zeitliche Auflösung: 5min
    radolan = DwdRadarValues(
        parameter=DwdRadarParameter.RY_REFLECTIVITY,
        start_date=DwdRadarDate.LATEST,
    )
    
    ry_latest = next(radolan.query())
    
    data, attributes = wrl.io.read_radolan_composite(ry_latest.data)

    if last_radolan_rain_date is None or attributes['datetime'] - last_radolan_rain_date > datetime.timedelta(minutes=15):
        # initially and after a period of no rain:
        # at least 5 measurements within a radius of 10km required to detect rain
        threshold = 5
        local_data = data[tuple(proximity_radolan_idx.T.tolist())]

    else:
        # when it is raining:
        # wait until only 2 measurements within a radius of 50km
        # to lower detection jitter
        threshold = 2
        local_data = data[tuple(vicinity_radolan_idx.T.tolist())]

    # Remove values with missing data
    clean_local_data = np.ma.masked_equal(local_data, attributes['nodataflag'])
    
    # Remove values below the desired precision
    clean_local_data = np.ma.masked_less_equal(clean_local_data, threshold * attributes['precision'])

    is_raining = (np.ma.count(clean_local_data) >= threshold)
    
    if is_raining:
        last_radolan_rain_date = attributes['datetime']
    else:
        last_radolan_rain_date = None
    
    return is_raining


def get_next_sunset():
    global observer
    
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    sunset = astral.sun.sunset(observer, now.date())
    
    if sunset < now:
        tomorrow = now.date() + datetime.timedelta(days=1)
        sunset = astral.sun.sunset(observer, tomorrow)
    
    return sunset
