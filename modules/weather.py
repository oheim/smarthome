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

from wetterdienst.provider.dwd.forecast import DwdMosmixType, DwdForecastDate
from wetterdienst.provider.dwd.forecast.metadata import DwdMosmixParameter

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
    api = Wetterdienst(provider = 'dwd', kind = 'forecast')
    stations = api(parameter="large", mosmix_type=DwdMosmixType.LARGE)
    local_stations = stations.filter_by_rank(latitude=latitude, longitude=longitude, rank=2)
    
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
    
    forecast = pd.DataFrame()
    for station_forecast in local_stations.values.read_mosmix_large(DwdForecastDate.LATEST):
        forecast = forecast.append(station_forecast)
    
    # Aggregate over all stations (use pessimistic values)
    forecast = forecast.groupby('datetime').agg({
            DwdMosmixParameter.LARGE.PROBABILITY_PRECIPITATION_LAST_1H.value: 'max',
            DwdMosmixParameter.LARGE.PRECIPITATION_DURATION.value: 'max',
            DwdMosmixParameter.LARGE.PROBABILITY_DRIZZLE_LAST_1H.value: 'max',
            DwdMosmixParameter.LARGE.PROBABILITY_FOG_LAST_1H.value: 'max',
            DwdMosmixParameter.LARGE.PROBABILITY_THUNDERSTORM_LAST_1H.value: 'max',
            DwdMosmixParameter.LARGE.WIND_GUST_MAX_LAST_1H.value: 'max',
            DwdMosmixParameter.LARGE.SUNSHINE_DURATION.value: 'max',
            DwdMosmixParameter.LARGE.TEMPERATURE_DEW_POINT_200.value: 'max',
            DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_200.value: 'max',
            DwdMosmixParameter.LARGE.TEMPERATURE_AIR_200.value: 'min',
            DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value: 'max',
            DwdMosmixParameter.LARGE.CLOUD_COVER_EFFECTIVE.value: 'min'
            })
    
    # Default = leave open
    schedule = pd.DataFrame({'CLOSE': False, 'REASON': '⛅️'}, index = forecast.index, columns=['CLOSE', 'REASON'])
    
    # Close, if more than 5 Minutes sunshine per hour
    sunny_idx = forecast[DwdMosmixParameter.LARGE.SUNSHINE_DURATION.value] > 5 * 60
    schedule[sunny_idx] = [True, '☀️']
    
    cloudy_idx = forecast[DwdMosmixParameter.LARGE.CLOUD_COVER_EFFECTIVE.value] > 7/8 * 100.0
    schedule[cloudy_idx] = [False, '☁️']
    
    dewy_idx = (forecast[DwdMosmixParameter.LARGE.TEMPERATURE_DEW_POINT_200.value] + forecast[DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_200.value] > forecast[DwdMosmixParameter.LARGE.TEMPERATURE_AIR_200.value] - forecast[DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value]) | (forecast[DwdMosmixParameter.LARGE.PROBABILITY_FOG_LAST_1H.value] > 40.0)
    schedule[dewy_idx] = [False, '🌫']
    
    cold_idx = forecast[DwdMosmixParameter.LARGE.TEMPERATURE_AIR_200.value] - forecast[DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value] < 277.15
    schedule[cold_idx] = [False, '❄️']
    
    windy_idx = forecast[DwdMosmixParameter.LARGE.WIND_GUST_MAX_LAST_1H.value] > 10
    schedule[windy_idx] = [False, '💨']
    
    rainy_idx = ((forecast[DwdMosmixParameter.LARGE.PROBABILITY_PRECIPITATION_LAST_1H.value] > 40.0) & (forecast[DwdMosmixParameter.LARGE.PRECIPITATION_DURATION.value] > 120)) | (forecast[DwdMosmixParameter.LARGE.PROBABILITY_DRIZZLE_LAST_1H.value] > 40.0)
    schedule[rainy_idx] = [False, '🌧']
    
    thundery_idx = forecast[DwdMosmixParameter.LARGE.PROBABILITY_THUNDERSTORM_LAST_1H.value] > 40.0
    schedule[thundery_idx] = [False, '⛈']
    
    # Don't close before sunrise
    sunrise = astral.sun.sunrise(observer)
    schedule.loc[sunrise] = [False, '🌙']
    
    # Open at sunset
    sunset = astral.sun.sunset(observer)
    index_after_sunset = schedule.index.where(schedule.index.to_pydatetime() > sunset).min()
    schedule.loc[sunset] = schedule.loc[index_after_sunset]
    schedule.loc[index_after_sunset] = [False, '🌙']
    
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
