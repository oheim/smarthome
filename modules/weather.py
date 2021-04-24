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

import astral
import astral.sun

import pandas as pd

observer = None
local_stations = None

def set_location(latitude, longitude):
    global local_stations
    global observer
    
    api = Wetterdienst(provider = 'dwd', kind = 'forecast')
    
    stations = api(parameter="large", mosmix_type=DwdMosmixType.LARGE)
    
    local_stations = stations.filter_by_rank(latitude=latitude, longitude=longitude, rank=2)
    observer = astral.Observer(latitude=latitude, longitude=longitude)

def get_sunscreen_schedule(latitude, longitude):
    global local_stations
    global observer
    
    if observer is None or observer.latitude != latitude or observer.longitude != longitude:
        set_location(latitude, longitude)
    
    forecast = pd.DataFrame()
    for station_forecast in local_stations.values.read_mosmix_large(DwdForecastDate.LATEST):
        forecast = forecast.append(station_forecast)
    
    # Aggregate over all stations (use pessimistic values)
    forecast = forecast.groupby('datetime').agg({
            DwdMosmixParameter.LARGE.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H.value: 'max',
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
    
    dewy_idx = forecast[DwdMosmixParameter.LARGE.TEMPERATURE_DEW_POINT_200.value] + forecast[DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_200.value] > forecast[DwdMosmixParameter.LARGE.TEMPERATURE_AIR_200.value] - forecast[DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value]
    schedule[dewy_idx] = [False, '🌫']
    
    cold_idx = forecast[DwdMosmixParameter.LARGE.TEMPERATURE_AIR_200.value] - forecast[DwdMosmixParameter.LARGE.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value] < 277.15
    schedule[cold_idx] = [False, '❄️']
    
    windy_idx = forecast[DwdMosmixParameter.LARGE.WIND_GUST_MAX_LAST_1H.value] > 10
    schedule[windy_idx] = [False, '💨']
    
    rainy_idx = forecast[DwdMosmixParameter.LARGE.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H.value] > 40.0
    schedule[rainy_idx] = [False, '🌧']
    
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
