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

from wetterdienst.dwd.forecasts import DWDMosmixParameter, DWDMosmixValues, DWDMosmixType
from wetterdienst.dwd.forecasts.metadata.dates import DWDForecastDate

from pandas import DataFrame

import astral
import astral.sun

parameters = [
        DWDMosmixParameter.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H,
        DWDMosmixParameter.WIND_GUST_MAX_LAST_1H,
        DWDMosmixParameter.SUNSHINE_DURATION,
        DWDMosmixParameter.TEMPERATURE_DEW_POINT_200,
        DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_200,
        DWDMosmixParameter.TEMPERATURE_AIR_200,
        DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_AIR_200,
        DWDMosmixParameter.CLOUD_COVER_EFFECTIVE]

def get_sunscreen_schedule(station_ids, latitude, longitude):
    mosmix = DWDMosmixValues(
            station_id = station_ids,
            mosmix_type = DWDMosmixType.LARGE,
            start_issue = DWDForecastDate.LATEST,
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
            DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_200.value: 'max',
            DWDMosmixParameter.TEMPERATURE_AIR_200.value: 'min',
            DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value: 'max',
            DWDMosmixParameter.CLOUD_COVER_EFFECTIVE.value: 'min'
            })
    
    # Default = leave open
    schedule = DataFrame(False, index = forecast_agg.index, columns=['CLOSE'])
    # Close, if more than 5 Minutes sunshine per hour
    schedule[forecast_agg[DWDMosmixParameter.SUNSHINE_DURATION.value] > 5 * 60] = True
    # Open, if windy
    schedule[forecast_agg[DWDMosmixParameter.WIND_GUST_MAX_LAST_1H.value] > 10] = False
    # Open, if rainy
    schedule[forecast_agg[DWDMosmixParameter.PROBABILITY_PRECIPITATION_GT_0_1_MM_LAST_1H.value] > 40.0] = False
    # Open, if cloudy
    schedule[forecast_agg[DWDMosmixParameter.CLOUD_COVER_EFFECTIVE.value] > 7/8 * 100.0] = False
    # Open, if below 4°C to protect from ice and snow
    schedule[forecast_agg[DWDMosmixParameter.TEMPERATURE_AIR_200.value] - forecast_agg[DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value] < 277.15] = False
    # Open, if not certainly above dew point to protect from moisture
    schedule[forecast_agg[DWDMosmixParameter.TEMPERATURE_DEW_POINT_200.value] + forecast_agg[DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_DEW_POINT_200.value] > forecast_agg[DWDMosmixParameter.TEMPERATURE_AIR_200.value] - forecast_agg[DWDMosmixParameter.ERROR_ABSOLUTE_TEMPERATURE_AIR_200.value]] = False
    
    observer = astral.Observer(latitude = latitude, longitude = longitude)

    # Don't close before sunrise
    sunrise = astral.sun.sunrise(observer)
    schedule.loc[sunrise] = False
    
    # Open at sunset
    sunset = astral.sun.sunset(observer)
    index_after_sunset = schedule.index.where(schedule.index.to_pydatetime() > sunset).min()
    schedule.loc[sunset] = schedule.loc[index_after_sunset]
    schedule.loc[index_after_sunset] = False

    schedule = schedule.sort_index()
    
    return schedule
