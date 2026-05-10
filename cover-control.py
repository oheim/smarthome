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
import asyncio
import json

import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision


from modules import weather, arduinoclient, telegram, mqttclient

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

config = dotenv.dotenv_values("Sunscreen.env")
weather.set_location(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
arduinoclient.set_address(hostname=config['ARDUINO_HOSTNAME'], port=int(config['ARDUINO_PORT']))


background = timeloop.Timeloop()

schedule = None
def update_schedule():
    global schedule
    global config
    global radar_rain
    global influx_api

    query = """
      from(bucket: "%BUCKET%")
        |> range(start: now(), stop: 1d)
        |> filter(fn: (r) => r._measurement == "MOSMIX_RATING")
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
        |> limit(n: 3)
    """

    try:
      result = influx_api.query(org=config['INFLUXDB_ORG'], query=query.replace('%BUCKET%', config['INFLUXDB_BUCKET']))
      for table in result:
          new_schedule = []
          for record in table.records:
              new_schedule.append(record.values)
          schedule = new_schedule
          return
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')


cooling_mode = None
next_sunrise = None
def prepare_cooling_mode():
    global config
    global influx_api
    global cooling_mode
    global next_sunrise

    query = """
      maxTemperature =
          from(bucket: "%BUCKET%")
              |> range(start: now(), stop: 1d)
              |> filter(fn: (r) => r._measurement == "MOSMIX")
              |> filter(fn: (r) => r._field == "ttt")
              |> max()
              |> group(columns: ["_measurement", "_field"])
              |> min()
              |> map(fn: (r) => ({r with _value: r._value - 273.15}))

      totalSunDuration =
          from(bucket: "%BUCKET%")
              |> range(start: now(), stop: 1d)
              |> filter(fn: (r) => r._measurement == "MOSMIX")
              |> filter(fn: (r) => r._field == "sund1")
              |> sum()
              |> group(columns: ["_measurement", "_field"])
              |> min()

      union(tables: [maxTemperature, totalSunDuration])
    """

    forecast = {}
    try:
      result = influx_api.query(org=config['INFLUXDB_ORG'], query=query.replace('%BUCKET%', config['INFLUXDB_BUCKET']))
      for table in result:
          for record in table.records:
              forecast[record.get_field()] = record.get_value()
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')

    cooling_mode = forecast['ttt'] >= 24.0 or (forecast['ttt'] >= 20.0 and forecast['sund1'] >= 8 * 60 * 60)
    next_sunrise = weather.get_next_sunrise()


async def activate_ventilation():
    global cooling_mode
    global last_processed_sunset
    
    if cooling_mode is None or not cooling_mode:
        return
    
    # wait until 2 hours after sunset for reduced insect activity
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    if last_processed_sunset is None or now < last_processed_sunset + datetime.timedelta(hours=2):
        return

    # don't open shortly before sunrise
    sunrise = weather.get_next_sunrise()
    if now > sunrise - datetime.timedelta(hours = 1):
        return

    
    query = """
          import "math"
          import "experimental"

          outside =
              from(bucket: "%BUCKET%")
                  |> range(start: now(), stop: 1h)
                  |> filter(fn: (r) => r._measurement == "MOSMIX")
                  |> filter(fn: (r) => r._field == "td" or r._field == "ttt")
                  |> group(columns: ["_measurement", "_field"])
                  |> max()
                  |> group()
                  |> map(fn: (r) => ({r with _value: r._value - 273.15}))
                  |> keep(columns: ["_measurement", "_field", "_value", "_time"])

          inside = 
              from(bucket: "%BUCKET%")
                  |> range(start: -5m)
                  |> filter(fn: (r) => r._measurement == "SHELLY")
                  |> filter(fn: (r) => r.addr == "%SHELLY_ADDR%")
                  |> last()
                  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                  |> map(fn: (r) => {
                    a = 17.27
                    b = 237.7
                    alpha = (a * r.Temperature) / (b + r.Temperature) + math.log(x: float(v: r.Humidity) / 100.0)
                    return {r with DewPoint: b * alpha / (a - alpha)}
                  })
                  |> experimental.unpivot()

          union(tables: [inside, outside])
    """
    try:
      result = influx_api.query(org=config['INFLUXDB_ORG'], query=query.replace('%BUCKET%', config['INFLUXDB_BUCKET']).replace("%SHELLY_ADDR%", config['SHELLY_SENSOR']))
      inside = {}
      outside = {}
      for table in result:
          for record in table.records:
              if record.get_measurement() == "MOSMIX":
                  outside[record.get_field()] = record.get_value()
              else:
                  inside[record.get_field()] = record.get_value()
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')
    
    if len(inside) < 3 or len(outside) < 2:
        logging.error("Fehlende Messwerte / Wetter-Vorhersage")
        return
    
    if inside["Temperature"] < outside["ttt"] + 1:
        return
    
    if inside["DewPoint"] < outside["td"]:
        return

    logging.info('Fenster werden automatisch geöffnet')
    await telegram.bot_send('Die Fenster werden geöffnet 🌬️')
    arduinoclient.open_window()
    window_is_closed = False
    dont_close_window_until = now + datetime.timedelta(minutes = 10)


radar_rain = None
@background.job(interval = datetime.timedelta(minutes = 5))
def update_radar():
    global radar_rain
    global schedule
    global config
    
    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        soon = now + datetime.timedelta(minutes = 10)
        if soon <= schedule[0]["_time"]:
          soon_idx = 0
        else:
          soon_idx = 1
        close_now = not schedule[0]['badWeather']
        close_soon = not schedule[soon_idx]['badWeather']
        
        if close_now or close_soon:
            radar_rain = weather.get_current_precipitation()
        else:
            # The screen is not closed. No need to query the radar.
            radar_rain = None
        
    except:
        logging.exception('Fehler beim Abruf der Radar-Daten')
        radar_rain = None

@background.job(interval = datetime.timedelta(minutes = 1))
def bg_apply_schedule():
    global loop
    asyncio.run_coroutine_threadsafe(apply_schedule(), loop)

def sun_is_shining():
    global config
    return is_power_above(int(config['PV_PEAK_POWER']) / 4)

def sun_is_not_shining():
    global config
    return is_power_below(int(config['PV_PEAK_POWER']) / 8)

is_closed = None
window_is_closed = None
dont_close_window_until = None
last_processed_sunset = None
async def apply_schedule():
    global config
    global is_closed
    global window_is_closed
    global schedule
    global radar_rain
    global dont_close_window_until
    global last_processed_sunset
    global next_sunrise

    try:
        update_schedule()
        
        reason = schedule[0]['reason']

        if is_closed:
            close_now = not schedule[0]['badWeather']
        else:
            # To prevent unnecessary movement:
            # If the sunscreen will be opened in the next two time frames, we don't close it.
            close_now = schedule[0]['goodWeather'] and schedule[1]['goodWeather'] and not schedule[2]['badWeather']

        # The forecast might be incorrect or outdated.
        # If the radar detects unexpected precipitation, we must open the suncreen.
        if radar_rain:
            close_now = False
            reason = '🌦'

        close_window_now = radar_rain or schedule[0]['closeWindow']
        
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        sunset = weather.get_sunset()
        if now > sunset:
            close_now = False
            reason = "🌙"
            if last_processed_sunset != sunset and dont_close_window_until is None:
                close_window_now = True
                prepare_cooling_mode()
                last_processed_sunset = sunset


        if dont_close_window_until is not None and dont_close_window_until > now:
            close_window_now = False
        else:
            close_window_reason = reason
            
            if next_sunrise is not None and now > next_sunrise:
                close_window_now = True
                close_window_reason = "🌄"
                next_sunrise = None

            if not close_window_now and window_is_closed:
                activate_ventilation()

        if window_is_closed:
            # nothing to do
            close_window_now = False
            

        # The sunscreen should be open during low irradiation.
        # An open window may stay open.
        #
        # TODO: In summer we need to control each direction differently
        # if close_now and not is_closed and not sun_is_shining():
        #     close_now = False
        #     reason = '🌅'
        #     extended_reason += '🌅'
        # if close_now and is_closed and sun_is_not_shining():
        #     close_now = False
        #     reason = '🌄'
        #     extended_reason += '🌅'

        logging.info('Status: {}'.format(reason))

        if not (is_closed == close_now):
            if close_now:
                logging.info('Markise wird ausgefahren %s', reason)
                arduinoclient.close_curtain()
                mqttclient.shelly_command(config['COVER_CONTROL_DEVICE_ID'], config['COVER_CONTROL_COMPONENT_ID'], 'close')
                if is_closed is not None:
                    await telegram.bot_send('Die Markise wird ausgefahren {}'.format(reason))
            else:
                logging.info('Markise wird eingefahren %s', reason)
                arduinoclient.open_curtain()
                mqttclient.shelly_command(config['COVER_CONTROL_DEVICE_ID'], config['COVER_CONTROL_COMPONENT_ID'], 'open')
                if is_closed is not None:
                    if close_window_now and window_is_closed is not None:
                        await telegram.bot_send('Die Markise wird eingefahren und die Fenster werden geschlossen {}'.format(reason))
                        window_is_closed = None # skip second telegram message below
                    else:
                        await telegram.bot_send('Die Markise wird eingefahren {}'.format(reason))
            is_closed = close_now

        if close_window_now:
            logging.info('Fenster werden automatisch geschlossen {}'.format(close_window_reason))
            arduinoclient.close_window()
            if window_is_closed is not None:
                await telegram.bot_send(text='Die Fenster werden geschlossen {}'.format(close_window_reason))
            window_is_closed = True
            dont_close_window_until = None

    except:
        logging.exception('Fehler beim Anwenden des Plans')


async def on_window_command(command, args):
    global window_is_closed
    global dont_close_window_until


    if command == 'fenster_auf':
        logging.info('Fenster werden geöffnet')
        dont_close_window_until = datetime.datetime.now(datetime.timezone.utc).astimezone() + datetime.timedelta(minutes = 10)
        arduinoclient.open_window()
        window_is_closed = False
        await telegram.bot_send(text='Die Fenster werden geöffnet')

    if command == 'fenster_zu':
        logging.info('Fenster werden geschlossen')
        arduinoclient.close_window()
        window_is_closed = True
        dont_close_window_until = None
        await telegram.bot_send(text='Die Fenster werden geschlossen')

# Receive power measurements for a PV device over MQTT.
# see cover-control-shelly.js

power_history = []

def is_power_above(threshold):
        if len(power_history) == 0:
                return False
        else:
                return min(power_history) > threshold

def is_power_below(threshold):
        if len(power_history) == 0:
                return False
        else:
                return max(power_history) < threshold

def on_power_measurement(power_measurement):
        global power_history

        power_history.append(power_measurement)
        if len(power_history) > 10:
                power_history.pop(0)

## Receive BLE button commands

def on_ble_event(payload):
        global is_closed
        global window_is_closed
        global dont_close_window_until

        if payload['addr'] == '7c:c6:b6:64:dc:ee':
                if payload['Button'] == 1024:
                        logging.info('BLE: Fenster auf')
                        dont_close_window_until = datetime.datetime.now(datetime.timezone.utc).astimezone() + datetime.timedelta(minutes = 10)
                        arduinoclient.open_window()
                        window_is_closed = False
                if payload['Button'] == 512:
                        logging.info('BLE: Fenster zu')
                        arduinoclient.close_window()
                        window_is_closed = True
                        dont_close_window_until = None
                if payload['Button'] == 256:
                        logging.info('BLE: Markise öffnen')
                        arduinoclient.open_curtain()
                        is_closed = False
                if payload['Button'] == 128:
                        logging.info('BLE: Markise schließen')
                        arduinoclient.close_curtain()
                        is_closed = True


def on_message(client, userdata, msg):
        if msg.topic == 'shellies/ble':
                on_ble_event(json.loads(msg.payload))
        else:
                on_power_measurement(int(msg.payload))


loop = None
influx_api = None
async def main():
    global config
    global loop
    global influx_api

    loop = asyncio.get_running_loop()

    mqttclient.connect(server=config['MQTT_SERVER'], user=config['MQTT_USER'], password=config['MQTT_PASSWORD'], message_callback=on_message)
    mqttclient.subscribe(config['MQTT_TOPIC'])
    mqttclient.subscribe('shellies/ble')

    influx_client = influxdb_client.InfluxDBClient(url=config['INFLUXDB_URL'], token=config['INFLUXDB_TOKEN'], org=config['INFLUXDB_ORG'])
    influx_api = influx_client.query_api()

    await telegram.bot_start(token=config['TELEGRAM_BOT_TOKEN'], chat_id=config['TELEGRAM_CHAT_ID'], commands=['fenster_auf', 'fenster_zu'], command_callback=on_window_command)

    update_schedule()
    prepare_cooling_mode()

    await apply_schedule()

    background.start()

    try:
        while True:
            await asyncio.sleep(60)
    finally:
        background.stop()
        mqttclient.disconnect()
        await telegram.bot_stop()

asyncio.run(main())
