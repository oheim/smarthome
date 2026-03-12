#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2025 Oliver Heimlich <oheim@posteo.de>
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

"""Subscribe to Bosch Smarthome Controller using Long Polling API
and publish temperature levels and boiler state to InfluxDB

@author: Oliver Heimlich <oheim@posteo.de>
"""


import dotenv
import influxdb_client
import requests
import json
import logging
import urllib3
import time
import atexit
import threading
import sys

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

urllib3.disable_warnings()
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

config = dotenv.dotenv_values("Bosch-Smarthome.env")

devices = {}
rooms = {}
influx_api = None
poll_id = None

# Store last known values
last_values = {}
refresh_interval = int(config.get('BOSCH_SMARTHOME_REFRESH_INTERVAL', 60))


def call_api(path):
    """Synchronous REST API call for initial data loading"""
    global config
    
    headers = {
        'api-version': '3.2',
        'accept': 'application/json'
    }

    try:
        response = requests.get(
            config['BOSCH_SMARTHOME_URL'] + path, 
            headers=headers, 
            cert=(config['BOSCH_SMARTHOME_CLIENT_CERT'], config['BOSCH_SMARTHOME_CLIENT_KEY']), 
            verify=False,
            timeout=10
        )
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Connection error for {path}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout for {path}: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error {e.response.status_code} for {path}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {path}: {e}")
        return None


def json_rpc_call(method, params=None):
    """Make a JSON-RPC call to the Long Polling API"""
    global config
    
    headers = {
        'Content-Type': 'application/json',
        'api-version': '3.2'
    }
    
    payload = [
        {
            "jsonrpc": "2.0",
            "method": method,
            "params": params if params else []
        }
    ]
    
    try:
        response = requests.post(
            config['BOSCH_SMARTHOME_POLL_URL'],
            headers=headers,
            json=payload,
            cert=(config['BOSCH_SMARTHOME_CLIENT_CERT'], config['BOSCH_SMARTHOME_CLIENT_KEY']),
            verify=False,
            timeout=60
        )
        response.raise_for_status()
        result = response.json()
        
        if result and len(result) > 0:
            if 'error' in result[0]:
                logging.error(f"JSON-RPC error: {result[0]['error']}")
                return None
            return result[0].get('result')
        return None
        
    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout in JSON-RPC call to {method}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for JSON-RPC {method}: {e}")
        return None


def load_initial_data():
    """Load device and room information via REST API"""
    global devices, rooms
    
    # Load devices
    devices_list = call_api("devices")
    if devices_list:
        for device in devices_list:
            devices[device['id']] = device
        logging.info(f"Loaded {len(devices)} devices")
    
    # Load rooms
    rooms_list = call_api("rooms")
    if rooms_list:
        for room in rooms_list:
            rooms[room['id']] = room
        logging.info(f"Loaded {len(rooms)} rooms")


def load_initial_measurements():
    """Load initial temperature and boiler state values"""
    global devices, rooms, last_values
    
    logging.info("Loading initial measurements...")
    
    for device_id, device in devices.items():
        try:
            room_id = device.get('roomId')
            room_name = rooms.get(room_id, {}).get('name', 'Unknown')
            device_name = device.get('name', 'Unknown')
            
            # Load temperature for ROOM_CLIMATE_CONTROL devices
            if device.get('deviceModel') == 'ROOM_CLIMATE_CONTROL' and 'TemperatureLevel' in device.get('deviceServiceIds', []):
                temperature_state = call_api(f'devices/{device_id}/services/TemperatureLevel/state')
                if temperature_state and 'temperature' in temperature_state:
                    measurement_key = f"temperature_{room_id}"
                    point = (
                        Point("BOSCH")
                        .tag("unit", "°C")
                        .tag("room", room_name)
                        .field("Temperature", float(temperature_state['temperature']))
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Loaded Temperature {room_name}: {temperature_state['temperature']}°C")
            
            # Load boiler state
            if device.get('deviceModel') == 'BOILER' and 'BoilerHeating' in device.get('deviceServiceIds', []):
                boiler_state = call_api(f'devices/{device_id}/services/BoilerHeating/state')
                if boiler_state and 'heatDemand' in boiler_state:
                    measurement_key = f"boiler_{device_id}"
                    point = (
                        Point("BOSCH")
                        .field("Boiler", boiler_state['heatDemand'] == 'HEAT_DEMAND')
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Loaded Boiler heat demand: {boiler_state['heatDemand'] == 'HEAT_DEMAND'}")
            
            # Load valve positions
            if 'ValveTappet' in device.get('deviceServiceIds', []):
                valve_state = call_api(f'devices/{device_id}/services/ValveTappet/state')
                if valve_state and 'position' in valve_state:
                    measurement_key = f"valve_{device_id}"
                    point = (
                        Point("BOSCH")
                        .tag("unit", "%")
                        .tag("device", device_name)
                        .tag("room", room_name)
                        .field("Valve", int(valve_state['position']))
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Loaded Valve {device_name}: {valve_state['position']}%")
            
            # Load humidity levels
            if 'HumidityLevel' in device.get('deviceServiceIds', []):
                humidity_state = call_api(f'devices/{device_id}/services/HumidityLevel/state')
                if humidity_state and 'humidity' in humidity_state:
                    measurement_key = f"humidity_{device_id}"
                    point = (
                        Point("BOSCH")
                        .tag("unit", "%")
                        .tag("device", device_name)
                        .tag("room", room_name)
                        .field("Humidity", int(humidity_state['humidity']))
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Loaded Humidity {device_name}: {humidity_state['humidity']}%")
        
        except Exception as err:
            logging.exception(f"Failed to load initial measurements for device {device_id}")


def subscribe():
    """Subscribe to device changes via Long Polling"""
    global poll_id
    
    result = json_rpc_call("RE/subscribe", ["com/bosch/sh/remote/*", None])
    if result:
        poll_id = result
        logging.info(f"Successfully subscribed with poll_id: {poll_id}")
        return True
    else:
        logging.error("Failed to subscribe")
        return False


def unsubscribe():
    """Unsubscribe from Long Polling"""
    global poll_id
    
    if poll_id:
        result = json_rpc_call("RE/unsubscribe", [poll_id])
        logging.info("Unsubscribed from Long Polling")
        poll_id = None
        return True
    return False


def process_notification(result):
    """Process incoming notification and update last_values"""
    global devices, rooms, last_values
    
    if not result or not isinstance(result, list):
        return
    
    try:
        for notification in result:
            path = notification.get('path', '')
            
            # Extract device ID and service from path
            path_parts = path.split('/')
            device_id = None
            service_type = None
            
            if 'devices' in path_parts:
                device_idx = path_parts.index('devices')
                if device_idx + 1 < len(path_parts):
                    device_id = path_parts[device_idx + 1]
                if device_idx + 3 < len(path_parts) and path_parts[device_idx + 2] == 'services':
                    service_type = path_parts[device_idx + 3]
            
            if not device_id or device_id not in devices:
                continue
            
            device = devices[device_id]
            room_id = device.get('roomId')
            room_name = rooms.get(room_id, {}).get('name', 'Unknown')
            device_name = device.get('name', 'Unknown')
            
            # Process state changes and update last_values
            if 'state' in notification:
                state = notification['state']
                
                if service_type == 'ValveTappet' and 'position' in state:
                    measurement_key = f"valve_{device_id}"
                    point = (
                        Point("BOSCH")
                        .tag("unit", "%")
                        .tag("device", device_name)
                        .tag("room", room_name)
                        .field("Valve", int(state['position']))
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Updated Valve {device_name}: {state['position']}%")
                
                elif service_type == 'HumidityLevel' and 'humidity' in state:
                    measurement_key = f"humidity_{device_id}"
                    point = (
                        Point("BOSCH")
                        .tag("unit", "%")
                        .tag("device", device_name)
                        .tag("room", room_name)
                        .field("Humidity", int(state['humidity']))
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Updated Humidity {device_name}: {state['humidity']}%")
                
                elif service_type == 'TemperatureLevel' and 'temperature' in state and device.get('deviceModel') == 'ROOM_CLIMATE_CONTROL':
                    measurement_key = f"temperature_{room_id}"
                    point = (
                        Point("BOSCH")
                        .tag("unit", "°C")
                        .tag("room", room_name)
                        .field("Temperature", float(state['temperature']))
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Updated Temperature {room_name}: {state['temperature']}°C")
                
                elif service_type == 'BoilerHeating' and 'heatDemand' in state:
                    measurement_key = f"boiler_{device_id}"
                    point = (
                        Point("BOSCH")
                        .field("Boiler", state['heatDemand'] == 'HEAT_DEMAND')
                    )
                    last_values[measurement_key] = point
                    logging.info(f"Updated Boiler heat demand: {state['heatDemand'] == 'HEAT_DEMAND'}")
    
    except Exception as err:
        logging.exception('Failed to process notification')


def refresh_measurements():
    """Periodically write all last known values to InfluxDB"""
    global last_values, influx_api, refresh_interval
    
    while True:
        try:
            time.sleep(refresh_interval)
            
            if last_values:
                logging.info(f"Writing {len(last_values)} measurements to InfluxDB...")
                for measurement_key, point in last_values.items():
                    try:
                        influx_api.write(bucket=config['INFLUXDB_BUCKET'], org=config['INFLUXDB_ORG'], record=point)
                    except Exception as err:
                        logging.exception(f"Failed to write measurement {measurement_key}")
        
        except Exception as err:
            logging.exception("Unexpected error in refresh thread")
            time.sleep(refresh_interval)


def long_poll():
    """Perform Long Polling with 30 second timeout"""
    global poll_id
    
    if not poll_id:
        return False
    
    result = json_rpc_call("RE/longPoll", [poll_id, 30])
    if result is not None:
        process_notification(result)
        return True
    return False


def main():
    global influx_api
    
    # Initialize InfluxDB connection
    influx_client = influxdb_client.InfluxDBClient(
        url=config['INFLUXDB_URL'], 
        token=config['INFLUXDB_TOKEN'], 
        org=config['INFLUXDB_ORG']
    )
    influx_api = influx_client.write_api(write_options=SYNCHRONOUS)
    
    # Load initial device and room data
    load_initial_data()
    
    # Register unsubscribe to be called on exit
    atexit.register(unsubscribe)
    
    # Start background thread for refreshing measurements
    refresh_thread = threading.Thread(target=refresh_measurements, daemon=True)
    refresh_thread.start()
    logging.info(f"Started refresh thread with {refresh_interval}s interval")
    
    # Subscribe to device changes
    if not subscribe():
        logging.error("Failed to subscribe, exiting")
        return

    # Load initial measurements
    load_initial_measurements()
    
    # Long polling loop
    logging.info("Starting Long Polling loop...")
    while True:
        try:
            if not refresh_thread.is_alive():
                logging.error("Refresh thread died unexpectedly")
                sys.exit(1)
                
            if not long_poll():
                logging.warning("Long poll failed, attempting to reconnect...")
                time.sleep(5)
                if not subscribe():
                    logging.error("Failed to resubscribe")
                    time.sleep(10)
        except KeyboardInterrupt:
            logging.info("Interrupted by user")
            break
        except Exception as err:
            logging.exception("Unexpected error in polling loop")
            time.sleep(5)


if __name__ == "__main__":
    main()