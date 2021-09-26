#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2020-2021 Oliver Heimlich <oheim@posteo.de>
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

"""Monitors an electric consumer for the end of operation

This script reads the current power consumption over network from a smart
switch (TP-Link Smartplug).  If we detect that the consumer, e. g., washing
machine, no longer consumes a lot of power, we send a telegram message.

@author: Oliver Heimlich <oheim@posteo.de>
"""

import subprocess
import json
import sys
import time
import logging
import locale
import dotenv

from modules import telegram

hostname = sys.argv[1]

locale.setlocale(locale.LC_ALL, 'de_DE.utf8')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

def call_smartplug(hostname, command):
    while True:
        try:
            proc = subprocess.Popen((['./tplink-smartplug/tplink_smartplug.py', '-t', hostname, '-c', command, '-q']),
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.STDOUT)
        
            proc_out, _ = proc.communicate()
            
            logging.debug("Response from smartplug: %s", proc_out)
            
            return json.loads(proc_out)
        
        except (json.decoder.JSONDecodeError):
            logging.error('Failed to communicate w/ smartplug: %s ', proc_out)
            logging.info('Trying again...')
            time.sleep(1.0)
            continue

def read_info(hostname):
    info = call_smartplug(hostname, 'info')
    sysinfo = info['system']['get_sysinfo']
    alias = sysinfo['alias']
    return alias

def read_emeter(hostname):
    energy = call_smartplug(hostname, 'energy')

    emeter_realtime = energy['emeter']['get_realtime']
    power_mw = emeter_realtime['power_mw']
    total_wh = emeter_realtime['total_wh']

    return power_mw, total_wh

def read_emeter_bulk(hostname, n):
    power_mw_bulk = []
    total_wh_bulk = []
    
    for i in range(n):
        if i > 0:
            time.sleep(1.0)
            
        power_mw, total_wh = read_emeter(hostname)
        power_mw_bulk.append(power_mw)
        total_wh_bulk.append(total_wh)
    
    return power_mw_bulk, total_wh_bulk

def detect_activity(hostname):
    power_mw_bulk, total_wh_bulk = read_emeter_bulk(hostname, 5)
    
    if max(power_mw_bulk) > 5000: # 5W
        activity = True
    else:
        activity = False
        
    return activity, total_wh_bulk[1]

def wait_for_state(hostname, target_state):
    while True:
        current_state, total_wh = detect_activity(hostname)
        if current_state == target_state:
            break
    
    return total_wh

def wait_full_cycle(hostname, cost_per_kwh, message):
    while True:
        while True:
            total_wh_start = wait_for_state(hostname, True)
            time_start = time.time()
            
            # The dryer might activate the motor for 10 seconds
            # after finishing a cycle
            # to prevent moisture while the laundry cools down.
            #
            # We don't want to falsely detect this as the start
            # of a new cycle.
            time.sleep(10.0)
            still_active = detect_activity(hostname)
            if still_active:
                break
        
        started_message_id = telegram.bot_send(text=message)
        
        total_wh_stop = wait_for_state(hostname, False)
        time_stop = time.time()
    
        telegram.bot_delete(message_id=started_message_id)
    
        cycle_duration = time_stop - time_start
        if cycle_duration < 10 * 60: # 10min
            logging.warning("Gerät war weniger als 10 Minuten eingeschaltet")
            continue

        cycle_wh = total_wh_stop - total_wh_start
        cycle_cost = cycle_wh * cost_per_kwh / 1000
        
        return locale.currency(cycle_cost)

devicename = read_info(hostname)

config = dotenv.dotenv_values(devicename + ".env")

telegram.bot_start(token=config['BOT_TOKEN'], chat_id=int(config['CHAT_ID']))

try:
    while True:
        cycle_cost = wait_full_cycle(hostname, float(config['POWER_COST']), config['STARTED_MESSAGE_TEMPLATE'])
        message = config['DONE_MESSAGE_TEMPLATE'].format(cycle_cost)
        logging.info(message)
        telegram.bot_send(text=message)
finally:
    telegram.bot_stop()
