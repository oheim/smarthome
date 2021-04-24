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

This script sends the optimal sunscreen position over network (UDP) to a
microcontroller (Arduino with ethernet shield).  The microcontroller supports
manual override with a hardware switch and triggers a remote control to send
radio commands to move the sunscreen.

Find the arduino code in cover-control-arduino.ino.

The optimal sunscreen position is based on a weather forecast, which we
retrieve for a nearby weather station by DWD (Deutscher Wetterdienst).

@author: Oliver Heimlich <oheim@posteo.de>
"""

import socket
import datetime
import time
import timeloop
import logging
import sys
import dotenv

from modules import telegram, weather

hostname = sys.argv[1]
config = dotenv.dotenv_values("Sunscreen-arduino.env")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)

background = timeloop.Timeloop()

schedule = None
@background.job(interval = datetime.timedelta(hours = 2))
def update_schedule():
    global schedule
    global config
    
    try:
        schedule = weather.get_sunscreen_schedule(latitude=float(config['LATITUDE']), longitude=float(config['LONGITUDE']))
        logging.info('Wettervorhersage aktualisiert')
        
    except:
        logging.exception('Fehler beim Abruf der Wetterdaten')


udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
def send_command(command):
    global hostname
    global udp
    
    udp.sendto(bytes(command, 'utf-8'), (socket.gethostbyname(hostname), 8888))

is_closed = None
@background.job(interval = datetime.timedelta(minutes = 1))
def apply_schedule():
    global is_closed
    global schedule
    
    try:
        now = datetime.datetime.now(datetime.timezone.utc).astimezone()
        close_now = schedule[schedule.index.to_pydatetime() > now]['CLOSE'].iloc[0]
        reason = schedule[schedule.index.to_pydatetime() > now]['REASON'].iloc[0]

        if close_now:
            if is_closed == False:
                telegram.bot_send('Die Markise wird ausgefahren ' + reason)
            send_command('close')
        else:
            if is_closed == True:
                telegram.bot_send('Die Markise wird eingefahren ' + reason)
            send_command('open')
        is_closed = close_now
        
    except:
        logging.exception('Fehler beim Anwenden des Plans')        

update_schedule()

telegram.bot_start(token=config['BOT_TOKEN'], chat_id=int(config['CHAT_ID']))

apply_schedule()

background.start()

try:
    while True:
        time.sleep(1)
finally:
    background.stop()
    telegram.bot_stop()
