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

"""
This script reads the current sunscreen position over network from a
microcontroller (tuya cover device).  If the current suncreen position differs
from the optimal position, we send a command to move the sunscreen.

@author: Oliver Heimlich <oheim@posteo.de>
"""

import tinytuya

def set_cover_device(device_id, hostname, local_key):
    global device
    
    device = tinytuya.CoverDevice(device_id, hostname, local_key)
    device.set_version(3.3)
    device.set_socketRetryLimit(5)


def set_plug_device(device_id, hostname, local_key):
    global device
    
    device = tinytuya.Device(device_id, hostname, local_key)
    device.set_version(3.3)
    device.set_socketRetryLimit(5)


def power_plug(seconds):
    global device
    
    device.turn_on()
    device.set_value(9, seconds)


def close_curtain():
    global device
    
    device.set_value(1, 'close')


def open_curtain():
    global device
    
    device.set_value(1, 'open')
