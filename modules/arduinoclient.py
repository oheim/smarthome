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
This script sends the optimal sunscreen position over network (UDP) to a
microcontroller (Arduino with ethernet shield).  The microcontroller supports
manual override with a hardware switch and triggers a remote control to send
radio commands to move the sunscreen.

Find the arduino code in cover-control-arduino.ino.

@author: Oliver Heimlich <oheim@posteo.de>
"""

import socket

def set_address(hostname, port):
    global udp
    
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.connect((socket.gethostbyname(hostname), port))


def close_curtain():
    send_command('curtain close')


def open_curtain():
    send_command('curtain open')


def send_command(command):
    global udp
    
    udp.send(bytes(command, 'utf-8'))
