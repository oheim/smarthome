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

"""Module to simplify the telegram bot api

@author: Oliver Heimlich <oheim@posteo.de>
"""


import logging
import time
import telegram.ext

updater = None
default_chat_id = None
custom_command_callback = None

def bot_start(token, chat_id, custom_command = None, command_callback = None):
    global updater
    global default_chat_id
    global custom_command_callback

    default_chat_id = chat_id
    
    updater = telegram.ext.Updater(token)

    updater.dispatcher.add_error_handler(on_error)

    if custom_command is not None:
        updater.dispatcher.add_handler(telegram.ext.CommandHandler(custom_command, on_custom_command))
        custom_command_callback = command_callback
    
    updater.start_polling()


def on_custom_command(update, context):
    global custom_command_callback
    
    try:
        if custom_command_callback is not None:
            custom_command_callback(context.args)

    except:
        logging.exception('Fehler beim Bearbeiten des Kommandos')


def on_error(update, context):
    global updater
    
    logging.exception('Error in telegram bot', exc_info = context.error)
    
    # If the bot is idle for several hours it might happen that it looses
    # connection after a NetworkError.  We can fix that with a simple reconnect
    if isinstance(context.error, telegram.error.NetworkError):
        updater.stop()
        time.sleep(2)
        updater.start_polling()
    

def bot_stop():
    global updater
    updater.stop()


def bot_send(text, chat_id = None):
    global default_chat_id
    
    if chat_id is None:
        chat_id = default_chat_id
        
    message = updater.bot.send_message(chat_id, text)
    
    return message.message_id

def bot_delete(message_id, chat_id = None):
    global default_chat_id
    
    if chat_id is None:
        chat_id = default_chat_id
        
    updater.bot.delete_message(chat_id, message_id)
    