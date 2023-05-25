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

import asyncio
import logging
import time
import telegram.ext

application = None
default_chat_id = None
custom_command = None
custom_command_callback = None

async def bot_start(token, chat_id, command = None, command_callback = None):
    global application
    global default_chat_id
    global custom_command
    global custom_command_callback

    default_chat_id = chat_id
    
    application = telegram.ext.Application.builder().token(token).read_timeout(20).get_updates_read_timeout(30).build()

    application.add_error_handler(on_error)

    if command is not None:
        application.add_handler(telegram.ext.CommandHandler(command, on_custom_command))
        
    custom_command = command
    custom_command_callback = command_callback
    
    await application.initialize()
    await application.updater.start_polling()
    await application.start()


async def on_custom_command(update, context: telegram.ext.ContextTypes.DEFAULT_TYPE):
    global custom_command_callback
    
    try:
        if custom_command_callback is not None:
            await custom_command_callback(context.args)

    except:
        logging.exception('Fehler beim Bearbeiten des Kommandos')


async def on_error(update, context: telegram.ext.ContextTypes.DEFAULT_TYPE):
    global application
    global custom_command
    
    logging.exception('Error in telegram bot', exc_info = context.error)
    
    # If the bot is idle for several hours it might happen that it looses
    # connection after a NetworkError.  We can fix that with a simple reconnect
    if isinstance(context.error, telegram.error.NetworkError):
        await application.updater.stop()
        await asyncio.sleep(2)
        await application.updater.start()

async def bot_stop():
    global application

    await application.updater.stop()
    await application.stop()
    await application.shutdown()


async def bot_send(text, chat_id = None):
    global application
    global default_chat_id
    
    if chat_id is None:
        chat_id = default_chat_id
        
    message = await application.bot.send_message(chat_id, text)
    
    return message.message_id

async def bot_delete(message_id, chat_id = None):
    global application
    global default_chat_id
    
    if chat_id is None:
        chat_id = default_chat_id
        
    await application.bot.delete_message(chat_id, message_id)
    
