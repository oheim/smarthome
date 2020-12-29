# smartplug-bot
Telegram bot, which notifies about power consumers

Install dependencies
````
pip3 install python-telegram-bot --upgrade
pip3 install python-dotenv --upgrade
````

Register telegram bot and add it to a new group.

Set up TP-Link Smart Plug.

Create config file `[Device Name].env` (you may copy `Washer.env.template`).

Start monitor script
````
python3 monitor.py [device hostname]
````
