# smartplug-bot
Telegram bot, which notifies about power consumers

Download code
````
git clone https://github.com/oheim/smartplug-bot.git
cd smartplug-bot
git submodule update --init
````

Install dependencies
````
pip3 install python-telegram-bot --upgrade
pip3 install python-dotenv --upgrade
````

Register telegram bot and add it to a new group.

Create config file `[Device Name].env` (you may copy `Washer.env.template`).

Start monitor script
````
python3 energy-monitor.py [device hostname]
````
