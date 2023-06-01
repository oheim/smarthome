import time
import paho.mqtt.client as paho
from paho import mqtt

# Receive power measurements for a PV device over MQTT.
# see cover-control-shelly.js

power_history = []

def is_power_above(threshold):
	if len(power_history) < 10:
		return False
	else:
		return min(power_history) > threshold

def is_power_below(threshold):
	if len(power_history) < 10:
		return False
	else:
		return max(power_history) < threshold

def on_message(client, userdata, msg):
	global power_history

	power_measurement = int(msg.payload)
	power_history.append(power_measurement)
	if len(power_history) > 10:
		power_history.pop(0)

client = None
def connect(server, user, password, topic):
	global client

	client = paho.Client(client_id="smarthome", userdata=None, protocol=paho.MQTTv5)
#	client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
	client.username_pw_set(user, password)
	client.connect(server, 1883)
	client.on_message = on_message

	client.subscribe(topic, qos=1)

	client.loop_start()

def disconnect():
	global client
	client.loop_end()
