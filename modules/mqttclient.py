import time
import string
import random
import paho.mqtt.client as paho
from paho import mqtt

# Receive power measurements for a PV device over MQTT.
# see cover-control-shelly.js

power_history = []

def is_power_above(threshold):
	if len(power_history) == 0:
		return False
	else:
		return min(power_history) > threshold

def is_power_below(threshold):
	if len(power_history) == 0:
		return False
	else:
		return max(power_history) < threshold

def on_message(client, userdata, msg):
	global power_history

	power_measurement = int(msg.payload)
	power_history.append(power_measurement)
	if len(power_history) > 10:
		power_history.pop(0)

# Send commands to shelly devices over MQTT
# see https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Mqtt/#mqtt-control

def shelly_command(topic_prefix, component_id, command):
	global client

	topic = "%s/command/%s" % (topic_prefix, component_id)
	client.publish(topic, command, qos=1)


client = None
def connect(server, user, password, topic, message_callback = None):
	global client

	client_id = 'smarthome-' + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
	client = paho.Client(client_id=client_id, userdata=None, protocol=paho.MQTTv5)
#	client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
	client.username_pw_set(user, password)
	client.connect(server, 1883)
	if message_callback is None:
		client.on_message = on_message
	else:
		client.on_message = message_callback

	client.subscribe(topic, qos=1)

	client.loop_start()

def disconnect():
	global client
	client.loop_end()
