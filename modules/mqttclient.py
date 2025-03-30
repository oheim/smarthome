import time
import string
import random
import paho.mqtt.client as paho
from paho import mqtt

# Send commands to shelly devices over MQTT
# see https://shelly-api-docs.shelly.cloud/gen2/ComponentsAndServices/Mqtt/#mqtt-control

def shelly_command(topic_prefix, component_id, command):
	global client

	topic = "%s/command/%s" % (topic_prefix, component_id)
	client.publish(topic, command, qos=1)


client = None
def connect(server, user, password, message_callback = None):
	global client

	client_id = 'smarthome-' + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))
	client = paho.Client(client_id=client_id, userdata=None, protocol=paho.MQTTv5)
#	client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
	client.username_pw_set(user, password)
	client.connect(server, 1883)
	if message_callback is not None:
		client.on_message = message_callback

	client.loop_start()


def subscribe(topic):
	global client
	client.subscribe(topic, qos=1)


def disconnect():
	global client
	client.loop_end()
