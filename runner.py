# -*- coding: utf-8 -*-

import os
import json
import paho.mqtt.client as mqtt
# detector
import detector_v1 as detector


# MQTT broker server
host = os.getenv('SSS_MQTT_HOST')
port = os.getenv('SSS_MQTT_PORT')

# subscribe topic
data_topic = 'sensor/data'
# publish topic
event_topic = 'sensor/event'


def on_connect(client, data, flags, response_code):
    print('status {0}'.format(response_code))
    client.subscribe(data_topic)


def on_message(client, data, msg):
    data = msg.payload.split(',')
    event = detector.detect(data)
    client.publish(event_topic, json.dumps(event))
    print('Received: {0} {1}'.format(msg.topic, msg.payload))
    print("Event: {0}".format(event))


def main():
    """
    subscribe: sensor/data
    publish: sensor/event

    データを受信後，イベント検知してパブリッシュ
    """

    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(host, port=port, keepalive=60)
    client.loop_forever()


if __name__ == '__main__':
    main()
