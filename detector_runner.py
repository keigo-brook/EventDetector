# -*- coding: utf-8 -*-

import os
import json
import paho.mqtt.client as mqtt
# detector
import detector_v1 as detector
from logging import getLogger, StreamHandler, FileHandler, DEBUG
logger = getLogger(__name__)
if not logger.handlers:
    fileHandler = FileHandler(r'./log/detector_runner.log')
    fileHandler.setLevel(DEBUG)
    streamHander = StreamHandler()
    streamHander.setLevel(DEBUG)
    logger.setLevel(DEBUG)
    logger.addHandler(fileHandler)
    logger.addHandler(streamHander)

# MQTT broker server
host = os.getenv('SSS_MQTT_HOST')
port = os.getenv('SSS_MQTT_PORT')

# subscribe topic
data_topic = 'sensor/data'
# publish topic
event_topic = 'sensor/event'


def on_connect(client, data, flags, response_code):
    logger.info('status {0}'.format(response_code))
    client.subscribe(data_topic)


def on_message(client, data, msg):
    logger.info('Received: {0} {1}'.format(msg.topic, msg.payload))
    new_data = msg.payload.split(',')
    event = detector.detect(new_data)

    # イベントが変わっていた場合のみpublish
    if event == None:
        logger.info("Event None")
    elif event['changed']:
        client.publish(event_topic, json.dumps(event))
        logger.info("Publisheed: {0}".format(event))


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
    logger.info("connected")
    client.loop_forever()


if __name__ == '__main__':
    main()
