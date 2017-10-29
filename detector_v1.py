# -*- coding: utf-8 -*-
import os
import db
import json
import paho.mqtt.client as mqtt

from datetime import datetime
from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker
from logging import getLogger, FileHandler, StreamHandler, DEBUG

logger = getLogger(__name__)
if not logger.handlers:
    fileHandler = FileHandler(r'./log/detector_v1.log')
    fileHandler.setLevel(DEBUG)
    streamHander = StreamHandler()
    streamHander.setLevel(DEBUG)
    logger.setLevel(DEBUG)
    logger.addHandler(fileHandler)
    logger.addHandler(streamHander)

Event = {
    'normal': 0L,
    'caution': 1L,
    'alert': 2L
}

FieldTiltSensors = {
    'A': os.getenv('FIELD_TILT_MAC_E'),
    'B': os.getenv('FIELD_TILT_MAC_B'),
    'C': os.getenv('FIELD_TILT_MAC_C'),
    'D': os.getenv('FIELD_TILT_MAC_D'),
    'E': os.getenv('FIELD_TILT_MAC_E'),
}



def detect(data):
    logger.info("########## received {0} ##########\n".format(datetime.now()) + str(data))
    # データからセンサの種類を特定し，データを保存
    if data[0] == '0' and data[3] == '$WIXDR' and float(data[13]) > 30.0:
        current_event = Event['alert']
        changed = db.check_event_changed(current_event)
        sensor = db.get_sensor('52660', FieldTiltSensors['E'])
        if sensor is not None:
            change_sensors_table(sensor, 1)
        sensor = db.get_sensor('52660', FieldTiltSensors['A'])
        if sensor is not None:
            change_sensors_table(sensor, 1)
        db.add_event(current_event, -1)
        logger.info('Weather Over the threshold')
        return { "event": current_event, "changed": changed }
    
        
    sensor = db.get_sensor(data[0], data[1])
    if sensor is None:
        if data[0] == '52660':
            logger.info("New tilt sensor, mac: {0}".format(data[1]))
            db.add_tilt_sensor('tilt sensor', data[1], 10)
        else:
            logger.info("the sensor is unknown, port: {0}, mac: '{1}'".format(data[0], data[1]))
        return None

    sensor.save_data(data)

    # しきい値を超えていたら強制的に警戒モード
    if sensor.is_over_threshold():
        current_event = Event['alert']
        changed = db.check_event_changed(current_event)
        change_sensors_table(sensor, 1)
        db.add_event(current_event, -1)
        logger.info('Over the threshold')
        return { "event": current_event, "changed": changed }

    # 検知アルゴリズムを用いて状態判定
    current_event, y = detect_by_algo(sensor)
    logger.info("event: {0}, y: {1}".format(current_event, y))
    changed = db.check_event_changed(current_event)

    # 前回と同じイベントの場合かつ傾斜センサのデータの場合，傾斜センサの閾値選択をする
    previous_event = db.get_previous_event().state
    if data[0] == '52660':
        logger.info("Event: {0} -> {1}, Sensor state: {2}".format(previous_event, current_event, sensor.latest_node_state()))
        if sensor.is_hysteresis():
            logger.info("Sensor hysteresis at: {0}".format(sensor.hysteresis_at))
        # センサの閾値テーブルを変更
        # if current_event == previous_event:
        #     choose_threshold(sensor, current_event)
       
        # 傾斜センサの状態を変更
        if sensor.latest_node_state() != current_event:
            if current_event == 2:
                change_sensors_table(sensor, 1)
            elif current_event == 1:
                change_sensors_table(sensor, 2)
            elif current_event == 0:
                change_sensors_table(sensor, 8)
    elif data[0] == '52652':
        logger.info("Event: {0} -> {1}, Soil sensor".format(previous_event, current_event))
        
    # イベントを保存
    db.add_event(current_event, y)
    return { "event": current_event, "changed": changed }


def detect_by_algo(sensor):
    """
    最新のデータを用いて検知アルゴリズムによりイベント検知
    """
    # すべてのアクティブな傾斜センサの合計の重みsを計算
    s = 0.0
    delta = 1.0 # 時間間隔
    c = 1.0 # 傾斜センサの重み
    d = 1.0 # 同上
    if sensor.mac == FieldTiltSensors['E']:
        current_alpha = alpha(sensor, c, d)
        s += current_alpha / delta
    else:
        sensor = db.get_sensor('52660', FieldTiltSensors['A'])
        if sensor is not None:
            current_alpha = alpha(sensor, c, d)
            s += current_alpha / delta
        sensor = db.get_sensor('52660', FieldTiltSensors['B'])
        if sensor is not None:
            current_alpha = alpha(sensor, c, d)
            s += current_alpha / delta
        sensor = db.get_sensor('52660', FieldTiltSensors['C'])
        if sensor is not None:
            current_alpha = alpha(sensor, c, d)
            s += current_alpha / delta
        sensor = db.get_sensor('52660', FieldTiltSensors['D'])
        if sensor is not None:
            current_alpha = alpha(sensor, c, d)
            s += current_alpha / delta
        s /= 4

    # a(t): 土中水分による時刻tにおける重みを計算
    soil_sensor = db.get_sensor('52652', os.getenv('SOIL_MAC'))
    b = 1.0 # 土中水分量の重み
    sign = lambda x: 1 if x >= 0.0 else -1.0
    a = soil_sensor.latest().moisture * (1 + b * sign(soil_sensor.latest_diff()) * (soil_sensor.latest().moisture - soil_sensor.min())) / 100.0

    # y: 最終的なアルゴリズムによる値
    y = abs(a * s)
    logger.info("y: {0}, a: {1}, s: {2}".format(y, a, s))

    # yをもちいてイベントを決定
    logger.info("alert threshold : {0}, caution threshold: {1}".format(os.getenv('Y_ALERT_THRESHOLD'), os.getenv('Y_CAUTION_THRESHOLD')))
    if y > float(os.getenv('Y_ALERT_THRESHOLD')):
        logger.info("alert threshold")
        return Event['alert'], y
    elif y > float(os.getenv('Y_CAUTION_THRESHOLD')):
        logger.info("caution threshold")
        return Event['caution'], y
    else:
        return Event['normal'], y


# 本来はFeedbackかけるプロセスと検知するプロセスをわけたいが，とりあえずここでフィードバックをかける
def choose_threshold(sensor, current_event):
    sensor_state = sensor.latest_node_state()
    if current_event < sensor_state:
        # イベント検知結果よりセンサの検知結果の方が厳しく判定している
        # 閾値をゆるめる方向に進める
        loose_threshold(sensor)
    elif current_event > sensor_state:
        # イベント検知結果よりセンサの検知結果の方がゆるく判定している
        # 閾値を厳しくする方向に進める
        bind_threshold(sensor)


def alpha(tilt_sensor, c=1.0, d=1.0):
    sign = lambda x: 1 if x >= 0.0 else -1.0
    last = tilt_sensor.latest_data()
    diff_x, diff_y = tilt_sensor.latest_diff()
    alpha_x = 1.0 + c * abs(last.tilt_x) * (1 + d * sign(diff_x))
    alpha_y = 1.0 + c * abs(last.tilt_y) * (1 + d * sign(diff_y))

    return alpha_x * abs(diff_x) + alpha_y * abs(diff_y)


def loose_threshold(sensor):
    table_id = sensor.latest_table_id()
    if table_id == 0:
        sensor.change_table(4)
    elif table_id == 4:
        sensor.change_table(5)
    elif table_id == 5:
        sensor.change_table(8)
    elif table_id == 8:
        sensor.change_table(9)


def bind_threshold(sensor):
    table_id = sensor.latest_table_id()
    if table_id == 4:
        sensor.change_table(0)
    elif table_id == 5:
        sensor.change_table(4)

    elif table_id == 8:
        sensor.change_table(5)
    elif table_id == 9:
        sensor.change_table(8)


def change_sensors_table(sensor, table_id):
    if sensor.mac == FieldTiltSensors['E']:
        sensor.change_table(table_id)
    else:
        # A~Dすべての状態を変更
        change_group_table(table_id)


def change_group_table(table_id):
    sensor = db.get_sensor('52660', FieldTiltSensors['A'])
    if sensor is None:
        return
    sensor.change_table(table_id)

    sensor = db.get_sensor('52660', FieldTiltSensors['B'])
    if sensor is None:
        return
    sensor.change_table(table_id)

    sensor = db.get_sensor('52660', FieldTiltSensors['C'])
    if sensor is None:
        return
    sensor.change_table(table_id)

    sensor = db.get_sensor('52660', FieldTiltSensors['D'])
    if sensor is None:
        return
    sensor.change_table(table_id)
