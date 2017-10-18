# -*- coding: utf-8 -*-

import db
import json
import paho.mqtt.client as mqtt

from datetime import datetime
from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker

Event = {
    'normal': 0L,
    'caution': 1L,
    'alert': 2L
}


def detect(data):
    # データからセンサの種類を特定し，データを保存
    sensor = db.get_sensor(data[0], data[1])
    if sensor is None:
        raise ValueError("the sensor is unknown, port: {0}, mac: '{1}'".format(data[0], data[1]))
    sensor.save_data(data)

    # しきい値を超えていたら強制的に警戒モード
    if sensor.is_over_threshold():
        current_event = Event['alert']
        changed = db.check_event_changed(current_event)
        db.add_event(current_event, -1)
        print('Over the threshold')
        return { "event": current_event, "changed": changed }

    # 検知アルゴリズムを用いて状態判定
    current_event, y = detect_by_algo()
    changed = db.check_event_changed(current_event)

    # 前回と同じイベントの場合かつ傾斜センサのデータの場合，傾斜センサの閾値選択をする
    previous_event = db.get_previous_event().state
    print("Event: {0} -> {1}, Sensor state: {2}".format(previous_event, current_event, sensor.latest_node_state()))
    if sensor.is_hysteresis():
        print("Sensor hysteresis at: {0}".format(sensor.hysteresis_at))
    elif data[0] == '52660':
        if current_event == previous_event:
            #choose_threshold(sensor, current_event)
            pass

        if sensor.latest_node_state() != current_event:
            if current_event == 2:
                sensor.change_table(1)
            elif current_event == 1:
                sensor.change_table(2)
            elif current_event == 0:
                sensor.change_table(3)

    db.add_event(current_event, y)
    return { "event": current_event, "changed": changed }


def detect_by_algo():
    """
    最新のデータを用いて検知アルゴリズムによりイベント検知
    """
    # すべての傾斜センサの合計の重みsを計算
    s = 0.0
    tilt_sensors = db.get_all_tilt_sensors()
    delta = 1.0 # 時間間隔
    c = 1.0 # 傾斜センサの重み
    d = 1.0 #
    for tilt_sensor in tilt_sensors:
        current_alpha = alpha(tilt_sensor, c, d)
        s += current_alpha / delta

    # a(t): 土中水分による時刻tにおける重みを計算
    soil_sensor = db.get_soil_sensor()
    b = 1.0 # 土中水分量の重み
    sign = lambda x: 1 if x >= 0.0 else -1.0
    a = soil_sensor.latest().moisture * (1 + b * sign(soil_sensor.latest_diff()) * (soil_sensor.latest().moisture - soil_sensor.min())) / 100.0

    # y: 最終的なアルゴリズムによる値
    y = a * s
    print("y: {0}, a: {1}, s: {2}".format(y, a, s))

    # yをもちいてイベントを決定
    # TODO: 最適な閾値を求める
    if y > 5:
        return Event['alert'], y
    elif y > 2.5:
        return Event['caution'], y
    else:
        return Event['normal'], y


# 本来はFeedbackかける人と検知する人をわけたいが，とりあえずここでフィードバックをかける
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
