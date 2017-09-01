# -*- coding: utf-8 -*-

import db
import json
import paho.mqtt.client as mqtt
from datetime import datetime
from sqlalchemy import *
from sqlalchemy.orm import scoped_session, sessionmaker

Event = {
    'normal': 0,
    'caution': 1,
    'alert': 2
}


def detect(data):
    # データからセンサの種類を特定し，データを保存
    sensor = db.get_sensor(data[0], data[1])
    if sensor is None:
        raise ValueError("the sensor is unknown, port: {0}, mac: '{1}' is unknown".format(data[0], data[1]))
    sensor.save_data(data)

    # しきい値を超えていたら強制的に警戒モード
    if sensor.is_over_threshold():
        current_event = Event['alert']
        db.add_event(current_event)
        print('Over the threshold')
        return { 'event': current_event }

    # 検知アルゴリズムを用いて状態判定
    current_event = detect_by_algo()
    db.add_event(current_event)
    # 前回と同じイベントの場合かつ傾斜センサのデータの場合，傾斜センサの閾値選択をする
    previous_event = db.get_previous_event().state
    if current_event is previous_event and data[0] is '52660':
         choose_threshold(data, current_event)

    return { 'event': current_event }


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
    a = soil_sensor.latest().moisture / 100 * (1 + b * sign(soil_sensor.latest_diff()) * soil_sensor.min())

    # y: 最終的なアルゴリズムによる値
    y = a * s
    print("y: {0}".format(y))

    # yをもちいてイベントを決定
    # TODO: 最適な閾値を求める
    if y > 5:
        return Event['alert']
    elif y > 1:
        return Event['caution']
    else:
        return Event['normal']



def choose_threshold(data, current_event):
    sensor_state = data[4]
    if current_event > sensor_state:
        # イベント検知結果よりセンサの検知結果の方が厳しく判定している
        # 閾値をゆるめる方向に進める
        pass
    elif current_event < sensor_state:
        # イベント検知結果よりセンサの検知結果の方がゆるく判定している
        # センサの状態をイベント検知結果にあわせる
        pass


def alpha(tilt_senosor, c=1.0, d=1.0):
    last, last_one = tilt_senosor.data.order_by(db.TiltSensorData.id.desc()).limit(2)
    if last == None or last_one == None:
        return 0.0

    sign = lambda x: 1 if x >= 0.0 else -1.0
    diff_x = last.tilt_x - last_one.tilt_x
    diff_y = last.tilt_y - last_one.tilt_y
    alpha_x = 1.0 + c * abs(last.tilt_x) * (1 + d * sign(diff_x))
    alpha_y = 1.0 + c * abs(last.tilt_y) * (1 + d * sign(diff_y))

    return alpha_x * abs(diff_x) + alpha_y * abs(diff_y)