#-*- coding: utf-8 -*-

import os
import math
import requests
import json
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker, object_session
from sqlalchemy.sql import func
from datetime import datetime, timedelta

engine = create_engine("mysql+mysqldb://{0}:{1}@{2}/?charset=utf8"
                       .format(os.getenv('SSS_DB_USER'), os.getenv('SSS_DB_PASS'), os.getenv('SSS_DB_HOST')),
                       echo=False, pool_recycle=3600)
engine.execute("CREATE DATABASE IF NOT EXISTS social_sensor_server")
engine.execute("USE social_sensor_server;")

Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()


class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    state = Column(Integer)
    created_at = Column(DateTime, default=datetime.now())


class TiltSensor(Base):
    __tablename__ = 'tilt_sensors'
    id = Column(Integer, primary_key=True)
    name = Column(String(20))
    threshold = Column(Float)
    mac = Column(String(32))
    data = relationship('TiltSensorData', backref='sensor', lazy='dynamic')
    hysteresis_at = Column(DateTime, default=datetime.now() - timedelta(hours=3))


    def save_data(self, data):
        add_tilt_data(self.id, data)


    def is_over_threshold(self):
        latest = self.latest_data()
        return latest.tilt_x > self.threshold or latest.tilt_y > self.threshold


    def latest_data(self):
        return self.data.order_by(TiltSensorData.id.desc()).first()


    def latest_diff(self):
        last_two = self.data.order_by(TiltSensorData.id.desc()).limit(2).all()
        if len(last_two) < 2:
            return 0.0, 0.0

        diff_x = last_two[0].tilt_x - last_two[1].tilt_x
        diff_y = last_two[0].tilt_y - last_two[1].tilt_y

        return diff_x, diff_y


    def is_hysteresis(self):
        return self.hysteresis_at > datetime.now() - timedelta(hours=2)


    def latest_node_state(self):
        return self.latest_data().node_state


    def latest_table_id(self):
        return self.latest_data().table_id


    def change_table(self, val):
        print("change sensor {0} status from {1} to {2}".format(self.id, self.latest_node_state, val))
        url = os.getenv('SSS_DB_HOST')
        data = {
            'TiltPattarnCode': [
                { 'DeviceId': self.mac, 'Val': val }
            ]
        }

        response = requests.post(
            url,
            json.dumps(data),
            cert=('./client.crt', './client.key'),
            verify=False,
            headers={'Content-Type': 'application/json'}
        )

        print("{0}".format(response.json()))

        return response.json()



class TiltSensorData(Base):
    __tablename__ = 'tilt_sensor_data'
    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer,
                       ForeignKey('tilt_sensors.id'))
    received_at = Column(DateTime)
    node_state = Column(Integer)
    battery_voltage = Column(Float)
    observed_at = Column(Integer)
    tilt_x = Column(Float)
    tilt_y = Column(Float)
    tempereture = Column(Float)
    table_id = Column(Integer)


class SoilSensor(Base):
    __tablename__ = 'soil_sensors'
    id = Column(Integer, primary_key=True)
    name = Column(String(20))
    threshold = Column(Float)
    mac = Column(String(32))
    data = relationship('SoilSensorData', backref='sensor', lazy='dynamic')


    def save_data(self, data):
        add_soil_data(self.id, data)


    def is_over_threshold(self):
        latest = self.latest()
        latest.moisture > self.threshold


    def latest(self):
        latest = self.data.order_by(SoilSensorData.id.desc()).first()
        return latest


    def latest_diff(self):
        last_two = self.data.order_by(SoilSensorData.id.desc()).limit(2).all()
        if len(last_two) < 2:
            return 0.0

        return last_two[0].moisture - last_two[1].moisture


    def min(self):
        min_moisture = self.data.order_by(SoilSensorData.moisture.asc()).first()
        return min_moisture.moisture


class SoilSensorData(Base):
    __tablename__ = 'soil_sensor_data'
    id = Column(Integer, primary_key=True)
    sensor_id = Column(Integer,
                       ForeignKey('soil_sensors.id'))
    received_at = Column(DateTime)
    command_id = Column(Integer)
    sensor_type_id = Column(Integer)
    data_size = Column(Integer)
    data_get_at = Column(String(20))
    data_type = Column(Integer)
    tempereture = Column(Float)
    moisture = Column(Float)
    ec = Column(Float)


def create_table():
    Base.metadata.create_all(bind=engine)


def drop_table():
    Base.metadata.drop_all(bind=engine)


def reset_table():
    drop_table()
    create_table()

def add_tilt_sensor(name, mac, threshold):
    new_sensor = TiltSensor(name=name, mac=mac.upper(), threshold=threshold)
    session = Session()
    session.add(new_sensor)
    session.commit()


# id, mac, receive time, node id, node state, battery voltage, number of observed data,
# [observed time, tilt axis x, tilt axis y, sensor unit temperetur, tilt threshold] * number of observed data
def add_tilt_data(sid, data):
    num_of_data = int(data[6])
    for i in range(num_of_data):
        new_data = TiltSensorData(
            sensor_id=sid,
            received_at=data[2],
            node_state=int(data[4]),
            battery_voltage=float(data[5]),
            observed_at=data[7 + 5 * i],
            tilt_x=float(data[8 + 5 * i]),
            tilt_y=float(data[9 + 5 * i]),
            tempereture=float(data[10 + 5 * i]),
            table_id=int(data[11 + 5 * i])
        )
        session.add(new_data)
    session.commit()


def add_soil_sensor(name, mac, threshold):
    new_sensor = SoilSensor(name='test', mac='test', threshold=0)
    session.add(new_sensor)
    session.commit()


# id, mac, received_at, command_id, sensor_type_id, data_size, data_get_time, data_type, tempereture, moisture, ec
def add_soil_data(sid, data):
    new_data = SoilSensorData(
        sensor_id=sid,
        received_at=data[2],
        command_id=int(data[3]),
        sensor_type_id=int(data[4]),
        data_size=int(data[5]),
        data_get_at=data[6],
        data_type=int(data[7]),
        tempereture=float(data[8]),
        moisture=float(data[9]),
        ec=float(data[10])
    )
    session.add(new_data)
    session.commit()


def add_event(event):
    new_event = Event(state=event)
    session.add(new_event)
    session.commit()



def create_test_sensor():
    add_tilt_sensor('tilt test1', '00:1d:12:90:00:03:a5:13', 10)
    add_soil_sensor('soil test1', '10:50:c2:ff:fe:dc:2f:01', 50)


def create_test_data():
    tilt_data = [
        '52660',
        '00:1d:12:90:00:03:a5:13',
        '170302185339',
        '001d12900003a513',
        '02',
        '32.767',
        '1',
        '541796019',
        '32.767',
        '16',
        '50',
        '1'
    ]
    soil_data = [
        '52652',
        '10:50:c2:ff:fe:dc:2f:01',
        '161109115009',
        '4002',
        '1001',
        '14',
        '000101004928',
        '0003',
        '-5.45',
        '8.83',
        '0'
    ]
    add_tilt_data(1, tilt_data)
    add_soil_data(1, soil_data)


def get_sensor(type_id, mac):
    current_sensor = None
    if type_id == '52660': # tilt sensor
        current_sensor = session.query(TiltSensor).filter(TiltSensor.mac==mac).first()
    elif type_id == '52652': # soil sensor
        current_sensor = session.query(SoilSensor).filter(SoilSensor.mac==mac).first()
    elif type_id == '0': # weather sernsor
        raise ValueError("weather sensor is not defined yet")
        pass
    else:
        raise ValueError("unkonwon sensor port: {0}".format(type_id))

    return current_sensor


def get_all_tilt_sensors():
    return session.query(TiltSensor).all()


def get_soil_sensor():
    return session.query(SoilSensor).first()


def get_previous_event():
    return session.query(Event).order_by(Event.id.desc()).first()
