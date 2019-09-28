#!/usr/bin/env python

from kafka import KafkaConsumer

import json
import csv
import glob
import logging
import time
import requests
import paho.mqtt.client as mqtt
import tb_api

logging.basicConfig(level=logging.INFO)

HOST = 'localhost:9092'
TOPIC_PATTERN = 'estacoes.{}.{}'
ENCODING = 'utf-8'

encode_utf8 = lambda v: json.dumps(v).encode(ENCODING)
decode_utf8 = lambda v: json.loads(v.decode(ENCODING))

consumer = KafkaConsumer(
    bootstrap_servers=HOST,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=decode_utf8)


def create_regex_pattern(path='./'):
    """ create a regex pattern that match all stations with .csv at given path"""
    csv_files = glob.glob('{}A*.csv'.format(path))
    regex = ''

    for path in csv_files:
        with open(path) as data:
            station_csv = csv.DictReader(data, delimiter=',')
            reading = next(station_csv)

        string = reading['stationName'].strip()
        regex += '^(estacoes\.{})\.+|'.format(string)

    return regex[:-1]

regex = create_regex_pattern()
consumer.subscribe(pattern=regex)


def get_stations_info(path='./'):
    """ get information from all stations with .csv at given path"""
    csv_files = glob.glob('{}A*.csv'.format(path))
    stations = {}

    for path in csv_files:
        with open(path) as data:
            station_csv = csv.DictReader(data, delimiter=',')
            reading = next(station_csv)

        station_code = reading['stationCode'].strip()
        if station_code in stations:
            continue
        else:
            stations[station_code] = reading['stationName'].strip()

    return stations


def create_met_stations(stations={}):
    """" creates devices for every weather station passed as parameter and return an array with created devices"""
    if not isinstance(stations, dict):
        logging.error('Wrong type. Pass a dict \{stationCode : stationName\}')
        return None

    if len(stations) == 0:
        return None
    else:
        token = tb_api.get_tenant_token()
        create = tb_api.create_device
        devices = [create(code, 'Estação meteorológica', device_label=name, token=token) \
            for code, name in stations.items()]
    return devices


########################################################
################       TEST FIELD       ################
########################################################

# stations = get_stations_info()
# devices = create_met_stations(stations)
# print(devices)

# for message in consumer:
#     print('{}'.format(message.topic))
#     print(message.value)


########################################################
################       TEST FIELD       ################
########################################################





########################################################
#######   TODO: Implement telemetry feed to TB   #######
########################################################


# def on_connect(client, userdata, flags, rc):
#     print("Connected to {} with result code {}".format(client._host, rc))


# def publish_data(client, topic, data, qos):
#     ret = client.publish(topic=topic, payload=data, qos=qos)

#     if ret.rc == 0:
#         return print('Message id:{} -- Published'.format(ret.mid))
#     else:
#         print('Message id:{} -- Error code {}'.format(ret.mid, ret.rc))


# def start_publish():

#     BROKER_PORT = 1883
#     BROKER_URL = 'jualabs.local'
#     CLIENT_ID = 'PSD-ASRF'
#     ACCESS_TOKEN = 'TESTPAHO'
#     TOPIC_TB = 'v1/devices/me/telemetry'
#     PUB_SLEEP_TIME = 1

#     TEMP_LOW = -50
#     TEMP_HIGH = 50

#     HUM_LOW = 0
#     HUM_HIGH = 100

#     mqtt_client = mqtt.Client(CLIENT_ID)
#     mqtt_client.username_pw_set(ACCESS_TOKEN)
#     mqtt_client.on_connect = on_connect

#     mqtt_client.connect(BROKER_URL, BROKER_PORT)

#     mqtt_client.loop_start()

#     try:
#         while True:
#             #### So funcionou com no formato JSON. Com string/int/float nao reconheceu o payload
#             temperature = json.dumps({'temperature': rnd.randint(TEMP_LOW, TEMP_HIGH)})
#             humidity = json.dumps({'humidity': rnd.randint(HUM_LOW, HUM_HIGH)})
#             mqtt_client.publish(topic=TOPIC_TB, payload=temperature, qos=0)
#             publish_data(mqtt_client, TOPIC_TB, humidity, qos=1)
#             time.sleep(PUB_SLEEP_TIME)

#     except KeyboardInterrupt:
#         mqtt_client.loop_stop()
#         mqtt_client.disconnect()


# start_publish()
