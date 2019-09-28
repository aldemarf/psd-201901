#!/usr/bin/env python

from kafka import KafkaConsumer

import json
import csv
import glob
import logging
import time
import requests
import paho.mqtt.client as mqtt

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


def create_stations(stations=[]):
    """" create devices for every station passed as parameter """
    if len(stations) == 0:
        return None
    else:
        pass


########################################################
################       TEST FIELD       ################
########################################################


# for message in consumer:
#     print('{}'.format(message.topic))
#     print(message.value)


########################################################
################       TEST FIELD       ################
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
