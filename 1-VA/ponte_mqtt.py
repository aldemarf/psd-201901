#!/usr/bin/env python

import json
import csv
import glob
import logging
import time
import requests
import paho.mqtt.client as pahoMqtt
import tb_api

from paho.mqtt import publish
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)

def create_regex_pattern(path='./'):
    """ create a regex pattern that match all stations with .csv at given path"""
    csv_files = glob.glob('{}A*.csv'.format(path))
    regex = ''

    for path in csv_files:
        with open(path) as data:
            station_csv = csv.DictReader(data, delimiter=',')
            reading = next(station_csv)

        string = reading['stationName'].strip().replace(' ', '')
        regex += '^(estacoes\.{})\.+|'.format(string)

    return regex[:-1]


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
##################   KAFKA CONSUMER   ##################
########################################################


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

regex = create_regex_pattern()
consumer.subscribe(pattern=regex)


########################################################
####################   MQTT FEEDER   ###################
########################################################


MQTT_HOST='localhost'
MQTT_PORT=1883
TB_TOPIC='v1/devices/me/telemetry'

tenant_token = tb_api.get_tenant_token()
tenant_devices = tb_api.get_tenant_devices(token=tenant_token, limit='10000')
devName = tb_api.get_device_name
devices_dict = {devName(device): device for device in tenant_devices}

try:
    msg_index = 0
    while True:
        data = next(consumer).value
        device_code = data['stationCode'].strip()
        device_name = data['stationName'].strip()

        if device_code not in devices_dict:
            device = tb_api.create_device(device_code, device_type='ESTACAO METEOROLOGICA', device_label=device_name, token=tenant_token)
            devices_dict[device_code] = device

        device_id = tb_api.get_device_id(devices_dict[device_code])
        device_token = tb_api.get_device_credential(device_id, token=tenant_token)
        publish.single(TB_TOPIC, payload=encode_utf8(data), qos=0, hostname=MQTT_HOST, port=MQTT_PORT, auth={'username': device_token})

        msg_index += 1
        logging.info('Message #{} published. Device: {}'.format(msg_index, device_code))

        # time.sleep(1.5)

except Exception as error:
    logging.error(error)
    logging.warning('End of transmissions...')
    logging.warning('Shutting down MQTT feed...')

else:
    logging.warning('End of transmissions...')
    logging.warning('Shutting down MQTT feed...')


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