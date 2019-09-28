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

csv_files = glob.glob('A*.csv')
regex = ''

for path in csv_files:
    with open(path) as data:
        station_csv = csv.DictReader(data, delimiter=',')
        reading = next(station_csv)

    string = reading['stationName'].strip()
    regex += '^(estacoes\.{})\.+|'.format(string)

consumer.subscribe(pattern=regex[:-1])

# for message in consumer:
#     print('{}'.format(message.topic))
#     print(message.value)


########################################################


def get_tenant_token(host='localhost', port='9090', user='tenant@thingsboard.org', pwd='tenant'):

    user_id = {"username": user, "password": pwd}
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    url = 'http://{}:{}/api/auth/login'.format(host, port)

    result = requests.post(url, json=user_id, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        token = data['token']
        return token
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None

# print(get_user_token())

def get_tenant_devices(device_name, host='localhost', port='9090', token='', **kwargs):

    token = 'Bearer {}'.format(token)
    headers = {'Accept': 'application/json', 'X-Authorization': token}
    url = 'http://{}:{}/api/tenant/devices?'.format(host, port)

    deviceType = kwargs.get('deviceType')
    textSearch = kwargs.get('textSearch')
    idOffset = kwargs.get('idOffset')
    textOffset = kwargs.get('textOffset')
    limit = kwargs.get('limit')

    if deviceType:
        url += 'type={}&'.format(deviceType)

    if textSearch:
        url += 'textSearch={}&'.format(textSearch)

    if idOffset:
        url += 'idOffset={}&'.format(idOffset)

    if textOffset:
        url += 'textOffset={}&'.format(textOffset)

    if limit:
        url += 'limit={}&'.format(limit)

    url = url[:-1]

    result = requests.get(url, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        devices = data['data']
        return devices
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None


def get_device_id(device):

    if isinstance(device, list):
        ids = [item['id']['id'] for item in device]
        return ids
    else:
        return device['id']['id']


def get_device_credential(device_id='', host='localhost', port='9090', token=''):

    token = 'Bearer {}'.format(token)
    headers = {'Accept': 'application/json', 'X-Authorization': token}

    url = 'http://{}:{}/api/device/{}/credentials'.format(host, port, device_id)

    result = requests.get(url, headers=headers)
    data = json.loads(result.content)

    if result.status_code == 200:
        credential = data['credentialsId']
        return credential
    else:
        logging.error('status response: {} -- {}'.format(data['status'], data['message']))
        return None


def get_devices_credentials(devices_list=[], host='localhost', port='9090', token=''):
    if len(devices_list) == 0:
        return None
    else:
        get = get_device_credential
        credentials = [get(device, host, port, token) for device in devices_list]
        return credentials



token = get_tenant_token()

result = get_tenant_devices('', token=get_tenant_token(), limit=1000)
id_list = get_device_id(result)
credentials_list = get_devices_credentials(id_list, token=token)
print(credentials_list)

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
