#!/usr/bin/env python
from kafka import KafkaProducer, KafkaConsumer
import json
from time import sleep
from datetime import datetime
import time
import glob
import csv
from itertools import tee


HOST = 'localhost:9092'
TOPIC_PATTERN = 'estacoes.{}.{}'
ENCODING = 'utf-8'

encode_utf8 = lambda v: json.dumps(v).encode(ENCODING)
decode_utf8 = lambda v: json.loads(v.decode(ENCODING))

# Create an instance of the Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=HOST,
        value_serializer=encode_utf8)
except:
    print('Broker unavailable.')

PATTERNS = ['/s*']

consumer = KafkaConsumer(
    bootstrap_servers=HOST,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=decode_utf8)

for pattern in PATTERNS:
    consumer.subscribe(pattern=pattern)



stations_data = {}
csv_files = glob.glob('./A*.csv')

for path in csv_files:
    with open(path) as data:
        station_csv = csv.DictReader(data, delimiter=',')
        station_data = [reading for reading in station_csv]
        station_code = station_data[0]['stationCode']

    stations_data[station_code] = station_data


for station, data in stations_data.items():
    for reading in data:
        print(TOPIC_PATTERN.format(reading['stationName'], reading['stationCode']))
        producer.send(TOPIC_PATTERN.format(reading['stationName'], reading['stationCode']), reading)



############################################

for message in consumer:
    print('processing...')
    print(message.topic)
    print(message.message)