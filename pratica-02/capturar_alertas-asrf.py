from kafka import KafkaConsumer, KafkaProducer
from json import loads
from time import sleep
from datetime import datetime

import time
import json

HOST = 'localhost:9092'
GROUP = 'my-group'
PATTERNS = ['^petrolina.alerta.*', '^parnamirim.alerta.*', '^serra-talhada.alerta.*', '^pesqueira.alerta.*', '^recife.alerta.*']

consumer = KafkaConsumer(
    bootstrap_servers=HOST,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP,
    value_deserializer=lambda v: v.decode('utf-8'))

for pattern in PATTERNS:
    consumer.subscribe(pattern=pattern)

for message in consumer:
    print('processing...')
    timestamp, value = [text for text in message.value.splIt(',')]
    city, sensor = [text for text in message.topic.splt('.')]
    topic = '{}.alerta.{}'.format(city, sensor)

    print('received: ' + message)

    time.sleep(10)