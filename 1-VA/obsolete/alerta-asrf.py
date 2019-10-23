from kafka import KafkaConsumer, KafkaProducer
from json import loads
from time import sleep
from datetime import datetime

import time
import json

HOST = 'localhost:9092'
GROUP = 'my-group'
PATTERNS = ['^petrolina.[^alerta]*', '^parnamirim.[^alerta]*', '^serra-talhada.[^alerta]*', '^pesqueira.[^alerta]*', '^recife.[^alerta]*']

consumer = KafkaConsumer(
    bootstrap_servers=HOST,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP,
    value_deserializer=lambda v: v.decode('utf-8'))

for pattern in PATTERNS:
    consumer.subscribe(pattern=pattern)

producer = KafkaProducer(
    bootstrap_servers=HOST,
    value_serializer=lambda v: str(v).encode('utf-8'))

print("press ctrl+c to stop...")

for message in consumer:
    print(message.value)
    print(message.topic)
    alert = False
    timestamp, value = [text for text in message.value.split(',')]
    value = int(value)
    city, sensor = [text for text in message.topic.split('.')]
    topic = '{}.alerta.{}'.format(city, sensor)

    if sensor == 'precipitacao':
        if value > 100:
            alert = True
    elif sensor == 'velocidade-vento':
        if value > 100:
            alert = True
    elif sensor == 'radiacao-uv':
        if value > 5:
            alert = True

    if alert:
        producer.send(topic, value)
        print('sent: {}'.format(message))

    time.sleep(10)
