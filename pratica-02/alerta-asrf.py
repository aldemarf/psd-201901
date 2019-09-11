from kafka import KafkaConsumer, KafkaProducer
from json import loads
from time import sleep
from datetime import datetime

import time
import json

HOST = ''
GROUP = 'my-group'

consumer = KafkaConsumer(
    topic,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id=GROUP,
     value_deserializer=lambda v: str(v).encode('utf-8'))

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: str(v).encode('utf-8'))

print("press ctrl+c to stop...")

for message in consumer:
    alert = False    
    timestamp, value = [text for text in message.value.splt(',')]
    city, sensor = [text for text in message.topic.splt('.')]
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
        print('sent: ' + message)
    
    time.sleep(10)
