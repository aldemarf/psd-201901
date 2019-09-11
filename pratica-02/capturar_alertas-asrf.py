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

for message in consumer:
    timestamp, value = [text for text in message.value.splt(',')]
    city, sensor = [text for text in message.topic.splt('.')]
    topic = '{}.alerta.{}'.format(city, sensor)

    print('received: ' + message)
    
    time.sleep(10)