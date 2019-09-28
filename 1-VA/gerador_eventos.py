#!/usr/bin/env python

import json
import time
import glob
import csv
import os
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaConnectionError, BrokerNotAvailableError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO)

HOST = 'localhost:9092'
TOPIC_PATTERN = 'estacoes.{}.{}'
ATTEMPTS = 3
ENCODING = 'utf-8'
PUBLISH_INTERVAL = 5

encode_utf8 = lambda v: json.dumps(v).encode(ENCODING)

stations_data = {}
csv_files = glob.glob('A*.csv')


for path in csv_files:
    with open(path) as data:
        station_csv = csv.DictReader(data, delimiter=',')
        station_data = [dict(reading) for reading in station_csv]

    station_code = station_data[0]['stationCode']
    stations_data[station_code] = station_data


for count in range(ATTEMPTS):
    try:
        producer = KafkaProducer(bootstrap_servers=HOST, value_serializer=encode_utf8)

        index = 0
        for station, data in stations_data.items():
            for reading in data:
                producer.send(TOPIC_PATTERN.format(reading['stationName'].strip(), reading['stationCode'].strip()), reading)
                logging.info('Sent message #{}'.format(index))
                index += 1
                time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        logging.info('')
        logging.warning('Shutdown event generator...')
        break

    except (BrokerNotAvailableError, NoBrokersAvailable, KafkaConnectionError) as kafka_err:
        logging.info('\n')
        logging.info('Trying again in 5 seconds... {}/{}'.format(count + 1, 3))
        time.sleep(5)

    else:
        break