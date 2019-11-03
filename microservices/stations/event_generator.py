import json
import time
import glob
import csv
import logging
from threading import Thread
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable


logging.basicConfig(level=logging.INFO)

HOST = 'localhost:9092'
ATTEMPTS = 3
ENCODING = 'utf-8'
PUBLISH_INTERVAL_1X = 3600
PUBLISH_INTERVAL_10X = 360
PUBLISH_INTERVAL_100X = 36

stop_gen = False
running = False
threads = set()

logging.warning(f'stop_gen set to FALSE')


def encode_utf8(v):
    return json.dumps(v).encode(ENCODING)


def kafka_producer(host=HOST, serializer=encode_utf8, attempts=ATTEMPTS):
    for count in range(attempts):
        try:
            return KafkaProducer(bootstrap_servers=host, value_serializer=serializer)

        except (BrokerNotAvailableError, NoBrokersAvailable) as kafka_error:
            logging.info('\n')
            logging.info(f'Trying again in 5 seconds... {count + 1}/{attempts}')
            time.sleep(5)


def read_stations_csv(path='./stations', pattern='A*.csv'):
    stations_data = {}
    csv_files = glob.glob(pathname=f'{path}/{pattern}')

    for path in csv_files:
        with open(path) as data:
            station_csv = csv.DictReader(data, delimiter=',')
            station_data = [dict(reading) for reading in station_csv]

        station_code = station_data[0]['stationCode']
        stations_data[station_code] = station_data

    return stations_data


def stop_generator():
    global stop_gen
    stop_gen = True
    logging.warning(f'stop_gen set to TRUE')
    return False


def event_generator(processing='single', publish_interval=1):
    global threads

    if processing.lower() == 'single':
        stations_serial_process(publish_interval)
        return True
    elif processing.lower() == 'multi':
        stations_parallel_process(publish_interval)
        return True
    else:
        return False


def stations_parallel_process(publish_interval=PUBLISH_INTERVAL_100X):
    global stop_gen, threads

    stations_data = read_stations_csv()

    for station_code, station_data in stations_data.items():
        if stop_gen:
            logging.info('Stopping events generation')
            break
        thread = Thread(target=process_station,
                        args=(station_data, publish_interval),
                        name=f'Thread-STA_{station_code}')
        threads.add(thread)
        thread.daemon = False
        thread.start()
        logging.info(f'Started Thread-STA_{station_code}')


def stations_serial_process(publish_interval=PUBLISH_INTERVAL_100X):
    global stop_gen
    stations_data = read_stations_csv()

    for staCode, station_data in stations_data.items():
        if stop_gen:
            logging.info('Stopping events generation')
            break
        process_station(station_data, publish_interval)


def process_station(station_data, publish_interval=PUBLISH_INTERVAL_100X):
    global stop_gen
    try:
        index = 1
        producer = kafka_producer()
        for staName, staCode, reading in iter_readings(station_data):
            if stop_gen:
                logging.info('Stopping events generation')
                producer.close()
                break
            producer.send(f'estacoes.{staName}.{staCode}', reading)
            logging.info(f'Station {staCode} - {staName} : Message #{index} sent')
            index += 1
            time.sleep(publish_interval)

    except KeyboardInterrupt:
        logging.info('')
        logging.warning('Shutdown event generator...')

    except Exception as error:
        logging.error(error)


def iter_readings(station_data):
    for reading in station_data:
        yield (reading["stationName"].strip(),
               reading["stationCode"].strip(),
               reading)
