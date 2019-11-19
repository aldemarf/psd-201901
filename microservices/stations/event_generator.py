import json
import time
import glob
import csv
import logging
from threading import Thread
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable
from conf import KAFKA_HOST, KAFKA_PORT


logging.basicConfig(level=logging.INFO)

HOST = f'{KAFKA_HOST}:{KAFKA_PORT}'
ATTEMPTS = 3
ENCODING = 'utf-8'
PUBLISH_INTERVAL_1X = 3600
PUBLISH_INTERVAL_10X = 360
PUBLISH_INTERVAL_100X = 36

stop_gen = False
running = False
threads = set()

logging.warning(f'stop_gen set to FALSE')


def encode_utf8(v, encoding='utf-8'):
    return json.dumps(v).encode(encoding)


def decode_utf8(v, encoding='utf-8'):
    return json.loads(v.decode(encoding))


def kafka_producer(host=HOST, serializer=encode_utf8, attempts=ATTEMPTS):
    for count in range(attempts):
        try:
            return KafkaProducer(bootstrap_servers=host, value_serializer=serializer)

        except (BrokerNotAvailableError, NoBrokersAvailable) as kafka_error:
            logging.info('\n')
            logging.info(f'Trying again in 5 seconds... {count + 1}/{attempts}')
            time.sleep(5)


def read_stations_csv(path='./stations', pattern='A*.csv'):
    """  """
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
    global stop_gen, threads
    status = ''

    if not stop_gen:
        stop_gen = True
        logging.warning(f'stop_gen set to TRUE')
        time.sleep(1.5)

    if len(threads) > 1:
        for thread in threads:
            if thread.is_alive():
                status += f'<strong>{thread.name} still running.</strong></br>\n'
            else:
                status += f'{thread.name} terminated.</br>\n'
    else:
        status = 'Single threaded process terminated.</br>\n'

    return status


def process_all_stations(publish_interval=PUBLISH_INTERVAL_100X):
    global stop_gen, threads

    try:
        stations_data = read_stations_csv()
        stations_running = {threads.name for thread in threads}

        for station_code, data in stations_data.items():
            if stop_gen:
                logging.info('Stopping events generation')
                break
            if station_code in stations_running:
                continue

            thread = Thread(target=process_station,
                            args=(data, publish_interval),
                            name=f'Thread-STA_{station_code}')
            threads.add(thread)
            thread.daemon = False
            thread.start()
            logging.info(f'Started Thread-STA_{station_code}')

    except Exception as error:
        logging.error(error)
        return False

    return True


def process_single_station(station, publish_interval=PUBLISH_INTERVAL_100X):
    global stop_gen, threads

    stations_data = read_stations_csv()

    if station not in stations_data:
        return False

    try:
        station_data = stations_data[station]

    except KeyError as error:
        logging.error('Invalid station ID -- Try again.')
        return

    thread = Thread(target=process_station,
           args=(station_data, publish_interval),
           name=f'Thread-STA_{station}')
    threads.add(thread)

    return threads


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
            staName = staName.replace(' ', '')
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
