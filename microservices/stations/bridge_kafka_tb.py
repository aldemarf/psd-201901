import csv
import glob

from thingsboard.api import *
from paho.mqtt import publish
from kafka import KafkaConsumer


stop_feed = False


def encode_utf8(v, encoding='utf-8'):
    return json.dumps(v).encode(encoding)


def decode_utf8(v, encoding='utf-8'):
    return json.loads(v.decode(encoding))


def create_regex_pattern(path='./stations', pattern='A*.csv'):
    """ create a regex pattern that match all stations with .csv at given path"""
    csv_files = glob.glob(f'{path}/{pattern}')
    regex_ = ''

    for path in csv_files:
        with open(path) as data:
            station_csv = csv.DictReader(data, delimiter=',')
            reading = next(station_csv)

        string_ = reading['stationName'].strip().replace(' ', '')
        regex_ += fr'^(estacoes.{string_}).+|'

    return regex_[:-1]


def get_stations_info(path='./stations', pattern='A*.csv'):
    """ get information from all stations with .csv at given path"""
    stations = {}
    csv_files = glob.glob(pathname=f'{path}/{pattern}')

    for path in csv_files:
        with open(path) as data:
            station_csv = csv.DictReader(data, delimiter=',')
            reading = next(station_csv)

        station_code = reading['stationCode'].strip()
        if station_code not in stations:
            stations[station_code] = reading['stationName'].strip()

    return stations


def create_met_stations(stations):
    """" creates devices for every weather station passed as parameter and return an array with created devices"""
    if not isinstance(stations, dict):
        logging.error(r'Wrong type. Pass a dict {stationCode : stationName}')
        return None

    if len(stations) == 0:
        return None
    else:
        token = get_tenant_token()
        create = create_device
        devices = [create(code, 'ESTAÇÃO METEREOLÓGICA', device_label=name, token=token)
                   for code, name in stations.items()]
    return devices


########################################################
##################   KAFKA CONSUMER   ##################
########################################################


def start_consumer(host='localhost:9092', deserializer=decode_utf8):

    consumer = KafkaConsumer(
        bootstrap_servers=host,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=deserializer)

    regex = create_regex_pattern()
    consumer.subscribe(pattern=regex)
    return consumer


########################################################
####################   MQTT FEEDER   ###################
########################################################

def start_bridge(host='localhost', port=1883, topic='v1/devices/me/telemetry'):
    global stop_feed

    tenant_token = get_tenant_token()
    tenant_devices = get_tenant_devices(token=tenant_token, limit='10000')
    devName = get_device_name
    devices_dict = {devName(device): device for device in tenant_devices}

    consumer = start_consumer()

    try:
        msg_index = 0
        while not stop_feed:
            data = next(consumer).value
            device_code = data['stationCode'].strip()
            device_name = data['stationName'].strip()

            if device_code not in devices_dict:
                device = create_device(device_code,
                                       device_type='ESTAÇÃO METEOROLÓGICA',
                                       device_label=device_name,
                                       token=tenant_token)
                devices_dict[device_code] = device

            device_id = get_device_id(devices_dict[device_code])
            device_token = get_device_credential(device_id, token=tenant_token)

            publish.single(topic,
                           payload=encode_utf8(data),
                           qos=0, hostname=host,
                           port=port,
                           auth={'username': device_token})

            msg_index += 1
            logging.info(f'Message #{msg_index} published. Device: {device_code}')
            # time.sleep(1.5)

    except Exception as error:
        logging.error(error)
        logging.warning('End of transmissions...')
        logging.warning('Shutting down MQTT feed...')

    finally:
        logging.warning('End of transmissions...')
        logging.warning('Shutting down MQTT feed...')


def stop_bridge():
    global stop_feed
    stop_feed = True

########################################################
################       TEST FIELD       ################
########################################################

# stations = get_stations_info()
# devices = create_met_stations(stations)
# print(devices)

# for message in consumer:
#     print(f'{message.topic}')
#     print(message.value)


########################################################
################       TEST FIELD       ################
########################################################