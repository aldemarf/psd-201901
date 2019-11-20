import logging

from kafka import KafkaConsumer, KafkaProducer

from conf import KAFKA_HOST, KAFKA_PORT
from stations.event_generator import decode_utf8, encode_utf8
from thingsboard.api import get_tenant_devices, get_tenant_token, get_latest_telemetry_wo_timestamp

HOST = f'{KAFKA_HOST}:{KAFKA_PORT}'

# PRODUCER
NEAREST_REQUEST_TOPIC = 'nearest.request'
APP_RESPONSE_TOPIC = 'app.response'

# CONSUMER
APP_REQUEST_TOPIC = fr'^(app.request).+'  # REGEX
NEAREST_RESPONSE_TOPIC = fr'^(nearest.response).+'  #REGEX

stop_interp_consumer = False
stop_interp_producer = False
stop_5near_producer = False
stop_5near_consumer = False


def get_stations_heat_index(stations, station_type='ESTAÇÃO METEOROLÓGICA'):
    tenant_token = get_tenant_token()
    tenant_devices = get_tenant_devices(token=tenant_token, limit='10000')

    stations_code = set(stations.keys())

    devices = {device['name']: [device['id']['id'], device['id']['entityType']]
               for device in tenant_devices
               if device['type'] == station_type and device['name'] in stations_code}

    get_data = get_latest_telemetry_wo_timestamp
    stations_hi = {name: get_data(id_, type_, token=tenant_token, keys='heat_index')
                   for name, (id_, type_) in devices.items()}

    return stations_hi


def idw_weight(x, power=1):
    return 1 / (x ** power)


def idw(heat_indexes, distances, power=1):
    hi_dist = {staCode if distance > 0
               else 'hi_value':
               [distance, heat_indexes[staCode]] if distance > 0
               else heat_indexes[staCode]
               for staCode, distance in distances.items()}

    if 'hi_value' in hi_dist:
        return hi_dist['hi_value']

    weights = 0
    numerator = 0
    for staCode, (distance, hi) in hi_dist:
        weight = idw_weight(distance, power)
        weights += weight
        numerator += hi * weight

    return numerator / weights


def dw(heat_indexes, distances):
    hi_dist = {staCode if distance > 0
               else 'hi_value':
                   [distance, heat_indexes[staCode]] if distance > 0
                   else heat_indexes[staCode]
               for staCode, distance in distances.items()}

    if 'hi_value' in hi_dist:
        return hi_dist['hi_value']

    distance_sum = 0
    numerator = 0
    for staCode, (distance, hi) in hi_dist:
        distance_sum += distance
        numerator += hi * distance

    return numerator / distance_sum


def start_consumer(regex, host=HOST, deserializer=decode_utf8):
    consumer = KafkaConsumer(
        bootstrap_servers=host,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=deserializer)

    consumer.subscribe(pattern=regex)
    return consumer


def start_producer(host=HOST, serializer=encode_utf8):
    producer = KafkaProducer(
        bootstrap_servers=host,
        value_serializer=serializer,
        max_in_flight_requests_per_connection=1,
        retries=2,
        acks=1
    )
    return producer


def process_app_requests():
    app_consumer = start_consumer(regex=APP_REQUEST_TOPIC)

    while not stop_interp_consumer:
        message = next(app_consumer)
        data = message.value
        topic = message.topic
        user_id = topic[topic.find('t.') + 2:]
        logging.info(f'Received request from APP : Topic {topic} -- {data}')

        lat = data['lat']
        lon = data['long']

        send_5near_request((lat, lon), user_id)


def send_5near_request(payload, userid, topic=NEAREST_REQUEST_TOPIC):
    interp_producer = start_producer()
    interp_producer.send(topic=f'{topic}.{userid}', value=payload)
    interp_producer.close()
    return logging.info(f'Sending request to MS (5-nearest) : Topic {topic}.{userid} -- {payload}')


def process_5near_response():
    near_consumer = start_consumer(regex=NEAREST_RESPONSE_TOPIC)

    while not stop_5near_consumer:
        message = next(near_consumer)
        data = message.value
        topic = message.topic
        user_id = topic[topic.find('t.') + 2:]
        logging.info(f'Received request from MS (5-nearest) : Topic {topic} -- {data}')

        distances = dict(data)
        stations_hi = get_stations_heat_index(distances)

        heat_index = {'heat_index': idw(stations_hi, distances)}

        send_app_response(heat_index, user_id)


def send_app_response(payload, userid, topic=APP_RESPONSE_TOPIC):
    interp_producer = start_producer()
    interp_producer.send(topic=f'{topic}.{userid}', value=payload)
    interp_producer.close()
    return logging.info(f'Sending response to APP : Topic {topic}.{userid} -- {payload}')

# distances = distance.nearest(-8.063149, -34.871098, 8, 'haversine')
# hi = list()
# hi = idw(hi, distances, 1)
