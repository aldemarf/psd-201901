from kafka import KafkaConsumer, KafkaProducer

from distance.method import haversine, euclidian
from stations.event_generator import decode_utf8, encode_utf8
from thingsboard.api import *
from conf import KAFKA_HOST, KAFKA_PORT

HOST = f'{KAFKA_HOST}:{KAFKA_PORT}'

# PRODUCER
INTERP_RESPONSE_TOPIC = 'nearest.response'

# CONSUMER
INTERP_REQUEST_TOPIC = fr'^(nearest.request).+'  # REGEX


stop_interp_consumer = False
stop_interp_producer = False
stop_5near_producer = False
stop_5near_consumer = False


def get_stations_location(station_type='ESTAÇÃO METEOROLÓGICA'):
    tenant_token = get_tenant_token()
    tenant_devices = get_tenant_devices(token=tenant_token, limit='10000')

    devices = {device['name']: [device['id']['id'], device['id']['entityType']]
               for device in tenant_devices
               if device['type'] == station_type}

    get_lat_lon = get_latest_telemetry_wo_timestamp
    stations_coordinates = {name: get_lat_lon(id_, type_, token=tenant_token, keys='latitude,longitude')
                            for name, (id_, type_) in devices.items()}

    return stations_coordinates


def calc_distance(lat='', lon='', method='haversine'):
    stations_distances = get_stations_location()

    if method.lower() == 'haversine':
        for station, coordinates in stations_distances.items():
            dist = haversine(lat, lon, float(coordinates['latitude']), float(coordinates['longitude']))
            stations_distances[station]['distance'] = dist

    elif method.lower() == 'euclidian':
        for station, coordinates in stations_distances.items():
            dist = euclidian(lat, lon, float(coordinates['latitude']), float(coordinates['longitude']))
            stations_distances[station]['distance'] = dist

    return stations_distances


def nearest(lat, lon, n=1, method='haversine'):
    stations = calc_distance(lat, lon, method=method)
    stations = [(item[0], item[1]['distance']) for item in stations.items()]
    stations.sort(key=lambda item: item[1])
    return dict(stations[:n])


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


def process_interp_requests():
    app_consumer = start_consumer(regex=INTERP_REQUEST_TOPIC)

    while not stop_interp_consumer:
        message = next(app_consumer)
        data = message.value
        topic = message.topic
        user_id = topic[topic.find('t.') + 2:]
        logging.info(f'Received request from MS (Interpolation) : Topic {topic} -- {data}')

        lat = data['lat']
        lon = data['long']

        distances = nearest(lat, lon, n=5, method='haversine')

        send_interp_response(distances, user_id)


def send_interp_response(payload, userid, topic=INTERP_RESPONSE_TOPIC):
    interp_producer = start_producer()
    interp_producer.send(topic=f'{topic}.{userid}', value=payload)
    interp_producer.close()
    return logging.info(f'Sending response to MS (Interpolation) : Topic {topic}.{userid} -- {payload}')



# print(nearest(-8.063149, -34.871098, 8, 'haversine '))  # Marco-Zero coord.
# print(nearest(-8.063149, -34.871098, 8, 'euclidian'))  # Marco-Zero coord.

