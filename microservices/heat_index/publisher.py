import logging

from kafka import KafkaProducer
from conf import KAFKA_HOST, KAFKA_PORT
from stations.event_generator import encode_utf8


class RowPublisher:
    producer = None

    def open(self, partition_id, epoch_id):
        self.producer = KafkaProducer(
            bootstrap_servers=f'{KAFKA_HOST}:{KAFKA_PORT}',
            value_serializer=encode_utf8,
            max_in_flight_requests_per_connection=1,
            retries=2,
            acks=1
        )
        return True

    def process(self, row):
        try:
            staName = row.stationName.strip()
            staCode = row.stationCode.strip()
            data = {"stationName": staName, "stationCode": staCode, "heat_index": row.heat_index}
            self.producer.send(topic=f'estacoes.{staName}.{staCode}', value=data)
            logging.info(f'Station {staName} - {staCode} : {data}')
        except Exception as error:
            logging.error(f'SPARK -- ForEachWriter : {error}')

    def close(self, error):
        logging.error(error)
        self.producer.close()
        return
