import logging

from kafka import KafkaProducer
from conf import KAFKA_HOST, KAFKA_PORT
from stations.event_generator import encode_utf8


class RowPublisher:
    producer = KafkaProducer(
        bootstrap_servers=f'{KAFKA_HOST}:{KAFKA_PORT}',
        value_serializer=encode_utf8,
        acks='all')

    def open(self, partition_id, epoch_id):
        return self.producer.bootstrap_connected()

    def process(self, row):
        data = encode_utf8({"heatIndex": row.heat_index})
        logging.info(data)
        self.producer.send(topic=f'estacoes.{row.staName}.{row.staCode}', value=data)

    def close(self, error):
        logging.error(error)
        self.producer.close()
