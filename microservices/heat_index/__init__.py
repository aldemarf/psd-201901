from conf import KAFKA_HOST, KAFKA_PORT
from heat_index.publisher import RowPublisher

from thingsboard.api import *

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, FloatType
from pyspark.sql.functions import udf, from_json, col

from math import sqrt

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-10s %(levelname)-6s %(message)s')

HOST = f'{KAFKA_HOST}:{KAFKA_PORT}'


def ctf(temp):  # Celsius to Fahrenheit
    return (temp * 9 / 5) + 32


def ftc(temp):  # Fahrenheit to Celsius
    return (temp - 32) * 5 / 9


@udf(returnType=FloatType())  # User Defined Function (Spark annotation)
def heat_index(t, rh):

    try:
        temp = ctf(float(t.strip()))
        hum = float(rh.strip())

    except Exception:
        return None

    hi = (1.1 * temp) + (0.047 * hum) - 10.3

    if hi < 80:
        return ftc(hi)
    else:
        hi = -42.379 + (2.04901523 * temp + 10.14333127 * hum) \
             - (0.22475541 * temp * hum) - (0.00683783 * temp ** 2) \
             - (0.05481717 * hum**2) + (0.00122874 * temp ** 2 * hum) \
             + (0.00085282 * temp * hum ** 2) - (0.00000199 * temp ** 2 * hum ** 2)

        # 80 <= T <= 112 && RH <= 13%
        if (hum < 13) and (80 <= temp <= 112):
            adjustment_subtraction = ((13 - hum) / 4) * sqrt((17 - abs(temp - 95) / 17))
            return ftc(hi - adjustment_subtraction)

        # 80 <= T <= 87 && RH > 85%
        elif (hum > 85) and (80 <= temp <= 87):
            adjustment_addition = ((hum - 85) / 10) * ((87 - temp) / 5)
            return ftc(hi + adjustment_addition)

        else:
            return ftc(hi)


def get_stations_hi(station_type='ESTAÇÃO METEOROLÓGICA'):
    tenant_token = get_tenant_token()
    tenant_devices = get_tenant_devices(token=tenant_token, limit='10000')

    devices = {device['name']: [device['id']['id'], device['id']['entityType']]
               for device in tenant_devices
               if device['type'] == station_type}

    get_hi = get_latest_telemetry_wo_timestamp
    stations_heat_index = {name: get_hi(id_, type_, token=tenant_token, keys='heat_index')
                           for name, (id_, type_) in devices.items()}
    return stations_heat_index


def create_spark_session():
    spark = SparkSession.builder \
        .appName('HeatIndex') \
        .getOrCreate()
    return spark


def start_hi_calc(sparkSession, host=KAFKA_HOST, port=KAFKA_PORT):
    try:
        subscribe_type = 'subscribePattern'
        topics_pattern = 'estacoes.*'
        startingOffset = 'latest'

        spark = sparkSession

        df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', f'{host}:{port}') \
            .option(subscribe_type, topics_pattern) \
            .option('startingOffsets', startingOffset) \
            .load() \
            .selectExpr('CAST(value AS STRING)')
        # .option('maxOffsetsPerTrigger', '1') \

        schema = StructType() \
            .add('timestamp', StringType()) \
            .add('stationCode', StringType()) \
            .add('stationName', StringType()) \
            .add('latitude', StringType()) \
            .add('longitude', StringType()) \
            .add('umid_max', StringType()) \
            .add('umid_min', StringType()) \
            .add('temp_max', StringType()) \
            .add('pressao', StringType()) \
            .add('pressao_min', StringType()) \
            .add('pto_orvalho_inst', StringType()) \
            .add('pto_orvalho_max', StringType()) \
            .add('radiacao', StringType()) \
            .add('temp_min', StringType()) \
            .add('pressao_max', StringType()) \
            .add('pto_orvalho_min', StringType()) \
            .add('temp_inst', StringType()) \
            .add('umid_inst', StringType()) \
            .add('precipitacao', StringType())

        sta_df = df.select(from_json(col('value').cast('string'), schema=schema).alias('sta'))\
            .select('sta.stationName',
                    'sta.stationCode',
                    'sta.temp_inst',
                    'sta.umid_inst')

        heat_index_df = sta_df.withColumn('heat_index', heat_index('temp_inst', 'umid_inst'))

        # consoleOutput = heat_index_df.writeStream\
        #     .outputMode("append")\
        #     .format("console")\
        #     .start()
        # consoleOutput.awaitTermination(timeout=1)

        kafkaOutput = heat_index_df.writeStream\
            .foreach(RowPublisher()) \
            .start()

    except Exception as error:
        logging.error(error)
        return False


def stop_heat_index(sparkSession, threads):
    status = ''

    sparkSession.stop()

    if len(threads) > 1:
        for thread in threads:
            name = thread.name
            if name == 'Thread-Spark_HeatIndex' and thread.is_alive():
                status += f'<strong>{name} still running.</strong></br>\n'
            else:
                status += f'{name} terminated.</br>\n'
    else:
        status = 'Heat Index calculation terminated.</br>\n'

    return status
