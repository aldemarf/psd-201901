from thingsboard.api import *
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType, \
    DoubleType
from pyspark.sql.functions import udf, from_json, col, unix_timestamp
from math import sqrt

# spark.sparkContext.setLogLevel("ERROR")


def ctf(temp):
    return temp * 9 / 5 + 32


@udf(returnType=FloatType())
def heat_index(t=89.6, rh=35):

    if not isinstance(t, float):
        t = float(t)

    if not isinstance(rh, float):
        rh = float(rh)

    hi = (1.1 * t) + (0.047 * rh) - 10.3

    if hi < 80:
        return hi

    else:
        hi = -42.379 + (2.04901523 * t + 10.14333127 * rh) \
             - (0.22475541 * t * rh) - (0.00683783 * t**2) \
             - (0.05481717 * rh**2) + (0.00122874 * t**2 * rh) \
             + (0.00085282 * t * rh**2) - (0.00000199 * t**2 * rh**2)

        # 80 <= T <= 112 && RH <= 13%
        if (rh < 13) and (80 <= t <= 112):
            adjustment_subtraction = ((13 - rh) / 4) * sqrt((17 - abs(t - 95) / 17))
            return hi - adjustment_subtraction

        # 80 <= T <= 87 && RH > 85%
        elif (rh > 85) and (80 <= t <= 87):
            adjustment_addition = ((rh - 85) / 10) * ((87 - t) / 5)
            return hi + adjustment_addition

        else:
            return hi


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


def calc_stations_hi():

    return NotImplemented


host = 'localhost'
port = '9092'
subscribe_type = 'subscribePattern'
consumer = KafkaConsumer(group_id='topics', bootstrap_servers=[f'{host}:{port}'])
topics_list = [topic for topic in consumer.topics() if topic[:8] == 'estacoes']
topics = ','.join(topics_list)
consumer.close()
topics_pattern = 'estacoes.*'
startingOffset = 'latest'

spark = SparkSession.builder\
    .appName('HeatIndex')\
    .getOrCreate()

df = spark.readStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', f'{host}:{port}')\
    .option(subscribe_type, topics_pattern)\
    .option('startingOffsets', startingOffset)\
    .load()\
    .selectExpr('CAST(value as STRING)')

schema = StructType()\
    .add('timestamp', StringType())\
    .add('stationCode', StringType())\
    .add('stationName', StringType())\
    .add('latitude', StringType())\
    .add('longitude', StringType())\
    .add('umid_max', StringType())\
    .add('umid_min', StringType())\
    .add('temp_max', StringType())\
    .add('pressao', StringType())\
    .add('pressao_min', StringType())\
    .add('pto_orvalho_inst', StringType())\
    .add('pto_orvalho_max', StringType())\
    .add('radiacao', StringType())\
    .add('temp_min', StringType())\
    .add('pressao_max', StringType())\
    .add('pto_orvalho_min', StringType())\
    .add('temp_inst', StringType())\
    .add('umid_inst', StringType())\
    .add('precipitacao', StringType())

sta_df = df.select(from_json(col('value').cast('string'), schema=schema).alias('sta'))\
    .select('sta.*')\
    .select('stationName', 'stationCode', 'latitude', 'longitude', 'temp_inst', 'umid_inst')

sta_df.printSchema()

consoleOutput = sta_df.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()
consoleOutput.awaitTermination(timeout=10)

heat_index_df = sta_df.withColumn('Heat_Index', heat_index('temp_inst', 'umid_inst'))
#
consoleOutput = heat_index_df.writeStream\
    .outputMode("append")\
    .format("console")\
    .start()
consoleOutput.awaitTermination(timeout=10)

print('')
