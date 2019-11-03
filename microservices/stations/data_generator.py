from kafka import KafkaProducer
import time
import pandas as pd
import threading
import glob
import os

#Set a defaut path and get the csv's list
intervalo1x = 3600
itervalo10x = 360
intervalo100x = 36
host = "localhost:9092"
path = os.getcwd()
files = glob.glob('A*.csv')

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers=host, value_serializer=lambda v: str(v).encode('utf-8'))


def sendCSV(filename):
    try:
        csv = pd.read_csv(f'{path}/{str(filename)}')
        columns = csv.head()
        columns = csv.columns
        rows = columns.iterrows()
        for row in rows:
            data = row[1]
            topic = data['stationCode']
            message = {}
            for column in columns:
                message[column] = data[column]
            producer.send(topic, message)
            print(f'#Sent {topic} : {str(message)}')
            time.sleep(intervalo100x)
    except:
        print("Error")


def createThread(csvName):
    t=threading.Thread(target=sendCSV, args=(csvName,))
    t.start()
    return 'process started'


while True:
    processList = list(map(createThread, files))
    time.sleep(30)


