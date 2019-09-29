from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
import time
import pandas as pd
import threading
import glob
import random
import os

#Set a defaut path and get the csv's list 
path = os.getcwd()
files = glob.glob('A*.csv')

# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

def sendCSV(filename):
    try:
        CSV = pd.read_csv(path+"/"+str(filename))
        columns = CSV.head()
        rows = columns.iterrows()
        for row in rows:
            data = row[1]
            topico = data['stationCode']
            message  = {}
            for column in columns:
                message[column] = data[column]
            producer.send(topico,message)
            print('#Sent '+topico+":"+str(message))
            time.sleep(4)
    except:
        print("Error")

def createThread(csvName):
    t=threading.Thread(target=sendCSV,args=(csvName,))
    t.start()
    return 'process started'

while True:
    processList = list(map(createThread,files))
    time.sleep(30)


