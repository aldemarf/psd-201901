from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
import time
import pandas as pd
import threading
import faust as ft
import random

path = "/home/rsi-psd-vm/Documents/rsi-psd-codes/psd/2019-2/pratica-02/Data/"
files = ['A301.csv','A307.csv','A309.csv','A322.csv','A328.csv',
        'A329.csv','A341.csv','A349.csv','A350.csv','A351.csv',
        'A357.csv','A366.csv','A370.csv']
# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))

def sendCSV(filename):
    CSV = pd.read_csv(path+str(filename))
    for row in CSV.head().iterrows():
        data = row[1]
        topico = data['stationCode']
        message  = {}
        for att in CSV.head():
            message[att] = data[att]
        producer.send(topico,message)
        print('#Sent '+topico+":"+str(message))
        pause = random.randint(5,12)
        time.sleep(pause)

def createThread(csvName):
    t=threading.Thread(target=sendCSV,args=(csvName,))
    t.start()
    return 'ok'

while True:
    print("-----")
    test = list(map(createThread,files))
    print(test)
    time.sleep(30)