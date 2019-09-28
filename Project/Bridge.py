from kafka import KafkaConsumer
import paho.mqtt.client as mqtt
import json
import time
import random

host =  "localhost"
token = "plKrzZWlhdSRBuXVibwe"
port = 1883

def on_connect(client,data, flags, rc):
    print("connected")

def on_publish(client,data,mid):
    print("published")

consumer = KafkaConsumer(
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda v: v.decode('utf-8'))

consumer.subscribe(['A301','A307','A309','A322','A328',
                    'A329','A341','A349','A350','A351',
                    'A357','A366','A370'])

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect

mqtt_client.on_publish = on_publish
mqtt_client.username_pw_set(token)
mqtt_client.connect(host,port,60)
mqtt_client.loop_start()


while True:
    for message in consumer:
        data = str(message.value)
        retorno = mqtt_client.publish("v1/devices/me/telemetry",data)
        time.sleep(2)





#def getClient(token):
#    client = mqtt.Client()
#    client.username_pw_set(token)
#    return client 

#def on_connect(client,data, flags, rc):
#    print("connected")

#def on_publish(client,data,mid):
#    print("published")

#consumer = KafkaConsumer(
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id='my-group',
#     value_deserializer=lambda v: str(v).encode('utf-8'))

#consumer.subscribe(['A301','A307','A309','A322','A328',
#                    'A329','A341','A349','A350','A351',
#                    'A357','A366','A370'])

#host =  "localhost"
#token = "plKrzZWlhdSRBuXVibwe"
#port = 1883
#client = getClient(token)
#client.on_connect = on_connect
#client.on_publish = on_publish
#client.connect(host,port,60)
#client.loop_forever()

#while True:
#    client.publish("v1/devices/me/telemetry","2",1) 
#    for message in consumer:
#        print(message.value)
#        client.publish("v1/devices/me/telemetry",message.value,1)
#    time.sleep(5)