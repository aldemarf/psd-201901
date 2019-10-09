from kafka import KafkaConsumer
import paho.mqtt.client as mqtt
import json
import time
import random
import glob
import tb_api as tb

TB_TOKEN = tb.get_tenant_token()
HOST =  "localhost"
token = "nx2JMXjH5CbC27eDHwGL"
PORT = 1883
#KAFKA_HOST = "172.16.206.12:9092"
KAFKA_HOST = "localhost:9092"


devices = tb.get_tenant_devices(token = TB_TOKEN,deviceType = 'PSD',limit=100)


def on_connect(client,data, flags, rc):
    print("connected")

def removeExtension(filename):
    filename = filename.replace('.csv','')
    return filename

def on_publish(client,data,mid):
    print("published")

def getMqttClient(token,on_connect, on_publish):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(HOST,PORT,60)
    return client 

def getDeviceToken(code):   
    #credential = tb.get_device_credential(device_id=code,token=TB_TOKEN)
    #if credential == None:
    #    new_device = tb.create_device(code, 'ESTAÇÃO METEREOLÓGICA','','localhost', '9090', TB_TOKEN)
    #    credential = tb.get_device_credential(device_id=code,token=TB_TOKEN)
    #    print("---------NEW DEVICE----------")
    #    print(new_device)
    devices = tb.get_tenant_devices(token = TB_TOKEN,deviceType = 'ESTAÇÃO METEREOLÓGICA',limit=100)
    for device in devices:
        if device['name'] == code:
            return tb.get_device_credential(device_id = device['id']['id'], token = TB_TOKEN)
    new_device = tb.create_device(code, 'ESTAÇÃO METEREOLÓGICA','','localhost', '9090', TB_TOKEN)
    return tb.get_device_credential(device_id = new_device['id']['id'], token = TB_TOKEN)
    


consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda v: v.decode('utf-8'))


stationList = list(map(removeExtension,glob.glob('A*')))
consumer.subscribe(stationList)
client = getMqttClient(token,on_connect,on_publish)
client.loop_start()


while True:
    for message in consumer:
        token = getDeviceToken(json.loads(message.value.replace("'",'"'))['stationCode'])
        client.username_pw_set(token)
        data = str(message.value)
        retorno = client.publish("v1/devices/me/telemetry",data)
        time.sleep(2)





