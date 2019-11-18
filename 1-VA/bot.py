from collections import OrderedDict
import telebot
from telebot.types import ForceReply
from kafka import KafkaConsumer
import tb_api as tb

TB_TOKEN = tb.get_tenant_token()
BOT_TOKEN = "867830026:AAEIMa16WFWSDTqQdWeHAFmNGwJYhBr-gwg"
KAFKA_HOST = "localhost:9092"


def createConsumer(topic):
    consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,                                       
    group_id='my-group',
    value_deserializer=lambda v: v.decode('utf-8'))
    consumer.subscribe(topic)
    return consumer

def getDeviceNameByCity(city):
    devices = tb.get_tenant_devices(token = TB_TOKEN,deviceType = 'ESTAÇÃO METEREOLÓGICA',limit=100)
    for device in devices:
        print(device)

def listener(messages):
    for message in messages:
        if message.content_type == 'text':
            # print the sent message to the console
            print(message.chat.first_name,
                    message.chat.id,
                    message.text)
            

bot = telebot.TeleBot(BOT_TOKEN)
bot.set_update_listener(listener)






@bot.message_handler(commands=['start'])
def command_start(message):
    chatID = message.chat.id
    tgUser = message.from_user
    
    helpText = "Insira sua cidade"

    getDeviceByCity("a")

    bot.send_message(chatID, helpText, parse_mode='MARKDOWN')


bot.polling()