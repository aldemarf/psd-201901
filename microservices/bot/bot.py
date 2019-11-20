import telebot
from telebot.types import ForceReply
from telebot import types
from kafka import KafkaConsumer
from kafka import KafkaProducer
from thingsboard.api import *
from conf import KAFKA_HOST, KAFKA_PORT

TB_TOKEN = get_tenant_token()
BOT_TOKEN = "867830026:AAEIMa16WFWSDTqQdWeHAFmNGwJYhBr-gwg"
KAFKA_HOST = f"{KAFKA_HOST}:{KAFKA_PORT}"
cidades = {'Recife': {
                'code':'A301',
                'lat':-8.059280000000001,
                'long':-34.959239000000004
                },
            'Palmares': {
                'code':'A357',
                'lat':-8.666667,
                'long':-8.666667
                },
            'Caruaru': {
                'code':'A341',
                'lat':-8.236069,
                'long':-35.98555
                },
            'Garanhuns': {
                'code':'A322',
                'lat':-8.91095,
                'long':-36.493381
                },
            'Surubim': {
                'code':'A328',
                'lat':-7.839628,
                'long':-35.801056
                },
            'Arco-Verde': {
                'code':'A309',
                'lat':-8.433544,
                'long':-37.055477
                },
            'Serra Talhada': {
                'code':'A350',
                'lat':-7.954277,
                'long':-38.295082
                },
            'Salgueiro': {
                'code':'A370',
                'lat':-8.666667,
                'long':-39.096111
                },
            'Cabrobó': {
                'code':'A329',
                'lat':-8.504,
                'long':-39.31528
                },
            'Petrolina': {
                'code':'A307',
                'lat':-9.388323,
                'long':-40.523262
                },
            'Ouricuri': {
                'code':'A366',
                'lat':-7.885833,
                'long':-40.102683
                },
            'Floresta': {
                'code':'A351',
                'lat':-8.598785000000001,
                'long':-38.584062
                },
            'Ibimirim': {
                'code':'A349',
                'lat':-8.509552000000001,
                'long':-37.711591
                }
            }


producer = KafkaProducer(
    bootstrap_servers=KAFKA_HOST, 
    value_serializer=lambda v: str(v).encode('utf-8'))

def createConsumer(topic):
    consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_HOST],
    auto_offset_reset='earliest',
    enable_auto_commit=True,                                       
    group_id='my-group',
    value_deserializer=lambda v: v.decode('utf-8'))
    consumer.subscribe(topic)
    return consumer



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
    helpText = "Os seguintes comandos estão disponíveis: \n /cidades \n /localizacao"
    bot.send_message(chatID, helpText, parse_mode='MARKDOWN')

@bot.message_handler(commands=['localizacao'])
def command_location(message):
    chatID = message.chat.id
    tgUser = message.from_user
    loc = types.KeyboardButton("localizacao", request_location=True)
    keyboard = types.ReplyKeyboardMarkup(row_width=1, one_time_keyboard=True, resize_keyboard=True)
    keyboard.add(loc)
    helpText = "Compartilhe comigo sua localização atual"
    bot.send_message(chatID, helpText,reply_markup=keyboard)

@bot.message_handler(content_types=['location'])
def handle_location(call):
    userid = call.from_user.id
    data = call.location
    topic = "app.requests."+str(userid)
    producer.send(topic, data)
    print('#Sent '+topic+":"+str(data))
    #Mudar aqui o topico para que Aldemar ta mandando o HI response.[userid]
    consumer = createConsumer(topic)
    for message in consumer:
        #por aqui o resultado do IH
        bot.send_message(userid, "Indice de calor: " + str(message))
        break
    print("finish")
    consumer.close()
    

@bot.message_handler(commands=['cidades'])
def command_cidades(message):
    chatID = message.chat.id
    tgUser = message.from_user
    helpText = "Escolha sua cidade: "
    markup = types.InlineKeyboardMarkup(2)
    for key,value in cidades.items():
        itembtn = types.InlineKeyboardButton(key,callback_data=key)
        markup.add(itembtn)
    bot.send_message(chatID, helpText, reply_markup=markup)

@bot.callback_query_handler(lambda query: query.data in cidades.keys())
def callback(call):
    userid = call.from_user.id
    data = cidades.get(call.data)
    topic = "app.requests."+str(userid)
    producer.send(topic, data)
    print('#Sent '+topic+":"+str(data))
    #Mudar aqui o topico para que Aldemar ta mandando o HI response.[userid]
    consumer = createConsumer(topic)
    for message in consumer:
        #por aqui o resultado do IH
        bot.send_message(userid, "Indice de calor: " + str(message))
        break
    print("finish")
    consumer.close()


bot.polling()

