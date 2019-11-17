from flask import Flask, make_response, jsonify, request, abort, render_template
from stations.event_generator import process_all_stations, process_single_station, stop_generator
from stations import bridge_kafka_tb
from thingsboard.api import *
from distance.method import *

import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-10s %(levelname)-6s %(message)s')

app = Flask(__name__)


@app.route('/')
def api():
    return render_template('services.html')


#################################################
################## 5-NEAREST ####################
#################################################


help_5_nearest = """
        <strong>arg 1</strong>: t;</br>
        <strong>description</strong>: The instant temperature measured at the station;</br>
        </br>
        <strong>arg 2</strong>: rh;</br>
        <strong>description</strong>: The instant relative humidity measured at the station;</br>
        </br>
        <strong>return</strong>: Return the Heat Index as a float number;</br>
        </br>
        </br>
        <strong>Examples:</strong></br>
        http://host/api/5near?lat=-34.06218487&lon=-8.41657131
        </br> """
# ()-> JSON{distance: 16.345000000000002} (km)


@app.route('/api/5near/status')
def status_5nearest():
    return 'Server : Running'


@app.route('/api/5near/help')
def show_help_5_nearest():
    return help_5_nearest


@app.route('/api/5near/', methods=['GET'])
def get_5_nearest():
    try:
        data = request.args
        lat = float(data['lat'])
        lon = float(data['lon'])

        distance = haversine(lat, lon, lat2=-8.0453602, lon2=-34.941812)

        # TODO: CONTINUE IMPLEMENTATION
        # stations = []
        return jsonify({'distance': distance})

    except Exception as e:
        logging.error(f'INVALID ENTRY: {e}')
    #     return jsonify({'stations': None})


#################################################
################# HEAT INDEX ####################
#################################################


help_heat_index = """
        <strong>arg 1</strong>: t;</br>
        <strong>description</strong>: The instant temperature measured at the station;</br>
        </br>
        <strong>arg 2</strong>: rh;</br>
        <strong>description</strong>: The instant relative humidity measured at the station;</br>
        </br>
        <strong>return</strong>: Return the Heat Index as a float number;</br>
        </br>
        </br>
        <strong>Examples:</strong></br>
        http://host/api/hi?temp=33.2&rh=55</br>
        -> JSON{heat_index: 25.335000000000004}</br>
        </br>
        http://host/api/hi?temp=30&rh=60.7
        </br>
        -> JSON{heat_index: 24.345000000000002}
"""


@app.route('/api/heat_index/status')
def status_hi():
    return 'Server : Running'


@app.route('/api/heat_index/help')
def show_help_hi():
    return help_heat_index


@app.route('/api/heat_index/', methods=['GET'])
def calc_hi():
    try:
        data = request.args
        temp = float(data['temp'])

    except Exception as e:
        logging.error(f'INVALID ENTRY: {e}')
        return jsonify({'heat_index': None})


#################################################
############### EVENT GENERATOR #################
#################################################

threads = set()
running = False
help_event_generator = """
        <strong>arg 1</strong>: proc;</br>
        <strong>description</strong>: Type of processing SINGLE or MULTI;</br>
        </br>
        <strong>arg 2</strong>: inter;</br>
        <strong>description</strong>: The interval between stations readings;</br>
        </br>
        <strong>return</strong>: Nothing;</br>
        </br>
        </br>
        <strong>Example:</strong></br>
        http://host/api/event_generator/start?proc=multi&inter=10</br>
        </br>
        http://host/api/event_generator/start?proc=single&inter=1.5</br>
"""


@app.route('/api/event_generator')
def event_gen_help():
    return help_event_generator


@app.route('/api/event_generator/start', methods=['GET'])
def start_generate_events():
    global running, threads

    if running:
        return abort(405)

    try:
        interval = float(request.args['inter'])
    except KeyError as error:
        interval = 2
        logging.warning(error)

    logging.info('Starting events generation')
    running = process_all_stations(publish_interval=interval)

    if running:
        return f'<h2>Events generation : OK!<h2>' \
               f'Processing all stations multithreaded</br>' \
               f'Interval: {interval} seconds'
    else:
        return f'<h2>Events generation : Failed!<h2>'


@app.route('/api/event_generator/start/<string:station>', methods=['GET'])
def start_generate_events_single_station(station):
    global threads

    stations = {thread.name for thread in threads}

    if running and station in stations:
        return abort(405)

    try:
        interval = float(request.args['interval'])
    except KeyError as error:
        interval = 2
        logging.warning(error)

    logging.info(f'Starting events generation from station {station}')
    response = process_single_station(station=station, publish_interval=interval)

    if response:
        return f'<h2>Events generation : OK!<h2>' \
               f'Processing station {station} single-threaded</br>' \
               f'Interval: {interval} seconds'
    else:
        return f'<h2>Events generation : Failed!<h2>'


@app.route('/api/event_generator/stop', methods=['GET'])
def stop_generate_events():
    global running

    logging.info('Stopping events generation')
    status = stop_generator()
    running = False
    return f'<h3>Stopped events generation</h3>' \
           f'{status}'


#################################################
############### KAFKA-TB BRIDGE #################
#################################################


@app.route('/api/bridge/start', methods=['GET'])
def start_bridge():
    bridge_kafka_tb.start_bridge()


@app.route('/api/bridge/stop', methods=['GET'])
def stop_MQTT_bridge():
    logging.info('Stopping events generation')
    status = bridge_kafka_tb.stop_bridge()
    return f'<h3>Stopped events generation</h3>' \
           f'{status}'



#################################################
################ ERROR HANDLERS #################
#################################################


@app.errorhandler(400)
def bad_request(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Bad request'}), 400)


@app.errorhandler(403)
def not_found(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Forbidden'}), 403)


@app.errorhandler(404)
def not_found(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(port=5050, debug=True)
