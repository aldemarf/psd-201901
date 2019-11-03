from flask import Flask, make_response, jsonify, request, abort
from stations.event_generator import event_generator, stop_generator
from thingsboard.api import *
from distance.haversine import *
from hi.heat_index import *
import logging
import stations

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-10s %(levelname)-6s %(message)s')

app = Flask(__name__)

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

        distance = dist_haversine(lat, lon, lat2=-8.0453602, lon2=-34.941812)

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


@app.route('/api/hi/status')
def status_hi():
    return 'Server : Running'


@app.route('/api/hi/help')
def show_help_hi():
    return help_heat_index


@app.route('/api/hi/', methods=['GET'])
def calc_hi():
    try:
        data = request.args
        temp = float(data['temp'])
        rh = float(data['rh'])
        hi = heat_index(temp, rh)
        return jsonify({'heat_index': hi})

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
        processing = request.args['proc']
    except KeyError as error:
        processing = 'multi'
        logging.warning(error)

    try:
        interval = float(request.args['inter'])
    except KeyError as error:
        interval = 2
        logging.warning(error)

    logging.info('Starting events generation')
    if processing == 'multi' or processing == 'single':
        running = event_generator(processing=processing, publish_interval=interval)
    else:
        return f'<h2>Events generation : Failed!<h2>' \
               f'<h3>Parameters:</h3>' \
               f'Method: {processing} threaded</br>' \
               f'Interval: {interval} seconds'

    return f'<h2>Events generation : OK!<h2>' \
           f'<h3>Parameters:</h3>' \
           f'Method: {processing} threaded</br>' \
           f'Interval: {interval} seconds'


@app.route('/api/event_generator/stop', methods=['GET'])
def stop_generate_events():
    global running
    logging.info('Stopping events generation')
    running = stop_generator()
    return '<h3>Stopped events generation</h3>'


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
    return make_response(jsonify({f'{error.code}': 'Not found'}), 403)


@app.errorhandler(404)
def not_found(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(debug=True)
