from flask import Flask, make_response, jsonify, request
from thingsboard.api import *
from distance.haversine import *
from hi.heat_index import *
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-10s %(levelname)-6s %(message)s')

app = Flask(__name__)

#################################################
################## 5_NEAREST ####################
#################################################


help_5near = """
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
def status_5nearnear():
    return 'Server : Running'


@app.route('/api/5near/help')
def show_help_5near():
    return help_5near


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
################## HAVERSINE ####################
#################################################


help_haversine = """
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
    return help_haversine


@app.route('/api/hi/', methods=['GET'])
def calc_hi_float():
    try:
        temp = float(request.args['temp'])
        rh = float(request.args['rh'])
        hi = heat_index(temp, rh)
        return jsonify({'heat_index': hi})

    except Exception as e:
        logging.error(f'INVALID ENTRY: {e}')
        return jsonify({'heat_index': None})


#################################################
################ ERROR HANDLERS #################
#################################################


@app.errorhandler(400)
def bad_request(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Bad request'}), 400)


@app.errorhandler(404)
def not_found(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(debug=True)
