from flask import Flask, make_response, jsonify, request
from tb_api import *
from math import acos, cos, sin, asin, sqrt, radians
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-10s %(levelname)-6s %(message)s')

help_ = """
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
        http://host/api/5near?temp=33.2&rh=55</br>
        -> JSON{heat_index: 25.335000000000004}</br>
        </br>
        http://host/api/5near?temp=30&rh=60.7
        </br>
        -> JSON{heat_index: 24.345000000000002}
"""

app = Flask(__name__)
EARTH_RADIUS = 6371


@app.route('/api/5near/status')
def status():
    return 'Server : Running'


@app.route('/api/5near/help')
def show_help():
    return help_


@app.route('/api/5near/', methods=['GET'])
def get_5_nearest():
    try:
        lat = request.args['lat']
        lon = request.args['lon']

        # TODO: CONTINUE IMPLEMENTATION
        # stations = []
        # return jsonify({'stations': stations})

    except Exception as e:
        logging.error(f'INVALID ENTRY: {e}')
    #     return jsonify({'stations': None})


def dist_haversine(lat1, lon1, lat2, lon2, radius=6371):
    """ Earth radius = 6.371km """

    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    distance = 2 * radius * asin(
        sqrt(
            sin(delta_lat / 2) ** 2
            + cos(lat1) * cos(lat2) * sin(delta_lon / 2) ** 2
        )
    )
    return distance


@app.errorhandler(400)
def bad_request(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Bad request'}), 400)


@app.errorhandler(404)
def not_found(error):
    logging.error(error)
    return make_response(jsonify({f'{error.code}': 'Not found'}), 404)


if __name__ == '__main__':
    app.run()

