from flask import Flask, make_response, jsonify, request
from heat_index import heat_index
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
        http://host/api/hi?temp=33.2&rh=55</br>
        -> JSON{heat_index: 25.335000000000004}</br>
        </br>
        http://host/api/hi?temp=30&rh=60.7
        </br>
        -> JSON{heat_index: 24.345000000000002}
"""

app = Flask(__name__)


@app.route('/api/hi/status')
def status():
    return 'Server : Running'


@app.route('/api/hi/help')
def show_help():
    return help_


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

