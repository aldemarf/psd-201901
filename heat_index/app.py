from flask import Flask, abort
from heat_index import heat_index

app = Flask(__name__)


help = """
        <strong>arg 1</strong>: t;</br>
        <strong>description</strong>: The instant temperature measured at the station;</br></br>
        <strong>arg 2</strong>: rh;</br>
        <strong>description</strong>: The instant relative humidity measured at the station;</br></br>
        <strong>return</strong>: Return the Heat Index as a float number;</br>
"""


@app.route('/hi/help')
def show_help():
    return help


@app.route('/api/hi/<float:temp>', methods=['GET'])
def calc_hi(temp):
    hi = heat_index(temp)
    return f'{hi}'


if __name__ == '__main__':
    app.run(debug=True)
