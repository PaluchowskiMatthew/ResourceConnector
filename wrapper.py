from flask import Flask, jsonify
from flasgger import Swagger

from optparse import OptionParser

import socket
import threading
import subprocess

script_thread = None

app = Flask(__name__)
Swagger(app)


@app.route("/")
def hello():
    return "Hello World!"


@app.route('/script_running')
def scripts():
    """Example endpoint checking if script is running.
       ---
       responses:
         200:
           description: True or false value indicating of script thread is running
           examples:
             script_running: True
    """
    return jsonify({'script_running': script_thread.is_alive()})


def vararg_callback(option, opt_str, value, parser):
    assert value is None
    value = []

    def floatable(str):
        try:
            float(str)
            return True
        except ValueError:
            return False

    for arg in parser.rargs:
        # stop on --foo like options
        if arg[:2] == "--" and len(arg) > 2:
            break
        # stop on -a, but not on -3 or -3.0
        if arg[:1] == "-" and len(arg) > 1 and not floatable(arg):
            break
        value.append(arg)

    del parser.rargs[:len(value)]
    setattr(parser.values, option.dest, value)


def parse_options():
    """Parser used for script command with all its necessary parameters needed to be run on the cluster

    :return:
    """
    parser = OptionParser()
    parser.add_option("-s", "--script-command", dest="script_multiarg",
                      help="Define which script and all its necessary parameters should be run on the cluster",
                      action="callback", callback=vararg_callback)

    return parser.parse_args()


def launch_script(command):
    app.logger.info('Full command:\n' + command)

    process = subprocess.Popen(
        [command],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    output = process.communicate()[0]
    app.logger.info('Full commmand output: ' + output)
    process.stdin.close()
    return


def run_flask(debug=False):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    sock.close()
    if debug:
        port = 8000
    app.run(port=port, debug=debug)


if __name__ == "__main__":
    options, args = parse_options()
    script_command = options.script_multiarg

    global script_thread
    script_thread = threading.Thread(target=launch_script, args=script_command)
    script_thread.start()

    run_flask(debug=True)


# @app.route('/colors/<palette>/')
# def colors(palette):
#     """Example endpoint returning a list of colors by palette
#     This is using docstrings for specifications.
#     ---
#     parameters:
#       - name: palette
#         in: path
#         type: string
#         enum: ['all', 'rgb', 'cmyk']
#         required: true
#         default: all
#     definitions:
#       Palette:
#         type: object
#         properties:
#           palette_name:
#             type: array
#             items:
#               $ref: '#/definitions/Color'
#       Color:
#         type: string
#     responses:
#       200:
#         description: A list of colors (may be filtered by palette)
#         schema:
#           $ref: '#/definitions/Palette'
#         examples:
#           rgb: ['red', 'green', 'blue']
#     """
#     all_colors = {
#         'cmyk': ['cian', 'magenta', 'yellow', 'black'],
#         'rgb': ['red', 'green', 'blue']
#     }
#     if palette == 'all':
#         result = all_colors
#     else:
#         result = {palette: all_colors.get(palette)}
#
#     return jsonify(result)