from flask import Flask, jsonify
#from flasgger import Swagger

from optparse import OptionParser

import socket
import threading
import subprocess
import json


WRAPPER_NAME = 'resourceconnector'
SCHEMA_FILE = 'wrapper_schema.json'

SCRIPT1 = 'bbic_stack.py'
SCRIPT2 = 'brain_region_filtering.py'

script_thread = None
command = 'No command!'
output = 'No output!'
progress = 0

app = Flask(__name__)
#Swagger(app)


@app.route("/")
def hello():
    return "Hello World Wide Wrapper!"


@app.route('/'+WRAPPER_NAME+'/v1/registry')
def registry():
    """Endpoint providing a list of options to be called by the API.
       ---
       responses:
         200:
           description: Returns a list of options to be called on the API
           examples: "resourceconnector/v1/status": ["GET"]
    """
    return jsonify({"resourceconnector/v1/status": ["GET"]})


@app.route('/'+WRAPPER_NAME+'/v1/status')
def status():
    """Endpoint retrieving the status of the script launched via API.
       ---
       responses:
         200:
           description: Returns a list of options to be called on the API
           examples: "resourceconnector/v1/status": ["GET"]
    """

    message = 'Task starting...'

    if SCRIPT1 in command:
        # Get status for bbic_stack.py
        # Script specific variables
        SUBTASKS = 4

        out = output.split('\n')
        quarters = out.count('Done.')

        if '--allstack' in command:
            # Four stacks are run
            if quarters == SUBTASKS:
                # Task is done
                message = 'Task is done.'
                progress = 100
            elif out[-1][0:10] == 'Progress: ':
                # Task is partially done
                last_line = out[-1]
                fraction = last_line[10:last_line.find('/')] / last_line[last_line.find('/')+1:]
                progress = quarters*100/SUBTASKS+fraction*100/SUBTASKS
                message = 'Task is in progress...'

            else:
                # Intermediary progress state. Return last known progress
                progress = progress
                message = 'Task status unconfirmed'

        else:
            # Single stack is run
            if quarters == 1:
                # Task is done
                progress = 100
                message = 'Task is done.'
            if out[-1][0:10] == 'Progress: ':
                # Task is partially done
                last_line = out[-1]
                fraction = last_line[10:last_line.find('/')] / last_line[last_line.find('/')+1:]
                progress = fraction*100
                message = 'Task is in progress...'
            else:
                # Intermediary progress state. Return last known progress
                progress = progress
                message = 'Task status unconfirmed'

    elif SCRIPT2 in command:
        # Get status for brain_region_filtering.py
        #TODO: implement status for brain_region_filtering
        pass

    else:
        # Script has no status implemented
        app.logger.info('No status for this script available.')


    return jsonify({ "message" : message, "progress" : progress})


@app.route('/'+WRAPPER_NAME+'/v1/status/schema')
def schema():
    """Endpoint explaining the schema for further call automation via API.
       ---
       responses:
         200:
           description: Returns a schema describing the call structure for the wrapper API.
           examples: see wrapper_schema.json file
    """

    with open(SCHEMA_FILE) as json_data:
        schema_json = json.load(json_data)

    return jsonify(schema_json)





@app.route('/'+WRAPPER_NAME+'/v1/script_running')
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
    """ Option taking a variable number of arguments.
    Taken from here: https://docs.python.org/2/library/optparse.html#optparse-option-callbacks
    :param option:
    :param opt_str:
    :param value:
    :param parser:
    :return:
    """
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

    :return: parsed options and arguments
    """
    parser = OptionParser()
    parser.add_option("-s", "--script-command", dest="script_multiarg",
                      help="Define which script and all its necessary parameters should be run on the cluster",
                      action="callback", callback=vararg_callback)

    return parser.parse_args()


def launch_script(command):
    """
    Method for script/command launching inside the shell
    :param command: script which should be run inside the shell
    """
    app.logger.info('Full command:\n' + command)

    process = subprocess.Popen(
        [command],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    output = process.communicate()[0].decode('utf-8')
    app.logger.info('Full command output: ' + output)
    process.stdin.close()


def run_flask(debug=False):
    """
    Method for launching flask server on available port.
    :param debug: optional
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    sock.close()
    if debug:
        port = 3333
    app.run(port=port, debug=debug)


if __name__ == "__main__":
    options, args = parse_options()
    script_command = options.script_multiarg

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