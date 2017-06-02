# -*- coding: utf-8 -*-

###############################################################################
#
# Code related to EPFL Master Semester Project:
# "A job management web service for cluster-based processing in Brain Atlasing"
#
# Version 1.0, 02 June 2017
#
# Copyright (c) 2017, Blue Brain Project
#                     Mateusz Paluchowski <mateusz.paluchowski@epfl.ch>
#                     Christian Tresch <christian.tresch@epfl.ch>
#
###############################################################################

from flask import Flask, jsonify, request
from optparse import OptionParser

import threading
import subprocess
import json
import time

WRAPPER_NAME = 'resourceconnector'
SCHEMA_FILE = 'config/registry_schema.json'
app = Flask(__name__)


'''---------- SCRIPT DEFINITIONS ----------'''
SCRIPT1 = 'bbic_stack'
SCRIPT2 = 'brain_region_filtering'


'''---------- GLOBALS ----------'''
script_thread = None
start_time = None
script_runs = True
error_output = False
resource_stdout = 'STDOUT:\n'
resource_stderr = 'STDERR:\n'
message = 'Task starting...'
script_command = 'Command empty!'
progress = 0


'''---------- API ROUTE DEFINITIONS ----------'''
@app.route("/")
def hello():
    return "API v1.0 to the ResourceConnector module of the Blue Brain Project"


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
    global script_runs
    global message
    global progress

    # Read script output
    command = script_command[0]
    global resource_stdout
    global resource_stderr
    print(resource_stdout)
    print(resource_stderr)

    ### ---------- SCRIPT 1 PROGRESS EXTRATION ---------- ###
    if SCRIPT1 in command:
        # Get status for bbic_stack.py

        # Script specific variables
        SUBTASKS = 4

        out = resource_stdout.split('\n')
        quarters = resource_stdout.count('Done.')

        if resource_stderr.count('ValueError: Unable to create group (Name already exists)') > 0:
            return jsonify({"message": 'Couldn start task because OUTPUT FILE already exists!', "progress": 0})

        if '--all-stacks' in command:
            # Four stacks are run
            if quarters == SUBTASKS:
                # Task is done
                message = 'Task is done.'
                progress = 100
                print(message)
                print('Time taken: '+str(time.clock()-start_time))
                script_runs = False
            elif out[-2][0:11] == '\rProgress: ':
                # Task is partially done
                last_line = out[-2]
                step = float(last_line[11:last_line.find('/')])
                total = float(last_line[last_line.find('/') + 1:])
                fraction = step / total
                quarter = quarters * 100 / SUBTASKS
                subtask = fraction * 100 / SUBTASKS
                progress = int(quarter + subtask)
                message = 'Task in progress...'
            else:
                # Intermediary progress state. Return last known progress
                message = 'Task status unconfirmed'

        else:
            # Single stack is run
            if quarters == 1:
                # Task is done
                progress = 100
                message = 'Task is done.'
                print(message)
                print('Time taken: '+str(time.clock()-start_time))
                script_runs = False
            elif out[-2][0:11] == '\rProgress: ':
                # Task is partially done
                last_line = out[-2]
                step = float(last_line[11:last_line.find('/')])
                total = float(last_line[last_line.find('/') + 1:])
                fraction = step / total
                progress = int(fraction*100)
                message = 'Task in progress...'
            else:
                # Intermediary progress state. Return last known progress
                message = 'Task status unconfirmed'

    ### ---------- SCRIPT 2 PROGRESS EXTRATION ---------- ###
    elif SCRIPT2 in command:
        # Get status for brain_region_filtering.py

        start = resource_stdout.rfind('\rProgress: ')

        if start != -1:
            end = resource_stdout.find('\n', start)
            last_line = resource_stdout[start:end]
            progress = int(last_line[11:])
            message = 'Task in progress...'
            if progress == 100:
                message = 'Task is done.'
        else:
            # Intermediary progress state. Return last known progress
            message = 'Task starting...'

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


@app.route('/'+WRAPPER_NAME+'/v1/exit')
def exit():
    """Call to gracefully shutting down the wrapper
       ---
       responses:
         200:
           description: Gracefully shutting down the wrapper from remote call
           examples:
             exit: True
    """

    # Shutdown script in subprocess
    global script_runs
    script_runs = False

    # Shutdown flask server
    shutdown_server()

    return jsonify({'exit': not script_runs})


'''---------- OPTION PARSER ----------'''
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

    parser.add_option("-p", "--port", dest="port",
                      help="Define which port is to be used for the wrapper to communicate on",
                      action="store", type='string')

    parser.add_option("-t", "--host", dest="host",
                      help="Define which host the wrapper service should be run on",
                      action="store", type='string')

    parser.add_option("-d", "--debug", dest="debug",
                      help="Choose to run the wrapper in debug mode",
                      action="store_true")

    return parser.parse_args()

'''---------- SCRIPT LAUNCH SECTION ----------'''
def launch_script(command):
    """
    Method for script/command launching inside the shell
    :param command: script which should be run inside the shell
    """
    global script_runs
    global error_output
    global resource_stdout
    global resource_stderr

    app.logger.info('Full command: ' + command)
    print('Full command: ' + command)

    global start_time
    start_time = time.clock()

    resource_process = subprocess.Popen(
        [command],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    # Poll process for new output until finished
    while script_runs:
        out_line = resource_process.stdout.readline().decode('utf-8')

        if out_line != '':
            resource_stdout = resource_stdout + out_line

        if error_output:  # only to be used for debugging purposes
            err_line = resource_process.stderr.readline().decode('utf-8')
            if err_line != '':
                resource_stderr = resource_stderr + err_line

    output = resource_process.communicate()[0].decode('utf-8')
    app.logger.info('Full process output: ' + output)
    print('Full process output: ' + output)
    resource_process.stdin.close()


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def run_flask(debug=False):
    """
    Method for launching flask server on available port.
    :param debug: optional
    """

    if debug:
        port = 5000
        host = 'localhost'
    else:
        port = options.port
        host = options.host

    app.run(host=host, port=port, debug=False) #NOT to be run in debug mode, since additional threads interfere with shared variables


if __name__ == "__main__":
    options, args = parse_options()
    script_command = options.script_multiarg

    script_thread = threading.Thread(name='Resource-Script-Thread', target=launch_script, args=script_command)
    script_thread.start()

    run_flask(debug=False)
    script_thread.join()


