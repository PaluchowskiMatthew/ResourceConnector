from flask import Flask, jsonify
#from flasgger import Swagger

from optparse import OptionParser

import socket
import threading
import subprocess
import json
import time

import sys; print(sys.executable)
import os; print(os.getcwd())
import sys; print(sys.path)


WRAPPER_NAME = 'resourceconnector'
SCHEMA_FILE = 'wrapper_schema.json'

SCRIPT1 = 'bbic_stack'
SCRIPT2 = 'brain_region_filtering'

script_thread = None
script_command = 'No command!'
xlock = threading.Lock()
#global task_output
task_output = 'No output!\n'

app = Flask(__name__)
#Swagger(app)


@app.route("/")
def hello():
    return "Welcome to the World Wide Wrapper!"


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
    progress = 0
    message = 'Task starting...'
    command = script_command[0]

    if SCRIPT1 in command:
        # Get status for bbic_stack.py
        # Script specific variables
        SUBTASKS = 4

        #nextline = process.stdout.readline() #decode('utf-8')
        global task_output
        #task_output += str('Listen when i ask -------------')  # +'\n'
        #with xlock:
        out = task_output.split('\n')
        quarters = task_output.count('Done.')

        if '--all-stacks' in command:
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

    parser.add_option("-p", "--port", dest="port",
                      help="Define which port is to be used for the wrapper to communicate on",
                      action="callback", callback=vararg_callback)

    parser.add_option("-t", "--host", dest="host",
                      help="Define which host the wrapper service should be run on",
                      action="callback", callback=vararg_callback)

    parser.add_option("-d", "--debug", dest="debug",
                      help="Choose to run the wrapper in debug mode",
                      action="store_true")

    return parser.parse_args()


def launch_script(command):
    global task_output

    """
    Method for script/command launching inside the shell
    :param command: script which should be run inside the shell
    """
    app.logger.info('Full command:\n' + command)
    #print('Full command:\n' + command)

    #global process
    process = subprocess.Popen(
        [command],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    # Poll process for new output until finished
    while True:
        from subprocess import check_output

        #taskcheck_output = check_output(["ntpq", "-p"])
        task_line = process.stdout.readline().decode('utf-8')
        #print('Tasks line: ' + task_line)

        #task_err = process.stderr.readline().decode('utf-8')
        #print('Tasks check: '+taskcheck_output)
        if task_line != '':
            #with xlock:
            task_output += str(task_line) #+'\n'
            print('Begin Task.')
            print('Tasks: '+ task_output)
            print('End Task.')
        #print('Tasks error: '+task_err)
        # if task_output.split('\n')[-1] == '' and process.poll() != None:
        #    break
        # sys.stdout.write(task_output)
        # sys.stdout.write(task_err)
        #sys.stdout.flush()
        out2 = task_output
        time.sleep(1)

    global output
    output = process.communicate()[0].decode('utf-8')
    print('Full command output: ' + output)
    app.logger.info('Full command output: ' + output)
    process.stdin.close()


def run_flask(debug=False):
    """
    Method for launching flask server on available port.
    :param debug: optional
    """

    if debug:
        port = 3333
        host = 'localhost'
    else:
        port = options.port
        host = options.host

    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    options, args = parse_options()
    script_command = options.script_multiarg

    print(script_command)

    script_thread = threading.Thread(target=launch_script, args=script_command)
    script_thread.start()

    run_flask(debug=True) #TODO take care of debug with options.debug flag


