import subprocess
import threading
from flask import Flask, jsonify

app = Flask(__name__)

def launch_script():
    resource_process = subprocess.Popen(
        ['"python -u bbic_stack.py ../../log/output_v04.h5 --create-from ../../data/bigbrain600/list.txt --orientation coronal --all-stacks"'],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)


    output = resource_process.communicate()[0].decode('utf-8')
    print('Full command output: ' + output)
    #app.logger.info('Full command output: ' + output)
    resource_process.stdin.close()

def run_flask(debug=False):
    port = 3333
    host = 'localhost'

    app.run(host=host, port=port, debug=debug)

if __name__ == "__main__":

    script_thread = threading.Thread(name='Resource-Script-Thread', target=launch_script)
    script_thread.start()

    run_flask(debug=False) #TODO take care of debug with options.debug flag
    #script_thread.join()
