from flask import Flask, render_template, request, session, redirect, jsonify, json

from uuid import uuid4
import os, time, sys, argparse
from configparser import ConfigParser

from dynamodb.connectionManager import ConnectionManager
from dynamodb.cameraController import CameraController
from models.camera import Camera 

application = Flask(__name__)
application.debug = True
application.secret_key = str(uuid4())

"""
   Define the urls and actions the app responds to
"""

@application.route('/')
def index():
    return 'Index Page'

@application.route('/create')
def create():
    return 'create'

@application.route('/read')
def db_read():
    return 'read'

@application.route('/update')
def db_update():
    return 'udpate'

@application.route('/destroy')
def db_destroy():
    return 'destroy' 

@application.route('/create_table/<table_name>', methods=["GET", "POST"])
def createTable(table_name):
    cm.createDevicesTable(table_name)

    while controller.checkIfTableIsActive(table_name) == False:
        time.sleep(3)

    return 'create_table %s' % table_name

@application.route('/delete_table/<table_name>', methods=['GET', 'POST'])
def delete_table(table_name):
    cm.destroyTable(talbe_name)
    return True

if __name__ == '__main__':
    """
       Configure the application according to the command line args and config files
    """

    parser = argparse.ArgumentParser(description='Run the DynamoDB manager service', prog='application.py')
    parser.add_argument('--config', help='')
    parser.add_argument('--mode', help='', choices=['local', 'service'], default='service')
    parser.add_argument('--endpoint_url', help='')
    parser.add_argument('--port', help='', type=int)
    parser.add_argument('--serverPort', help='', type=int)

    args = parser.parse_args()

    configFile = args.config
    config = None

    use_instance_metadata = ''

    cm = ConnectionManager(mode=args.mode, config=config, endpoint_url=args.endpoint_url, port=args.port, use_instance_metadata=use_instance_metadata)
    controller = CameraController(cm)

    serverPort = args.serverPort
    if serverPort is None:
        serverPort = 5000
    if cm:
        application.run(debug=True, port=serverPort, host='0.0.0.0')
