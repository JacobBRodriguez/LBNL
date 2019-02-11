from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import json
import requests as req
import urllib.parse
import yaml# pip install pyyaml
from alc_class import alc_client
from elastic_class import elastic_client
from google_calendar_class import google_cal_client
from datetime import datetime

# Host name and port number that server will operate under for Skyspark to discover
hostName = "localhost"
hostPort = 9000

# URI and headers for Elastic Search POST request.
REMOTE_URI_NERSC = 'https://fn.nersc.gov/t/get_timeseries/get_timeseries'
REMOTE_HEADERS_NERSC = {"Content-Type": "application/json; charset=utf8"}

# Read in authentication credentials from yaml configuration file
with open('authentication.yaml', 'r') as file_auth:
    authentication_yaml = yaml.load(file_auth)

# Declare ALC client from alc_class.py with credentials from yaml file
ALC_CLIENT = alc_client(username=authentication_yaml['ALC']['username'],password=authentication_yaml['ALC']['password'])
# Declare ELASTIC client from elastic_class.py with credentials from yaml file
ELASTIC_CLIENT = elastic_client(uri=REMOTE_URI_NERSC,headers=REMOTE_HEADERS_NERSC,username=authentication_yaml['ElasticSearch']['username'],password=authentication_yaml['ElasticSearch']['password'])
# Declare CALENDAR client from google_calendar_class.py. Credentials supplied through JSON file
CALENDAR_CLIENT = google_cal_client()


class MyServer(BaseHTTPRequestHandler):

    def do_GET(self):
        '''Defines a GET request handler for server http requests
        Parameters
        ----------
        self : object
        class object that contains incoming URI from Skyspark. 
        Can reference URI path and parse accordingly.

        Returns
        -------
        None
        Returns no data to outer function, instead writes to requesting socket.

        '''

        if self.path.endswith("elastic"):
            unquoted_path = urllib.parse.unquote_plus(self.path)
            items = unquoted_path.split('?') # Payload = 1	
            data = items[1]
			
            ret = ELASTIC_CLIENT.get_timeseries(data=data)
	
            # URI path not found
            if ret == 404:
                self.send_response(404)
                self.end_headers()
            # Incorrect query returned no results
            if ret == 204:
                self.send_response(204)
                self.end_headers()
            # Unauthorized with given credentials
            elif ret == 401:
                self.send_response(401)
                self.end_headers()
            # Write back JSON data
            else:
                payload = json.dumps(ret)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(payload.encode('utf-8'))

        elif self.path.endswith("alc"):
            unquoted_path = urllib.parse.unquote_plus(self.path)
            items = unquoted_path.split('?') # Log = 1, start_date = 2, end = 3
            data = [items[1]] # Logs go in as a list
            start_date = datetime.strptime(items[2], '%Y-%m-%d %I:%M:%S %p').strftime('%m/%d/%Y %I:%M:%S %p')

            end_date = datetime.strptime(items[3], '%Y-%m-%d %I:%M:%S %p').strftime('%m/%d/%Y %I:%M:%S %p')

            ret = ALC_CLIENT.collect_data(trend_log_paths=data, start_time=start_date, final_time=end_date)

            # Incorrect query returned no results
            if ret == 204:
                self.send_response(204)
                self.end_headers()
            # Unauthorized with given credentials
            elif ret == 401:
                self.send_response(401)
                self.end_headers()
            # Trends for the specified point are not enabled on server side
            elif ret == 501:
                self.send_response(501)
                self.end_headers()
            # Write back JSON data
            else:
                payload = json.dumps(ret)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(payload.encode('utf-8'))

        elif self.path.endswith("calendar"):
            print("Hi Jacob")
            unquoted_path = urllib.parse.unquote_plus(self.path)
            items = unquoted_path.split('?') # ID = 1
            data = items[1]

            ret = CALENDAR_CLIENT.get_events(calendar_id=data)

            # Incorrect query returned no results
            if ret == 204:
                self.send_response(204)
                self.end_headers()
            # Unauthorized with given credentials
            elif ret == 401:
                self.send_response(401)
                self.end_headers()
            # Trends for the specified point are not enabled on server side
            elif ret == 501:
                self.send_response(501)
                self.end_headers()
            # Write back JSON data
            else:
                payload = json.dumps(ret)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(payload.encode('utf-8'))

def main():
    # Declare HTTP server request object with declared hostname and port number.
    myServer = HTTPServer((hostName, hostPort), MyServer)
    print(time.asctime(), "Server Starts - %s:%s" % (hostName, hostPort))

    try:# Run server forever or until keyboard termination.
        myServer.serve_forever()
    except KeyboardInterrupt:
        print("Keyboard interrupt. Server terminated")
        myServer.socket.close()


main()
