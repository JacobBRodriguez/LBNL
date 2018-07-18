#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Functions to interact with SkySpark database using Axon queries

Module includes the following functions:
request        Send Axon request to SkySpark, return resulting text
__name__       Simple console to send REST request to SkySpark

Created on Sun Nov 19 15:29:51 2017
Last updated on 2018-07-18

@author: rvitti
@author: marco.pritoni
@author: jrodriguez13

"""
import configparser
import re
import requests
import urllib.parse
import pandas as pd
import json
import datetime


import scram

# Define constants
DEFAULT_URL = "http://skyspark.lbl.gov/api/lbnl/"
CONFIG_FILE = "./spyspark.cfg"
MAX_ATTEMPTS = 3

# Define global module variables, in particular config object
config = configparser.ConfigParser()
result_list = config.read(CONFIG_FILE)
if not result_list:
    raise Exception("Missing config file spyspark.cfg")
host_addr = config['Host']['Address']


# Exception raised if empty result is received from SkySpark
class AxonException(Exception):
    pass


class spyspark_client(object):

###############################################################################    
    def __init__(self, URL=None, config_file=None):

        if URL:
            self.URL = URL
        else:
            self.URL  = host_addr

        # authentication is now at the beginning when the class is instantiated
        
        if  not scram.current_token(): # if the current auth token is empty on the file

            scram.update_token()
            
        self.auth_token = scram.current_token()
        
        #Added functionaily from Andrew's code @jbrodriguez@ucdavis.edu
        
        Config = configparser.ConfigParser()
        Config.read(config_file)
        self.host = Config.get("DB_config", "host")
        self.username = Config.get("DB_config", "username")
        self.password = Config.get("DB_config", "password")
        self.database = Config.get("skyspark_meta", "database")
        self.protocol = Config.get("DB_config", "protocol")
        self.measurement = Config.get("skyspark_meta", "measurement")
        self.port = Config.get("DB_config", "port")
        self.tags = Config.get("skyspark_meta", "tags")
        self.tags = self.tags.split(',')
        self.fields = Config.get("skyspark_meta", "fields")
        self.fields = self.fields.split(',')

        return

###############################################################################
    def _compose_url(self,query):
        
        request_uri = host_addr + "eval?expr=" + urllib.parse.quote(query)

        return request_uri

###############################################################################
    def _send_request(self,request_uri, result_type):

        #auth_token = scram.current_token() # removed because already saved in class variable
        headers= {"authorization": "BEARER authToken="+self.auth_token,
                  "accept": result_type}
        try:
            r = requests.get(request_uri, headers=headers)
        except e:
            print("Error in request: "+e)
            return None
        return r

###############################################################################
    def _manage_errors(self,r,result_type):

        if r.status_code == 200:
            if r.text != "empty\n":
                return
            else:
                raise AxonException("Empty result, check query")
        if r.status_code == 400:    # Missing required header
            raise Exception("HTTP request is missing a required header")
        if r.status_code == 404:    # Invalid URI
            raise Exception("URI does not map to a valid operation URI")    
        if r.status_code == 406:    # Invalid "accept" header
            raise Exception("Unsupported MIME type requested")
        if r.status_code == 403:    # Authorization issue, try to reauthorize
            scram.update_token()
            self.auth_token = scram.current_token() # added to save new auth_token in class variable

        else:
            raise Exception("HTTP error: %d" % r.status_code)

###############################################################################
    def _parse_metadata_table_json(self, res):
        
        return pd.io.json.json_normalize(res["rows"])

###############################################################################
    def _parse_TS_data_json(self, res, result_type):
        
        ## get metadata info
        metadata = (pd.io.json.json_normalize(res["cols"][1:len(res["cols"])]))
        ## transform json into dataframe (TODO: add example for documentation)

        TSdata = (pd.io.json.json_normalize(res["rows"]))
        
        
        ## format timestamp data inside the dataframe
        # remove intial 't:' and final tz from timestamp: 't:2017-11-26T00:25:00-08:00 Los_Angeles',
        pat = r"(t:)([0-9T\-:]{19})(.{3,})" # regex to separate into three groups
        repl = lambda m: m.group(2) # take central group
        try:
            TSdata["ts"] = pd.to_datetime(TSdata["ts"].str.replace(pat, repl)) # change into datetime type and apply regex
        except:
            #raise Exception("No time series data returned from query")
            print("No time series data returned from query")
            return None
        TSdata.set_index("ts", inplace=True, drop=True) # set datetime as index
        #### need to do: need to correct for timezone!!!

        ## format numerical values inside the dataframe
        # remove intial 'n:' and final unit from value: 'n:74.5999984741211 °F',    
        pat = r"(n:)([0-9\.]{1,})(\s.{2,})" # regex to separate into three groups
        repl = lambda m: m.group(2) # take central group    
        cols = TSdata.columns.tolist()
        for col in cols:
            TSdata[col] = pd.to_numeric(TSdata[col].str.replace(pat, repl),errors="coerce") # get value

        ## rename columns based on id from the metadata
        #regx = r"(\s)(.+)"
        #test_str = res["cols"][1]["id"]
        #matches = re.search(regx, test_str)
        #matches.group(2)
        #cols_name = res["cols"][1]["id"]
        TSdata.columns = metadata.loc[metadata["name"].isin(TSdata.columns.tolist()),"id"].tolist()
        
        if result_type == "both":
            return metadata, TSdata
        elif result_type == "ts": 
            return TSdata

###############################################################################
    def _parse_results(self, r, result_format, result_type):

        ## this method manages the different result_formats: csv, json, zinc

        ## csv
        if result_format == "text/csv":
            text = re.sub('â\x9c\x93','True',r.text)    # Checkmarks
            text = re.sub('Â','',text)
            return text

        ## json
        elif result_format == "application/json" :

            res = json.loads(r.text)

            if result_type == "meta":
                return self._parse_metadata_table_json(res)
            elif result_type == "ts":
                return self._parse_TS_data_json(res=res,result_type=result_type)
            elif result_type == "both":
                return self._parse_TS_data_json(res=res,result_type=result_type)

        ## TODO: add zinc
        elif result_format == "text/zinc":

            return r


###############################################################################
    def request(self, query: str, result_format: str = "application/json", result_type: str = "meta"):  ## -> str: removed type returned, more flex! 
        """Send Axon request to SkySpark through REST API, return resulting text
        
        Use SkySpark REST API to query the database using the Axon query passed
        as first argument.  Use authorization token stored in spyspark.cfg.  If
        an authorization issue is detected, attempt to re-authorize.  If other
        HTTP issues are detected, raise Exception.  Return result as string.
        
        If the Axon query returns 'empty\n', a custom AxonException is raised.
        This can occur if there are no results or if the query is bad.
        
        Keyword arguments:
        query       -- Axon query as string
        result_type -- Requested MIME type in which to receive results
                       (default: "text/csv" for CSV format)
        """

        ## I slit this into subparts
        ## 1 - compose url
        request_uri = self._compose_url(query)
        
        # Attempt to send request; if an authorization issue is detected,
        # retry after updating the authorization token

        for i in range(0, MAX_ATTEMPTS):

            ## 2 - get auth token and send request
            r = self._send_request(request_uri, result_format)

            ## 3 - manage exceptions
            err = self._manage_errors(r, result_format)

            if err:
                return err

            ## 4 - parse results
            else:
                res= self._parse_results(r,result_format, result_type)

                return res

###############################################################################
    def _readAll(self, query, result_format = "application/json", result_type="meta"):


        return self.request(query=query, result_format=result_format, result_type=result_type)

###############################################################################
    def _hisRead(self, query, result_format = "application/json", result_type="ts"):

        # eventually we want to dot this after a readAll

        return self.request(query=query, result_format=result_format, result_type=result_type)

###############################################################################
    def read(self,):

        return

###############################################################################
    def eval(self,):

        return

###############################################################################
    def evalAll(self,):

        return
###############################################################################
    def hisWrite(self,):

        return

###############################################################################
# Added functionality 7/10/18 @jbrodriguez@ucdavis.edu
    def _transform_to_dict(s, key): # Prototype function. May not use.
        dic = {}
        dic[key] = s
        return dic

###############################################################################
    def _build_json(self, data, tags, fields, measurement): # Prototype function. May not use.
        data['measurement'] = measurement
        data['fields'] = data.iloc[:,2].apply(self._transform_to_dict, key=fields)
        data['tags'] = data.iloc[:,1].apply(self._transform_to_dict, key=tags)
        data['time'] = data['time']
        json = data[['measurement','time','tags','fields']].to_dict("records")
        return json

###############################################################################
    def query_data(self, query, result_type): # Basic query of all meta and timeseries data
        if result_type == "meta":
            df = self._readAll(query)
        elif result_type == "ts":
            df = self._hisRead(query)
        return df
###############################################################################
# Function takes in dictionary of tags to query database on and returns metadata that matches tag requirements
    def get_metadata(self, tags):
        query = 'readAll('
        flag = 0
        if tags: # If given list of tags
            for key, value in tags.items(): # For each tag in the list, add to query that is being built
                if flag == 0: # For first tag, do not include 'and'
                    flag = 1
                    if value == True: # If Marker is true
                        query = query + key
                        
                    elif value == False: # If Marker is false
                        query = query + 'not '+ key
                            
                    elif ('id' in key): # If Ref wants to include key
                            query = query + key+'==@'+ value
                    
                    # NOTE: Non-marker tags cannot have negated query i.e. 'not link=='28.75'
                    #elif ('not' in value) and ('link' in key): # Query based on link number
                     #   query = query + 'not '+key+'=="'+ value.split(' ')[1]+'"'
                            
                    elif ('link' in key): # Query based on link number
                        query = query + key +'=="'+ value+'"'
                    
                    elif ('combustionVolume' in key): # Query based on combustionVolume
                        query = query + key+'=='+value
                        
                    elif ('navName' in key): # Query based on navName
                        query = query + key +'=="'+ value+'"'
                        
                    elif ('siteRef' in key): # Query based on siteRef
                            query = query + key+'==@'+ value
               
                else: # include 'and' for rest of tags in query
                    if value == True: # Query based on marker tags
                        query = query + ' and ' + key
                        
                    elif value == False: # Marker tag queries
                        query = query + ' and not '+ key
                        
                    elif ('id' in key): # If Ref wants to include key
                        query = query +' and ' + key +'==@'+ value
                            
                    elif ('link' in key): # For link number
                        query = query +' and ' + key +'=="'+ value+'"'
                    
                    elif ('combustionVolume' in key): # For combustionVolume
                        query = query +' and '+ key+'=='+value
                        
                    elif ('navName' in key): # For navName
                        query = query +' and '+ key +'=="'+ value+'"'
                        
                    elif ('siteRef' in key): # For siteRef
                        query = query +' and '+ key+'==@'+ value
                        
            query = query + ')'
            print(query)
            df = self._readAll(query)
            return df    
###############################################################################
# Function takes in dataframe of metadata information and returns timeseries data in dataframe format
    def get_ts_from_meta(self, metadata):
        
        main_df = pd.DataFrame()
        now = datetime.datetime.now() # Getting current date so can query data based off of most recent timestamp
        date = now.strftime("%Y,%m,%d")
        
        for item in metadata['id']: # for each meter id in metadata dataframe, query for timeseries data and append to main dataframe
            query = 'readAll(equipRef==@'+item.split(' ')[0][2:]+').hisRead(date(2010,01,01)..date('+date+'), {limit: null})'
            ts = self._hisRead(query) # Get historical timeseries data from query
            main_df = main_df.append(ts) # Append returned timeseries data to current dataframe
            
        return main_df
###############################################################################
    def query(self, query): # Querying for data based on user input query. 
                            # Can be and type of axon query that user defines. Returns dataframe
        df = pd.DataFrame()
        if ("->link==" in query) and (".hisRead" not in query):
            df = self._readAll(query)
        elif ("equipRef==" in query) and (".hisRead" not in query):
            df = self._readAll(query)
        elif ("->link==" in query) and (".hisRead" in query):
            df = self._hisRead(query)
        elif ("equipRef==" in query) and (".hisRead" in query):
            df = self._hisRead(query)
        return df
###############################################################################
    def _make_client(self): # Prototype function that may or may not use from influx. Might use Andrew's code as class object
        self.client = InfluxDBClient(self.host, self.port, self.username, self.password, self.database, ssl=True, verify_ssl=True)
        self.df_client = DataFrameClient(self.host, self.port, self.username, self.password, self.database, ssl=True, verify_ssl=True)
        
###############################################################################
    def send_to_influx(self, data): # Prototype function that might throw out in favor of class object from Andrew's Influxdb code
        data = data.stack()
        data = data.reset_index()
        data.columns = ['time', 'Meter_Name', 'Value']
        data.dropna(inplace=True)
        measurement = 'measurement'
        tags = 'Meter Name'
        fields = 'Value'
        json = self._build_json(data, tags, fields, measurement)
        return data
###############################################################################
if __name__ == '__main__':
    """Simple console to send REST request to SkySpark and display results"""
    ref = "https://skyfoundry.com/doc/docSkySpark/Axon"
    sample = "read(point and siteRef->dis==\"Building 77\" and " + \
             "equipRef->dis==\"AHU-33\" and discharge and air " + \
             "and temp and sensor).hisRead(yesterday))\n" +\
             "Enter 'q' or 'quit' to exit"
    query = ""
    while query.lower() != "quit" and query.lower() != "q":
        query = input("Enter Axon query:\n>")
        if query.lower() == "help":
            print("""\nReference: %s\nExample: %s""" % (ref, sample))
        elif query.lower() != "quit" and query.lower() != "q":
            try:
                print(request(query))
            except AxonException as e:
                print(e.args[0]+'\n')
