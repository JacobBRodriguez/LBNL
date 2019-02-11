#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Functions to interact with SkySpark database using Axon queries

Module includes the following functions:
request        Send Axon request to SkySpark, return resulting text
__name__       Simple console to send REST request to SkySpark

Created on Sun Nov 19 15:29:51 2017
Last updated on 2018-11-4

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
import threading
import math
import time

from TS_Util_Clean_Data_test import ts_util
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import scram

# Define constants
DEFAULT_URL = "http://skyspark.lbl.gov/api/lbnl/"
CONFIG_FILE = "./spyspark.cfg"
MAX_ATTEMPTS = 3
tu = ts_util() # For data quality analysis

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
    def __init__(self, URL=None):

        if URL:
            self.URL = URL
        else:
            self.URL  = host_addr

        # authentication is now at the beginning when the class is instantiated
        
        if  not scram.current_token(): # if the current auth token is empty on the file

            scram.update_token()
            
        self.auth_token = scram.current_token()
        
        return

###############################################################################
    def _compose_url(self,query):
        
        """ Construct request URI for querying data from Skyspark database.
        
        Parameters
        ----------
        query : str
            Axon query string.
            
        Returns
        -------
        request_uri : str
            URI string containing host address and Axon query.
            
        """
        
        request_uri = host_addr + "eval?expr=" + urllib.parse.quote(query)

        return request_uri

###############################################################################
    def _send_request(self,request_uri, result_type):
        
        """ Send URI get request to Skyspark database and return http response for later parsing.
        
        Parameters
        ----------
        request_uri : str
            URI string containing host address and Axon query.
        result_type : str
            Resulting format of returned data (application/json, csv, or zinc).
            
        Returns
        -------
        r : text
            Resulting text from returned 'get' request from Skyspark database query. 
            
        """
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

        """ Give appropriate error text based on incoming status code.
        
        Parameters
        ----------
        r : json
            JSON return body.
        result_type : str
            Resulting format of returned data (application/json, csv, or zinc).
            
        Raises
        -------
        Exception
            400 Error exceptions based on incoming status code 
            
        """
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
        
        """ Parse response body from get request if metadata is returned.
        
        Parameters
        ----------
        res : HTTP response body
            Response body of GET request returned from Skyspark database.
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing meter metadata from Skyspark 
            
        """
        
        return pd.io.json.json_normalize(res["rows"])

###############################################################################
    def _parse_TS_data_json(self, res, result_type):
        
        """ Parse response body from get request if time series is returned.
        
        Parameters
        ----------
        res : HTTP response body
            Response body of GET request returned from Skyspark database.   
        result_type : str
            Returned data that is request by user (ts for time series or both for time series and metadata).
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing meter time series data and/or metadata from Skyspark 
            
        """
        
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
        
        """ Parse response body based on result format that incoming data is in.
        
        Parameters
        ----------
        r : HTTP response body
            Response body of GET request returned from Skyspark database.  
        result_format : str
            Requested MIME type in which to receive results (default: "text/csv" for CSV format).    
        result_type : str
            Returned data that is request by user (ts for time series or both for time series and metadata).
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing meter time series data and/or metadata from Skyspark 
            
        """

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
        
        """ Send Axon request to SkySpark through REST API, return resulting text.
        
        Use SkySpark REST API to query the database using the Axon query passed
        as first argument.  Use authorization token stored in spyspark.cfg.  If
        an authorization issue is detected, attempt to re-authorize.  If other
        HTTP issues are detected, raise Exception.  Return result as string.
        
        Note
        ----
        If the Axon query returns 'empty\n', a custom AxonException is raised.
        This can occur if there are no results or if the query is bad.
        
        Parameters
        ----------
        query : str
            Axon query as string.
        result_format : str
            Requested MIME type in which to receive results (default: "text/csv" for CSV format).
        result_type : str
            Requested return type of data (ts or meta).
            
        Returns
        -------
        res : pd.DataFrame()
            Dataframe containing timeseries or metadata data from Skyspark.
            
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
                # First use of client will trigger JSONDecodeError. Need to update auth token and resend request.
                try:
                    res= self._parse_results(r,result_format, result_type)
                except ValueError:
                    scram.update_token()
                    self.auth_token = scram.current_token()
                    r = self._send_request(request_uri, result_format)
                    err = self._manage_errors(r, result_format)

                    if err:
                        return err
                    else:
                        res= self._parse_results(r,result_format, result_type)
                    
                return res

###############################################################################
    def _readAll(self, query, result_format = "application/json", result_type="meta"):
        
        """ Run Axon query and return meter metadata from Skyspark database.
        
        Parameters
        ----------
        query : String
            Axon query. 
        result_format : str
            Returned format from HTTP reponse body (default is application/json).
        result_type : str
            Requested return type of incoming data (dafault is meta).
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing metadata information from Skyspark database.
            
        """

        return self.request(query=query, result_format=result_format, result_type=result_type)

###############################################################################
    def _hisRead(self, query, result_format = "application/json", result_type="ts"):
        
        """ Run Axon query and return meter time series from Skyspark database.
        
        Parameters
        ----------
        query : String
            Axon query. 
        result_format : str
            Returned format from HTTP reponse body (default is application/json).
        result_type : str
            Requested return type of incoming data (dafault is ts).
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing time series information from Skyspark database.
            
        """

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
    def hisWrite(self, query):
     
        return
###############################################################################
# Added functionality 7/10/18 @jbrodriguez@ucdavis.edu
    def query_data(self, query, result_type):
        """ Query Skyspark via Axon language.
        
        Parameters
        ----------
        query : str
            Axon query string.
        result_type : str
            Time series or metadata return option (usage 'ts' or 'meta').
            
        Returns
        -------
        pd.DataFrame()
            Dataframe containing timeseries or metadata data.
            
        """
        if result_type == "meta":
            df = self._readAll(query)
        elif result_type == "ts":
            df = self._hisRead(query)
        return df
###############################################################################
    def _query_now(self, metadata):
        """ Private function to query Skyspark for timeseries data from metadata.
        
        Parameters
        ----------
        metadata : pd.DataFrame()
            Dataframe of metadata returned from get_metadata function.
            
        Returns
        -------
        pd.DataFrame()
            Dataframe containing time series of current time (now).
            
        """
        main_df = pd.DataFrame()
        
        for item in metadata['id']: # for each meter id in metadata dataframe, query for timeseries data and append to main dataframe
            query = 'readAll(equipRef==@'+item.split(' ')[0][2:]+').hisRead(now, {limit: null})'
            ts = self._hisRead(query)
            main_df = main_df.append(ts)
        return main_df 
###############################################################################
    def get_metadata(self, tags):
        
        """ Query Skyspark for meters that match metadata tags.
        
        Parameters
        ----------
        tags : dictionary
            Dictionary of tags for filtering meters.
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing metadata/meters that match given criteria.
            
        """
        # NOTE: Non-marker tags cannot have negated query i.e. 'not link=='28.75'
        query = 'readAll('
        flag = 0
        if tags: # If given list of tags
            for key, value in tags.items(): # For each tag in the list, add to query that is being built
                if flag == 0: # For first tag, do not include 'and'
                    flag = 1
                    # If want to include both gas and elec
                    if ( ('elec or gas' in key) or ('gas or elec' in key) ) and value == True:
                        query = query + '(elec or gas)'
                    # If NOT want to include both gas and elec   
                    elif ( ('elec or gas' in key) or ('gas or elec' in key) ) and value == False: 
                        query = query + 'not (elec or gas)'
                    # If Marker is true
                    elif value == True: 
                        query = query + key
                    # If Marker is false    
                    elif value == False: 
                        query = query + 'not '+ key
                    # If Ref wants to include key        
                    elif ('id' in key): 
                            query = query + key+'==@'+ value
                    # Query based on link number        
                    elif ('link' in key): 
                        query = query + key +'=="'+ value+'"'
                    # Query based on combustionVolume
                    elif ('combustionVolume' in key): 
                        query = query + key+'=='+value
                    # Query based on navName    
                    elif ('navName' in key): 
                        query = query + key +'=="'+ value+'"'
                    # Query based on siteRef    
                    elif ('siteRef' in key): 
                            query = query + key+'==@'+ value
               
                else: # include 'and' for rest of tags in query
                    
                    if ( ('elec or gas' in key) or ('gas or elec' in key) ) and value == True: 
                        query = query + ' and (elec or gas) '
                        
                    elif ( ('elec or gas' in key) or ('gas or elec' in key) ) and value == False:
                        query = query + ' and not (elec or gas)'
                    
                    elif value == True:
                        query = query + ' and ' + key
                        
                    elif value == False:
                        query = query + ' and not '+ key
                        
                    elif ('id' in key):
                        query = query +' and ' + key +'==@'+ value
                            
                    elif ('link' in key):
                        query = query +' and ' + key +'=="'+ value+'"'
                    
                    elif ('combustionVolume' in key):
                        query = query +' and '+ key+'=='+value
                        
                    elif ('navName' in key):
                        query = query +' and '+ key +'=="'+ value+'"'
                        
                    elif ('siteRef' in key):
                        query = query +' and '+ key+'==@'+ value
                        
            query = query + ')'
            print(query) # Output composed query to user
            df = self._readAll(query)
            return df     
###############################################################################
# Function takes in dataframe of metadata information and returns timeseries data in dataframe format
    def get_ts_from_meta(self, metadata):
        
        """ Return all time series data from given meter metadata input.
        
        Parameters
        ----------
        metadata : pd.DataFrame()
            DataFrame containing meter metadata.
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing time series data from meters in metadata input.
            
        """
        return_df = pd.DataFrame()
        now = datetime.datetime.now() # Getting current date so can query data based off of most recent timestamp
        date = now.strftime("%Y,%m,%d")
        
        for meter in metadata['id']: # for each meter id in metadata dataframe, query for timeseries data and append to main dataframe
            query = 'readAll(equipRef==@'+meter.split(' ')[0][2:]+').hisRead(date(2010,01,01)..date('+date+'), {limit: null})'
            timeseries = self._hisRead(query)
            return_df = return_df.append(timeseries)
            
        return return_df
############################################################################### 
# Function is to take in dataframe column (Accumulator, Raw) from gas meters and check if interval data is correct
# If intervals are fine, returns original frame, else returns reindexed dataframe based on reasoned interval
    def do_raw_analysis(self, data):
        
        """ Check if time series data has consistent timestamp or pulse interval for gas meters. Will reindex on assumed interval if no consistency is initially found.
        
        Parameters
        ----------
        data : pd.DataFrame()
            'Accumulator, Raw' DataFrame column from gas meter time series data.
            
        Returns
        -------
        pd.DataFrame()
            DataFrame containing original or reindexed time series data.
            
        """
        data = data.dropna()
        timeFlag = False
        pulseFlag = False
        if data.index.to_series().diff().dt.seconds.std() == 0:
            print('Consistent timestamp interval')

        else:
            timeFlag = True
            print('Inconsistent timestamp interval')

        if (data.diff().min() == 0).bool():
            pulseFlag = True
            print('Inconsistent pulse interval')

        else:
            print('Consistent pulse interval')

        if timeFlag and pulseFlag:
            print('Reindexing data based on assumed interval')
            assumedInterval = data.index.to_series().diff().dt.seconds.min()
            print('Assumed interval is '+str(assumedInterval)+' seconds')
            assumedInterval = str(assumedInterval) + 'S'
            new_index = pd.date_range(start=data.index[0], end=data.index[data.index.size - 1], freq=assumedInterval)
            data = data.reindex(new_index)
            return data

        return data
###############################################################################
# Function is designed for data quality analysis of incoming link data for run_analysis_from_meta function    
    def data_quality_analysis(self, data):

        """ Run data quality analysis on meter data against given metrics to determine overall meter health.
        
        Parameters
        ----------
        data : pd.DataFrame()
            DataFrame containing meter data.
            
        Returns
        -------
        curr_sens : pd.DataFrame()
            DataFrame containing meter metrics and values achieved for each test.
            
        """
        curr_sens= pd.DataFrame()
        curr_sens["first_valid"] = tu.first_valid_per_col(data)
        curr_sens["last_valid"] = tu.last_valid_per_col(data)
        curr_sens["period_length"] = curr_sens["last_valid"] - curr_sens["first_valid"]
        curr_sens["count"] = data.count()
        curr_sens["missing_n"] = tu.count_missing(data, output="number")
        curr_sens["missing_perc"] = tu.count_missing(data, output="percent")
        curr_sens["zeroVal_n"] = tu.count_if(data, operator="=", val=0, output="number")
        curr_sens["zeroVal_perc"] = tu.count_if(data, operator="=", val=0, output="percent")
        curr_sens["flatline_n"] = tu.count_flatlines(data, output="number")
        curr_sens["flatline_perc"] = tu.count_flatlines(data, output="percent")
        curr_sens["outliersStdv_n"] = tu.count_outliers(data,method="std",coeff=3)
        curr_sens["outliersStdv_perc"] = tu.count_outliers(data,method="std",coeff=3,output="percent")
        curr_sens["dominate_issue"] = curr_sens.apply(self._find_dominate_issue, axis=1)
        curr_sens["% ok"] = curr_sens.apply(self._find_percent_ok, axis=1)

        return curr_sens    
###############################################################################
# Helper function to find the max of the data quality analysis columns and returns max name
    def _find_dominate_issue(self, frame):
        
        """ Helper function to data_quality_analysis() to find the maximum value achieved in all columns and returns name of column.
        
        Parameters
        ----------
        frame : pd.DataFrame()
            DataFrame containing meter metrics with values from data_quality_analysis function.
            
        Returns
        -------
        String
            String of the name of the column that achieved highest value from meter analysis.
            
        """
        maxNum = max(frame['missing_perc'],frame['zeroVal_perc'],frame['flatline_perc'],frame['outliersStdv_perc'])
        
        if maxNum == 0:
            return 'All percentages zero'
        elif frame['missing_perc'] == maxNum:
            return 'missing_perc'
        elif frame['zeroVal_perc'] == maxNum:
            return 'zeroVal_perc'
        elif frame['flatline_perc'] == maxNum:
            return 'flatline_perc'
        else:
            return 'outliersStdv_perc'   
###############################################################################
# Helper function to find the percent of OK for data quality analysis and returns that percent
    def _find_percent_ok(self, frame):
        
        """ Helper function to data_quality_analysis() to find the overall percentage of meter being OK based on subtraction of error metrics.
        
        Parameters
        ----------
        frame : pd.DataFrame()
            DataFrame containing meter metrics with values from data_quality_analysis function.
            
        Returns
        -------
        Int
            Integer number representing the percentage value of meter being OK in relation to error metrics.
            
        """
        percent_bad = frame['missing_perc']+frame['zeroVal_perc']+frame['flatline_perc']+frame['outliersStdv_perc']
        return (100 - percent_bad)
###############################################################################
# Helper function to return list of dates that we will be running the analysis on for meter quality checks
    def get_dates_list(self, worksheet, month_range):
        
        """ Helper function to run_analysis_from_meta() to generate list of dates for meter analysis.
        
        Parameters
        ----------
        worksheet : GSpread worksheet
            GSpread library worksheet containing manual read values for given date range and meters.
            
        Returns
        -------
        dateList : list
            List containing dates for given range data analysis will be ran on.
            
        """
        begin_date_range = datetime.datetime.strptime(month_range[0], '%m/%d/%Y').strftime('%Y,%m,%d')
        end_date_range = datetime.datetime.strptime(month_range[1], '%m/%d/%Y').strftime('%Y,%m,%d')
        dates = {}
        dateList = []
        timestamps = worksheet.col_values(1)
        del timestamps[0]
      
        for item in timestamps: 
            if item != '':
                if begin_date_range <= datetime.datetime.strptime(item.replace(" ",""), '%m/%d/%Y').strftime('%Y,%m,%d') <= end_date_range:
                    dates[item] = item
              
        for key, value in dates.items():
            dateList.append(value.replace(" ",""))
            
        return dateList
###############################################################################
# Function takes in dataframe of metadata information and url of GoogleSheets wanting to compare to
    # Returns a dataframe of meter comparison to GoogleSheets  
 
    def run_analysis_from_meta(self, metadata, Sheets_url, month_range, save_each_meter_to_csv=False):
        
        """ Run meter comparison and data quality analysis on meters in metadata DataFrame.
        
        Parameters
        ----------
        metadata : pd.DataFrame()
            DataFrame containing meter metadata.  
        Sheets_url : String
            String of the url that points to the Google Sheets document that contains manual read data.   
        save_each_meter_to_csv : Boolean
            Optional Boolean value to save meter analysis information into csv and png files (default is False).
            
        Returns
        -------
        comparison_df : pd.DataFrame()
            DataFrame containing all meter's comparison information for given time frame.
            
        data_quality_df : pd.DataFrame()
            DataFrame containing all meter's data quality analysis information for given time frame.
            
        """
        # Holding dataframes
        data_quality_df = pd.DataFrame()
        comparison_df = pd.DataFrame()
        
            # GoogleSheets imports
        scope = ['https://spreadsheets.google.com/feeds'] # Keep this as the scope
        credentials = ServiceAccountCredentials.from_json_keyfile_name('My Project-01d0ad43c251.json',scope) # Json comes from Google API                                                    # page. Can just use my email on API page from this to access spreadsheets
        gc = gspread.authorize(credentials)
        
        Spreadsheet = gc.open_by_url(Sheets_url)
        worksheet = Spreadsheet.get_worksheet(0)
        months_list = self.get_dates_list(worksheet,month_range)
        link_num_list = worksheet.get_all_values()
 
        last_month_index = (len(months_list)-1)
        date_time_start = datetime.datetime.strptime(months_list[0], '%m/%d/%Y').strftime('%Y,%m,%d')
        date_time_end = datetime.datetime.strptime(months_list[last_month_index], '%m/%d/%Y').strftime('%Y,%m,%d')
        
        for item in metadata['link']: # For each link number in list
           
            if not math.isnan(float(item)):
                link_float = "{:.2f}".format(float(item)) # Need float to retain 2 past decimal point precision
                info_get = self.query('readAll(equipRef->link=="'+item+'").hisRead(date('+date_time_start+')..date('+date_time_end+'), {limit: null})')
                time_dict = {}
                manual_read_value = {}
                skyspark_value = {}
                energy_column = ""
                multiplier = 0 # Need multiplier for Skyspark energy data for gas and water
                
                for meter in info_get.columns:
                    if 'BTU' in meter:
                        multiplier = pow(10,5)
                        break
                    elif 'kWh' in meter:
                        multiplier = 1
                        break
                    
                for column in info_get.columns:
                    if("Energy" in column and "Accumulator" not in column):
                        energy_column = column

                for k,i in enumerate(months_list):
                    #do something with index k
                    #do something with element i # taking list of dates needed for comparison
                    if k > (last_month_index - 1):
                        break
                    try:
                        timeStart = i + " 12:15:00"
                        timeEnd = months_list[k+1] + " 12:00:00"
                        
                        ts_data = info_get[energy_column].to_frame()[timeStart : timeEnd]
                        ts_data = (float(ts_data.sum()[0])) / multiplier # Multiplier = 1 for electricity meters and 10^5 for gas meters
                        for row in link_num_list:
                            if row[1] == link_float:
                                
                                if pd.to_datetime(row[0]) == datetime.datetime.strptime(months_list[k+1], '%m/%d/%Y'):
                                    time_dict[months_list[k+1]] = ((float(row[4].replace(',','')) - ts_data) /                                                                                          float(row[4].replace(',',''))) * 100 # Percent different
                                    manual_read_value[months_list[k+1]] = float(row[4].replace(',',''))
                                    skyspark_value[months_list[k+1]] = ts_data
                                    

                    except:
                        print("Error for data: "+i+" on meter "+item)

            try:
                data_quality_analysis_info = self.data_quality_analysis(info_get)
                data_quality_df = data_quality_df.append(data_quality_analysis_info)

                percent_difference = pd.DataFrame(time_dict, index=[0]).T.rename( columns={0:"Percent Dif"})
                manual_read_dict = pd.DataFrame(manual_read_value, index=[0]).T.rename( columns={0:"Manual Read"})
                skyspark_dict = pd.DataFrame(skyspark_value, index=[0]).T.rename( columns={0:"Skyspark Read"})
                result = pd.concat([manual_read_dict, skyspark_dict, percent_difference], axis=1)
                result.index = pd.to_datetime(result.index, format="%m/%d/%Y")
                result = result.sort_index()
                comparison_df[item] = result["Percent Dif"]
                barplot = result.plot.bar(y=["Manual Read", "Skyspark Read"], rot=0, figsize=(22,10))
                fig = barplot.get_figure()
                fig.autofmt_xdate()

                if save_each_meter_to_csv: # If we want to save each meter to an individual
                    link_string = "Link_"+str(item)
                    result.to_csv(link_string+"_Reads.csv")
                    fig.savefig(link_string+".png")
                        
            except:    
                    print("Error on link number: "+item)
                    
        comparison_df.index = pd.to_datetime(comparison_df.index)
        comparison_df = comparison_df.sort_index()
        return comparison_df, data_quality_df
###############################################################################
    def query(self, query):
        
        """ Run Axon query based on link number or equipRef and return time series or metadata.
        
        Parameters
        ----------
        query : String
            Axon query containing link number/equipRef ID.
            
        Returns
        -------
        df : pd.DataFrame()
            DataFrame containing time series or metadata information from Skyspark database.
            
        """
        df = pd.DataFrame()
        if ("->link==" in query) and (".hisRead" not in query):
            df = self._readAll(query)
        elif ("equipRef==" in query) and (".hisRead" not in query):
            df = self._readAll(query)
        elif ("id==" in query) and (".hisRead" not in query):
            df = self._readAll(query)
        elif ("->link==" in query) and (".hisRead" in query):
            df = self._hisRead(query)
        elif ("equipRef==" in query) and (".hisRead" in query):
            df = self._hisRead(query)
        elif ("id==" in query) and (".hisRead" in query):
            df = self._hisRead(query)
        return df
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
