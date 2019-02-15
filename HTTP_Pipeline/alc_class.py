from suds.client import Client  # pip install suds-jurko
from suds.transport.https import HttpAuthenticated
from urllib.request import HTTPSHandler
import ssl
from datetime import datetime


class _Custom_Transport(HttpAuthenticated):

    def u2handlers(self):
        '''Defines an http handler appropriate for the b59 server.

        Returns
        -------

        handlers : urllib2 https handler
        HTTPS communications handler with context configured for
        communication with ALC server.

        '''

        # Use handlers from superclass
        handlers = HttpAuthenticated.u2handlers(self)

        # Create ssl context with correct tls version and cipher set
        ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ctx.set_ciphers('HIGH:!DH:!aNULL')
        # Add a https handler using the ssl context
        handlers.append(HTTPSHandler(context=ctx))

        return handlers


##################################################################################################
# End Class _Custom_Transport
##################################################################################################

class alc_client():

    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password
        return

    ##################################################################################################
    # End __init__()
    ##################################################################################################

    def _connect(self):
        """Connect to the ALC server SOAP interface.

        Returns
        -------
        client : suds.client.Client
        The connection client with which to call services.

        """

        wsdlurl = 'https://alc-50a-webctrl.lbl.gov/_common/webservices/Trend?wsdl'
        client = Client(wsdlurl, transport=_Custom_Transport())
        client.set_options(username=self.username)
        client.set_options(password=self.password)

        return client

    ##################################################################################################
    # End _connect()
    ##################################################################################################

    def collect_data(self, trend_log_paths, start_time, final_time, columns=None):

        """Collect data from ALC server via SOAP interface.

        Parameters
        ----------
        trend_log_paths : list of str
        TList of strings of the paths to the trend logs on the ALC server.

        For example: ``["#lbnl_59-bl-024/fan_spd"]``

        start_time : string
        Start time of data collection in ``mm/dd/yyyy hh:mm:ss AM/PM`` format.
        final_time : string
        Final time of data collection in ``mm/dd/yyyy hh:mm:ss AM/PM`` format.
        columns : list of str, default = None (Deprecated. Not operation dependent. Remove in later updates.)
        A list of strings in the same order as trend_log_paths to rename the
        corresponding trend_log_path column header in the returned pandas
        DataFrame.

        Returns
        -------

        holdingDict : dictionary
        Dictionary that mirrors structure of ElasticSearch information with Tree structure of {value -> [{DateTime,Data}*]}

        Other Requirements
        -------
        Create a text file named "ErrorReport" in the same directory as this file.
        The failed collected data from ALC server will be recorded in the "ErrorReport" file

        """

        # Gather trend data
        limit_from_start = True
        max_records = 0
        client = self._connect()
        i = 1

        if not columns:
            columns = trend_log_paths

        for log, column in zip(trend_log_paths, columns):

            try:
                r = client.service.getTrendData(log, start_time, final_time, limit_from_start, max_records)

                # Parse and convert to dictionary
                time = r[::2]
                data = [float(x) for x in r[1::2]]

            except Exception as e:
                print("\nError getting meter data: ", str(e), "\n")

                if 'does not exist' in str(e):  # Query incorrect
                    return 204

                elif 'Unauthorized' in str(e):  # Credentials incorrect
                    return 401

                elif 'Trends are not enabled' in str(e):  # Trend data not enabled
                    return 501

        dictionary = dict(zip(time, data))
        dictlist = []
        holdingDict = {}

        for key, value in dictionary.items():
            dictlist.append([str(datetime.strptime(key, "%m/%d/%Y %I:%M:%S %p").strftime("%Y-%m-%d %H:%M:%S")), value])
        holdingDict["value"] = dictlist

        return holdingDict

##################################################################################################
# End collect_data()
##################################################################################################
