from __future__ import print_function
import datetime
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools  # pip install --upgrade google-api-python-client oauth2client
from datetime import datetime

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/calendar.readonly'


class google_cal_client():

    def __init__(self):
        return

    ##################################################################################################
    # End __init__()
    ##################################################################################################

    def _establish_client(self):
        """
        Function uses saved credentials to establish connection to
        calendar and returns client for querying purposes.

        :return
        service : googleapiclient object
        Object used to make requests to established calendar via API

        """
        store = file.Storage('token.json')
        creds = store.get()

        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
            creds = tools.run_flow(flow, store)

        service = build('calendar', 'v3', http=creds.authorize(Http()))
        return service

    ##################################################################################################
    # End _establish_client()
    ##################################################################################################

    def _get_calendar_list(self):
        """
        Function uses googleapiclient object to return list
        of available calendars for current user credentials

        :return:
         None
        """
        # Call the calendar API
        # TODO: confirm change to PST
        now = datetime.now().isoformat()
        service = self._establish_client()
        page_token = None
        calendarIDs = []
        while True:  # Get all available calendars
            calendar_list = service.calendarList().list(pageToken=page_token).execute()
            for calendar_list_entry in calendar_list['items']:
                calendarIDs.append(calendar_list_entry['id'])

            page_token = calendar_list.get('nextPageToken')  # May be multiple calendar pages
            if not page_token:  # No more pages left
                break

    ##################################################################################################
    # End _get_calendar_list() NOTE May not need functionality
    ##################################################################################################

    def get_events(self, start, end, calendar_id=None):
        """
        Function queries specific calendar for booking information
        based on start and end date provided and returns information
        in boolean format.

        :param start: string
        String representation of the start date to query in form 'MM/DD/YYYY hh:mm:ss'

        :param end: string
        String representation of the end date to query in form 'MM/DD/YYYY hh:mm:ss'

        :param calendar_id: string
        String representation of LBNL Google Calendar ID

        :return:
        returned_dict : dictionary
        Dictionary containing boolean values of when room is booked (start:True/end:False)
        Structure of dictionary is tree form [Value] -> [(start,end)]*
        """
        returned_dict = {}
        service = self._establish_client()

        # Define time range that client wants data returned for
        start_time = (datetime.strptime(start, "%m/%d/%Y %H:%M:%S")).isoformat() + 'Z'
        end_time = (datetime.strptime(end, "%m/%d/%Y %H:%M:%S")).isoformat() + 'Z'

        # TODO: Change return to give more accurate description to Skyspark
        event_times = []

        try:
            events_result = service.events().list(calendarId=calendar_id, timeMin=start_time, timeMax=end_time,
                                                  maxResults=60, singleEvents=True,
                                                  orderBy='startTime').execute()
            events = events_result.get('items', [])

        except Exception as e:
            print("\nError getting room data: ", str(e), "\n")
            if 'HttpError 404' in str(e):
                return 404

        if not events:
            return event_times

        for event in events:
            start = event['start'].get('dateTime', event['start'].get('date'))
            start_time = str(datetime.strptime(start, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
            end = event['end'].get('dateTime', event['end'].get('date'))
            end_time = str(datetime.strptime(end, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
            event_times.append([start_time, True])
            event_times.append([end_time, False])

        returned_dict["value"] = event_times
        return returned_dict

##################################################################################################
# End get_events(start, end, calendar_id=None)
##################################################################################################


