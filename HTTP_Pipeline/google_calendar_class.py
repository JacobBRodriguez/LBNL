from __future__ import print_function
import datetime
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools # pip install --upgrade google-api-python-client oauth2client
from datetime import datetime, timedelta

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/calendar.readonly'

class google_cal_client():

	def __init__(self):
		return

##################################################################################################
# End __init__()
##################################################################################################

	def _establish_client(self):
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
		# Call the calendar API
		# TODO: confirm change to PST
		now = datetime.now().isoformat()
		service = self._establish_client()
		page_token = None
		calendarIDs =[]
		while True: # Get all available calendars
			calendar_list = service.calendarList().list(pageToken=page_token).execute()
			for calendar_list_entry in calendar_list['items']:
				calendarIDs.append(calendar_list_entry['id'])

			page_token = calendar_list.get('nextPageToken') # May be multiple calendar pages
			if not page_token: # No more pages left
				break
##################################################################################################
# End _get_calendar_list(client=None) NOTE May not need functionality
##################################################################################################

	def get_events(self, calendar_id=None):

		returned_dict = {}
		service = self._establish_client()
		# Set datetime to exactly 1 day ago so as to get recent history of bookings and future bookings.
		now = (datetime.now() - timedelta(1)).isoformat() + 'Z'
		# TODO: Change return to give more accurate description to Skyspark
		event_times = []

		try:
			events_result = service.events().list(calendarId=calendar_id, timeMin=now, maxResults=30, singleEvents=True, orderBy='startTime').execute()
			events = events_result.get('items', [])

		except Exception as e:
			print("\nError getting room data: ",str(e),"\n")
			if 'HttpError 404' in str(e):
				return 404

		if not events:
			return event_times

		for event in events:
			start = event['start'].get('dateTime', event['start'].get('date'))
			start_time = str(datetime.strptime(start, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
			end = event['end'].get('dateTime', event['end'].get('date'))
			end_time = str(datetime.strptime(end, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
			event_times.append([start_time, 1])
			event_times.append([end_time, 0])

		returned_dict["value"] = event_times
		return returned_dict
		
##################################################################################################
# End get_events(calendar_id=None)
##################################################################################################



