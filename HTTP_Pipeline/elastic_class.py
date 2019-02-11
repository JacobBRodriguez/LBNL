import json
import requests as req

class elastic_client():
	
	def __init__(self, uri=None, headers=None, username=None, password=None):
		self.uri = uri
		self.headers = headers		
		self.username = username
		self.password = password
		return
##################################################################################################
# End __init__()
##################################################################################################
	def get_timeseries(self, data):
		'''Function to get timeseries data from ElasticSearch for an endpoint and parse results
		Parameters
		----------
		data : string
		String composed of metrics to find exact point in ElasticSearch

		Returns
		-------
		returned_dict : dictionary
		Dictionary of parsed timesereis information with Tree structure of {value -> [[DateTime,Data]*]}

		Raises
		------
		401: Unauthorized credentials 
		404: Not Found/Query incorrect
		
		'''
		
		returned_dict = {}
		try:
			ret = req.post(self.uri, headers=self.headers,auth=(self.username,self.password),data=data)
			var = json.loads(ret.json())
			value_list = []
			for item in var['value']:
				value_list.append([item[0],item[1]])

			returned_dict["value"] = value_list
			
		except Exception as e:
			print("\nError getting meter data: ",str(e),"\n")

			if ret.status_code == 401: # Credentials incorrect
				return 401
			elif ret.status_code == 404: # URL incorrect or not found
				return 404
			elif 'value' in str(e): # Query incorrect and no timeseries data returned
				return 204

		return returned_dict
##################################################################################################
# End get_timeseries()
##################################################################################################
