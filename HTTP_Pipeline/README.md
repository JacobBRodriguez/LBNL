# Skyspark HTTP Pipeline
Project aims to collect time series data streams from disperse source systems through a local Python server acting as a middleman for HTTPS requests to third-party APIs. Data is collected, transformed, and stored in a centralized data warehouse.

## Getting Started
The primary source file to run is local_server.py.
It contains a main() function that launches the server.

### Prerequisites
Code runs with Python 3.6 and above.

In addition to the core class files found in this repo, there are two other files that must be in the same directory when running the server. The first is a credentials.json file that is used to access data from the Google Calendar API (guide found [here](https://developers.google.com/calendar/quickstart/python)). The second is a YAML authentication file (see example below).

#### Example YAML file
```
# -----------------------------
# Authentication for various APIs used in local server
# -----------------------------

ElasticSearch:
  username: 'NERSC_USER'
  password: 'nersc_password'

ALC:
  username: 'ALC_USER'
  password: 'alc_password'
  ```
  
  ### Installing
  Make sure all core files and additional files are in the same directory.
  
  #### Library imports
  The only installs are library dependencies via Pip
  ```
  pip install pyyaml
  pip install --upgrade google-api-python-client oauth2client
  pip install suds-jurko
  ```
  
  ## Deployment
  Project is deployed by running local_server.py through cmd or terminal.
  Before deploying, confirm that localhost port 9000 is open. Server runs under port.
  ### Running via CMD or Terminal
  ```
  python local_server.py
  ```
  You should see an output similar to this if server is running properly:
  ```
  Thu Feb 21 13:01:39 2019 Server Starts - localhost:9000
  ```
  
  ### Terminating server
  To terminate server and close port 9000, use Ctrl-C in command window.
  If correctly terminated, the output should be the following:
  ```
  Keyboard interrupt. Server terminated
  ```
  
  ## Authors
  * **Jacob Rodriguez** - *Server/API connector creation* - [Jacob Rodriguez](https://github.com/JacobBRodriguez)
  
  ## Acknowledgments
  
  * Chris Weyandt - initial idea for server and helped with successful deployment
