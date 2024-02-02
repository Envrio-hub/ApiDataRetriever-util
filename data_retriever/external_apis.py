__version__="1.1.0"
__authors__=['Ioannis Tsakmakis']
__date_created__='2023-11-27'
__last_updated__='2024-02-02'

import json, requests, xmltodict
import pandas as pd
from datetime import datetime, timedelta

with open("credentials.json","r") as f:
    credentials = json.loads(f.read())

with open(credentials['davis'],'r') as r:
    credentials_davis = json.loads(r.read())

with open(credentials['metrica'],'r') as r:
    credentials_metrica = json.loads(r.read())

class DavisApi():

    def __init__(self,user):
        self.api_key = credentials_davis[user]["key"]
        self.client_secret = credentials_davis[user]["secret"]

    def get_stations(self):
        headers = {"X-Api-Secret":self.client_secret}
        stations = requests.get(url = "https://api.weatherlink.com/v2/stations",params = {"api-key": self.api_key},headers=headers)
        return stations.json()
    
    def get_station_metadata(self,station_id):
        headers = {"X-Api-Secret":self.client_secret}
        stations = requests.get(url = f"https://api.weatherlink.com/v2/stations/{station_id}",params = {"api-key": self.api_key},headers=headers)
        return stations.json()
    
    def get_sensor_catalog(self):
        headers = {"X-Api-Secret":self.client_secret}
        stations = requests.get(url = "https://api.weatherlink.com/v2/sensor-catalog",params = {"api-key": self.api_key},headers=headers)
        return stations.json()  

    def get_sensors(self):
        headers = {"X-Api-Secret":self.client_secret}
        stations = requests.get(url = "https://api.weatherlink.com/v2/sensors",params = {"api-key": self.api_key},headers=headers)
        return stations.json()   
    
    def get_current(self,station_id):
        headers = {"X-Api-Secret":self.client_secret}
        station_data = requests.get(url = f"https://api.weatherlink.com/v2/current/{station_id}",params = {"api-key": self.api_key},headers=headers)
        return station_data.json()
    
    def get_historic(self,station_id,start,end):
        headers = {"X-Api-Secret":self.client_secret}
        station_data = requests.get(url = f"https://api.weatherlink.com/v2/historic/{station_id}",
                                params = {"api-key": self.api_key,
                                          "start-timestamp": start,
                                          "end-timestamp": end},headers=headers)
        return station_data.json()
    
    def get_report(self,station_id):
        headers = {"X-Api-Secret":self.client_secret}
        station_report = requests.get(url = f"https://api.weatherlink.com/v2/report/et/{station_id}",params = {"api-key": self.api_key},headers=headers)
        return station_report.json()

class MetricaApi():

    def __init__(self, user):
        self.base_url_metrica = credentials_metrica[user]['base_url']
        self.username = credentials_metrica[user]['username']
        self.password = credentials_metrica[user]['password']
    
    def log_in(self):
        try:
            response = requests.post(f'{self.base_url_metrica}/token', headers={"Content-Type": "application/json"},
                                     json={"email": self.username, "key": self.password})

            if response.status_code == 200:
                return {"message":"successfull authendication","access_token":response.json().get('token')}
            else:
                return {f'\nRequest failed with status code {response.status_code}\n'}
        except Exception as e:
            return {"status":"","message":e}
        
    def post_stations(self,access_token):
        headers = {"content-type": "application/json",
                   "Authorization": f'Bearer {access_token}'}

        response = requests.post(url=f'{self.base_url_metrica}/stations', headers=headers)

        return response.json()
    
    def post_sensors(self,access_token,station_id):
        headers = {"content-type": "application/json",
                   "Authorization": f'Bearer {access_token}'}
        
        json_body = {"station_id": station_id}

        response = requests.post(f'{self.base_url_metrica}/sensors',headers=headers, json=json_body)
        
        return response.json()

    def post_data(self,access_token,datefrom,dateto,timefrom,timeto,sensor_id):
        headers = {'content-type': 'application/json',
                'Authorization': f'Bearer {access_token}'}

        json_body = {
                    'datefrom': datefrom,
                    'dateto': dateto,
                    'timefrom': timefrom,
                    'timeto': timeto,
                    'sensor': [sensor_id]}

        response = requests.post(f'{self.base_url_metrica}/measurements', headers=headers, json=json_body)

        return response.json()

class addUPI():

    def __init__(self,url = 'http://scient.static.otenet.gr:82/addUPI',user = "dpth",password = "dpth"):
        self.s = requests.Session()
        self.headers = {'content-type': 'application/xml'}
        params = {'function': 'login', 'user': user, 'passwd': password}
        response = self.s.get(url, params = params, headers = self.headers)
        if response.status_code == 200:
            self.session_id =  xmltodict.parse(response.text)["response"]["result"]["string"]
            self.url = url
        else:
            raise Exception(f"Fail to connect to the server. status code {response.status_code}")

    def get_config(self,node_id=None,depth=None):
        params = {'function': 'getconfig', 'session-id': self.session_id, 'id': node_id,
                    'depth': depth,'date-format': 'iso8601'}
        data = requests.get(self.url, params = params, headers = self.headers)
        return xmltodict.parse(data.text)
    
    def get_attrib(self,node_id,attr = None):
        if attr == None:
            params = {'function': 'getattrib', 'session-id': self.session_id, 'id': node_id,
                    'date-format': 'iso8601'}
            data = requests.get(self.url, params = params, headers = self.headers)
            datadf = pd.read_xml(data.text.encode( 'iso-8859-1').decode('utf-8'))
            return datadf
        else:
            params = {'function': 'getattrib', 'session-id': self.session_id, 'id': node_id,
                    'attrib': attr,'date-format': 'iso8601'}
            datadf = pd.read_xml(data.text.encode( 'iso-8859-1').decode('utf-8'))
            return datadf

    def get_data(self, sensor_id, start=(datetime.now()- timedelta(hours = 3)), end=datetime.now(), step=1800):
        params = {'function':'getdata', 'session-id': self.session_id, 'id': sensor_id,
                'date-format':'iso8601', 'date': datetime.fromtimestamp(start).strftime("%Y%m%dT%H:%M:%S"),
                'slots': int((end- start)/step)} 
        data = self.s.get(self.url, params = params, headers = self.headers)
        if xmltodict.parse(data.text)['response']['node'].get('v'):
            datadf = pd.DataFrame(xmltodict.parse(data.text)['response']['node']['v'])
            datadf.loc[0, '@t'] = datetime.strptime(datadf.iloc[0]['@t'], '%Y%m%dT%H:%M:%S')
            for i in range(1, len(datadf)):
                datadf.loc[i, '@t'] = datadf.loc[i-1, '@t'] + timedelta(seconds=int(datadf.loc[i, '@t']))
            data_dict = {"date_time":datadf['@t'].values,"values":datadf['#text'].values}
            return data_dict
        else:
            return {"code":xmltodict.parse(data.text)['response']['node']['error']['@code'],"message":f"Sensor - {sensor_id}: xmltodict.parse(data.text)['response']['node']['error']['@msg']"}

class xFarmApi():

    def __init__(self,url = 'https://api.xfarm.ag/api/public/v1/auth/login',user = "ioannis@xfarm.ag",password = "Grecia_2019"):
        self.s = requests.Session()
        params = {"username": user, "password": password}
        response = self.s.post(url=url, json=params)
        if response.status_code == 200:
            self.access_token =  response.json()['access_token']
            self.base_url = 'https://api.xfarm.ag/api/private/v1/xsense/'
        else:
            raise Exception(f"Fail to connect to the server. status code {response.status_code}")
    
    def get_device_list(self):
        headers = {'Authorization': f'Bearer {self.access_token}'}
        device_list = requests.get(url = 'https://api.xfarm.ag/api/private/v1/xsense/devices', headers=headers)
        if device_list.status_code == 200:
            return device_list.json()
        else:
            response = xmltodict.parse(device_list.text)
            return {"status":response['Map']['error']}
    
    def get_device_data(self, station_code, start, end):
        headers = {'Authorization': f'Bearer {self.access_token}'}
        periodTo = (end - start).days
        params = {"deviceID":station_code, "from":start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), "periodTo": f'{periodTo}d'}
        device_data = requests.get(url = 'https://api.xfarm.ag/api/private/v1/xsense/deviceData', headers=headers, params=params)
        if device_data.status_code == 200:
            return device_data.json()
        else:
            response = xmltodict.parse(device_data.text)
            return {"status":response['Map']['error']}   


