__version__="1.2.3"
__authors__=['Ioannis Tsakmakis']
__date_created__='2023-11-27'
__last_updated__='2024-02-07'

import requests, xmltodict
import pandas as pd
from datetime import datetime, timedelta

class DavisApi():

    def __init__(self, user, credentials_davis):
        self.api_key = credentials_davis[user]["key"] if user in credentials_davis.keys() else None
        self.client_secret = credentials_davis[user]["secret"] if user in credentials_davis.keys() else None

    def validate_user(self):
        if self.api_key:
            return {"status_code":200,"message":"sucessful user validation"}
        else:
            return {"status_code":401,"message":"Invalid user"}           

    def get_stations(self):
        headers = {"X-Api-Secret":self.client_secret}
        stations = requests.get(url = "https://api.weatherlink.com/v2/stations",params = {"api-key": self.api_key},headers=headers)
        if stations.status_code == 200:
            return {"status_code":200,"stations":stations.json()}
        else:
            return {"status_code":stations.status_code,"message":stations.json()['message']}
    
    def get_station_metadata(self,station_id):
        headers = {"X-Api-Secret":self.client_secret}
        stations = requests.get(url = f"https://api.weatherlink.com/v2/stations/{station_id}",params = {"api-key": self.api_key},headers=headers)
        if stations.status_code == 200:
            return {"status_code":200,"stations":stations.json()}
        else:
            return {"status_code":stations.status_code,"message":stations.json()['message']}
    
    def get_sensor_catalog(self):
        headers = {"X-Api-Secret":self.client_secret}
        sensors = requests.get(url = "https://api.weatherlink.com/v2/sensor-catalog",params = {"api-key": self.api_key},headers=headers)
        if sensors.status_code == 200:
            return {"status_code":200,"sensors":sensors.json()}
        else:
            return {"status_code":sensors.status_code,"message":sensors.json()['message']}

    def get_sensors(self):
        headers = {"X-Api-Secret":self.client_secret}
        sensors = requests.get(url = "https://api.weatherlink.com/v2/sensors",params = {"api-key": self.api_key},headers=headers)
        if sensors.status_code == 200:
            return {"status_code":200,"sensors":sensors.json()}
        else:
            return {"status_code":sensors.status_code,"message":sensors.json()['message']}
    
    def get_current(self,station_id):
        headers = {"X-Api-Secret":self.client_secret}
        station_data = requests.get(url = f"https://api.weatherlink.com/v2/current/{station_id}",params = {"api-key": self.api_key},headers=headers)
        if station_data.status_code == 200:
            return {"status_code":200,"station_data":station_data.json()}
        else:
            return {"status_code":station_data.status_code,"message":station_data.json()['message']}
    
    def get_historic(self,station_id,start,end):
        headers = {"X-Api-Secret":self.client_secret}
        station_data = requests.get(url = f"https://api.weatherlink.com/v2/historic/{station_id}",
                                params = {"api-key": self.api_key,
                                          "start-timestamp": start,
                                          "end-timestamp": end},headers=headers)
        if station_data.status_code == 200:
            return {"status_code":200,"station_data":station_data.json()}
        else:
            return {"status_code":station_data.status_code,"message":station_data.json()['message']}
    
    def get_report(self,station_id):
        headers = {"X-Api-Secret":self.client_secret}
        station_report = requests.get(url = f"https://api.weatherlink.com/v2/report/et/{station_id}",params = {"api-key": self.api_key},headers=headers)
        if station_report.status_code == 200:
            return {"status_code":200,"station_repost":station_report.json()}
        else:
            return {"status_code":station_report.status_code,"message":station_report.json()['message']}

class MetricaApi():

    def __init__(self, user, credentials_metrica):
        self.base_url_metrica = credentials_metrica[user]['base_url'] if user in credentials_metrica.keys() else None
        self.username = credentials_metrica[user]['username'] if user in credentials_metrica.keys() else None
        self.password = credentials_metrica[user]['password'] if user in credentials_metrica.keys() else None

    def log_in(self):
        if self.base_url_metrica:
            try:
                response = requests.post(f'{self.base_url_metrica}/token', headers={"Content-Type": "application/json"},
                                        json={"email": self.username, "key": self.password})
                if response.status_code == 200:
                    return {"status_code":200,"message":"successfull authendication","access_token":response.json().get('token')}
                else:
                    return {"status_code": response.status_code}
            except Exception as e:
                return {"status_code":"","message":e}
        else:
            return {"status_code":401,"message":"Invalid user"}
        
    def post_stations(self,access_token):
        headers = {"content-type": "application/json",
                   "Authorization": f'Bearer {access_token}'}
        response = requests.post(url=f'{self.base_url_metrica}/stations', headers=headers)
        if response.status_code == 200:
            return {"status_code":200,"stations":response.json()['stations']}
        else:
            return {"status_code":response.status_code,"message":response.text}
    
    def post_sensors(self,access_token,station_id):
        headers = {"content-type": "application/json",
                   "Authorization": f'Bearer {access_token}'}
        json_body = {"station_id": station_id}
        response = requests.post(f'{self.base_url_metrica}/sensors',headers=headers, json=json_body)
        if response.status_code == 200:
            return {"status_code":200,"sensors":response.json()}
        else:
            return {"status_code":response.status_code,"message":response.text}

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
        if response.status_code == 200:
            return {"status_code":200,"sensor_data":response.json()['measurements']}
        else:
            return {"status_code":response.status_code,"message":response.text}

class addUPI():
    def __init__(self, user, credentials_adcon):
        self.headers = {'content-type': 'application/xml'}
        self.credentials = credentials_adcon
        self.user = user if user in credentials_adcon.keys() else None
        
    def log_in(self):
        if self.user:
            params = {'function': 'login', 'user': self.credentials[self.user]['username'], 'passwd': self.credentials[self.user]['password']}
            self.response = requests.get(self.credentials[self.user]['base_url'], params = params, headers = self.headers)
            if self.response.status_code == 200:
                self.session_id =  xmltodict.parse(self.response.text)["response"]["result"]["string"]
                self.url = self.credentials[self.user]['base_url']
                return {"status_code":200, "message":"successfull authendication"}
            else:
                return print({"status_code":self.response.status_code,"message":self.response.text})
        else:
            return {"status_code":401,"message":"Invalid user"}


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
                'date-format':'iso8601', 'date': start.strftime("%Y%m%dT%H:%M:%S"),
                'slots': int((end.timestamp()- start.timestamp())/step)} 
        data = requests.get(self.url, params = params, headers = self.headers)
        if data.status_code == 200:
            if xmltodict.parse(data.text)['response']['node'].get('v'):
                datadf = pd.DataFrame(xmltodict.parse(data.text)['response']['node']['v'])
                datadf.loc[0, '@t'] = datetime.strptime(datadf.iloc[0]['@t'], '%Y%m%dT%H:%M:%S')
                for i in range(1, len(datadf)):
                    datadf.loc[i, '@t'] = datadf.loc[i-1, '@t'] + timedelta(seconds=int(datadf.loc[i, '@t']))
                data_dict = {"date_time":datadf['@t'].values,"values":datadf['#text'].values}
                return data_dict
            else:
                return {"status_code":data.status_code,"error_code":xmltodict.parse(data.text)['response']['node']['error']['@code'],"message":f"{xmltodict.parse(data.text)['response']['node']['error']['@msg']}"}
        elif data.status_code == 400:
            return {"status_code":data.status_code,"error_code":xmltodict.parse(data.text)['response']['error']['@code'],"message":f"{xmltodict.parse(data.text)['response']['error']['@msg']}"}


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


