__version__='0.0.4'
__author__=['Ioannis Tsakmakis']
__date_created__='2024-01-26'
__last_updated__='2024-02-05'

from databases_utils import crud, influx
from data_retriever.external_apis import DavisApi, MetricaApi, addUPI
from datetime import datetime, timedelta
from data_retriever.alarm import EmailAlarm
import json

class DavisDataRetriever():

    def __init__(self, username, local_path):
        self.username = username
        with open(f'{local_path}/credentials.json','r') as f:
            self.credential_paths = json.load(f)

    def get_data(self):

        print(f'\ncronjob - davis started: {datetime.now()}\n')

        with open(self.credential_paths['davis'],'r') as f:
            credentials = json.load(f)

        # Initiate davis instance
        davis = DavisApi(user=self.username, credentials_davis=credentials)

        if davis.log_in()['status_code'] != 200:
            return print(davis.log_in())

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all adcon stations
        stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"latest_update":station.Stations.latest_update,"code":station.Stations.code} for station in crud.Stations.get_by_brand(brand='davis')]

        stations_info_ds = davis.get_stations()
        station_ids = [station_id['station_id'] for station_id in stations_info_ds['stations']]

        stations_info_filtered = [station_info for station_info in stations_info if int(station_info.get('code')) in station_ids]

        # Retrieving data
        for station in stations_info_filtered:
            start = station['latest_update']
            end= datetime.now().timestamp()
            if end - start > 86400:
                end = start + 86400
            elif end - start <=0:
                print({"error":"","message":"Less than an hour passed since the last update. No new recrods"})
                exit()
            print(f'\nStart: {datetime.fromtimestamp(start)}\n',f'\nEnd: {datetime.fromtimestamp(end)}\n')
            sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,"code":sensor.MonitoredParameters.code,'measurement':sensor.MonitoredParameters.measurement} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
            data = davis.get_historic(station_id=station['code'],start=int(start),end=int(end))
            if data.get('sensors'):
                data = [data_set['data'] for data_set in data['sensors'] if data_set.get('sensor_type')==30 or data_set.get('sensor_type')==4 or data_set.get('sensor_type')==31]
                data_dict = {"date_time":[datetime.fromtimestamp(data_step['ts']) for data_step in data[0]],
                                "air_temperature":[((data_step['temp_out']-32)*5/9) for data_step in data[0]],
                                "relative_humidity":[data_step['hum_out'] for data_step in data[0]],
                                "wind_speed":[data_step['wind_speed_avg'] for data_step in data[0]],
                                "wind_speed_of_gust":[data_step['wind_speed_hi'] for data_step in data[0]],
                                "wind_from_direction":[data_step['wind_dir_of_prevail'] for data_step in data[0]],
                                "downwelling_shortwave_flux_in_air":[data_step['solar_rad_avg'] for data_step in data[0]],
                                "lwe_thickness_of_precipitation_amount":[data_step['rainfall_mm'] for data_step in data[0]],
                                "water_evapotranspiration_amount":[data_step['et'] for data_step in data[0]]}
                print(f'Latest data update at {data_dict['date_time'][-1]}')
                crud.Stations.update_latest_update(station['id'],data_dict['date_time'][-1].timestamp())
                for sensor in sensors_info:
                    input_data = {"date_time":data_dict['date_time'],"values":data_dict[sensor['measurement']]}
                    flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=input_data)
            else:
                    message_part = {"code":data['code'],"message":f'Station {station["id"]}: {data['message']}'}
                    message_text = f'Timestamp:{datetime.now()}\n{message_part}'
                    EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=f'Station: {station["name"]["en"]}', message=message_text)

        print(f'\ncronjob - davis ended: {datetime.now()}\n')

    def get_data_historic(self, from_datetime, station_code=None):

        with open(self.credential_paths['davis'],'r') as f:
            credentials = json.load(f)

        # Initiate davis instance
        davis = DavisApi(user=self.username, credentials_davis=credentials)

        if davis.log_in().status_code != 200:
            return print(davis.log_in())

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all adcon stations
        if station_code:
            stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"latest_update":station.Stations.latest_update,"code":station.Stations.code,"name":station.Stations.name} for station in crud.Stations.get_by_brand(brand='davis')]
        else:
            stations_info = [{"id":station.id,"date_created":station.date_created,"latest_update":station.latest_update,"code":station.code,"name":station.name} for station in crud.Stations.get_by_code(code=station_code)]
                 
        stations_info_ds = davis.get_stations()
        station_ids = [station_id['station_id'] for station_id in stations_info_ds['stations']]

        stations_info_filtered = [station_info for station_info in stations_info if int(station_info.get('code')) in station_ids]
    
        # Retrieving data
        for station in stations_info_filtered:
            days = (datetime.now() - from_datetime).days
            for i in range(1,days):
                start = (from_datetime + timedelta(days=i-1))
                end= start + timedelta(days=1)
                print(f'\nStation: {station['name']}\n',f'\nStart: {start}\n',f'\nEnd: {end}\n')
                sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,"code":sensor.MonitoredParameters.code,'measurement':sensor.MonitoredParameters.measurement} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
                data = davis.get_historic(station_id=station['code'],start=int(start.timestamp()),end=int(end.timestamp()))
                if data.get('sensors'):
                    data = [data_set['data'] for data_set in data['sensors'] if data_set.get('sensor_type')==30 or data_set.get('sensor_type')==4 or data_set.get('sensor_type')==31]
                    data_dict = {"date_time":[datetime.fromtimestamp(data_step['ts']) for data_step in data[0]],
                                "air_temperature":[((data_step['temp_out']-32)*5/9) for data_step in data[0]],
                                "relative_humidity":[data_step['hum_out'] for data_step in data[0]],
                                "wind_speed":[data_step['wind_speed_avg'] for data_step in data[0]],
                                "wind_speed_of_gust":[data_step['wind_speed_hi'] for data_step in data[0]],
                                "wind_from_direction":[data_step['wind_dir_of_prevail'] for data_step in data[0]],
                                "downwelling_shortwave_flux_in_air":[data_step['solar_rad_avg'] for data_step in data[0]],
                                "lwe_thickness_of_precipitation_amount":[data_step['rainfall_mm'] for data_step in data[0]],
                                "water_evapotranspiration_amount":[data_step['et'] for data_step in data[0]]}
                    crud.Stations.update_latest_update(station['id'],data_dict['date_time'][-1].timestamp())
                    for sensor in sensors_info:
                        input_data = {"date_time":data_dict['date_time'],"values":data_dict[sensor['measurement']]}
                        flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=input_data)
                else:
                    print({"code":data['code'],"message":f'Station {station["id"]}: {data['message']}'})

class MetricaDataRetriever():

    def __init__(self, username, local_path):
        self.username = username
        with open(f'{local_path}/credentials.json','r') as f:
            self.credential_paths = json.load(f)

    def get_data(self):

        # Initiate Metrica instance
        with open(self.credential_paths['metrica'],'r') as f:
            credentials = json.load(f)

        metrica = MetricaApi(user=self.username, credentials_metrica=credentials)

        try_log_in = metrica.log_in()
        if try_log_in.get('status_code'):
            if try_log_in['status_code'] == 401:
                return print(try_log_in)

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all metrica stations
        stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"latest_update":station.Stations.latest_update,"code":station.Stations.code,"name":station.Stations.name} for station in crud.Stations.get_by_brand(brand='metrica')]

        # Get Metrica Access Token
        if not try_log_in.get('access_token'):
            [print(try_log_in)]
            return try_log_in
        else:
            # Retrieving data
            for station in stations_info:
                start = station['latest_update']
                end= datetime.now().timestamp()
                if end - start > 86400:
                    end = start + 86400
                elif end - start <=0:
                    return print({"error":"","message":"Less than an hour passed since the last update. No new recrods"})
        
                sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,"code":sensor.MonitoredParameters.code,
                                'measurement':sensor.MonitoredParameters.measurement} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
                for sensor in sensors_info:
                    print(f'\nStation : {station['name']['en']}\n',
                        f'\nSensor: {sensor['measurement']}\n',
                        f'\nStart: {datetime.fromtimestamp(start)}\n',
                        f'\nEnd: {datetime.fromtimestamp(end)}\n')
                    data = metrica.post_data(access_token=try_log_in['access_token'], datefrom=datetime.fromtimestamp(start).date().strftime('%Y-%m-%d'),
                                            dateto=datetime.fromtimestamp(end).date().strftime('%Y-%m-%d'), timefrom='00:00', timeto='23:55', sensor_id=sensor['code'])
                    if data['measurements'][0].get('values'):
                        data_dict = {"date_time":[datetime.strptime(f'{data_step["mdate"]}T{data_step["mtime"]}','%Y-%m-%dT%H:%M:%S') for data_step in data['measurements'][0]['values']],
                                    "values":[data_step['mvalue'] for data_step in data['measurements'][0]['values']]}
                        flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=data_dict)
                        if data_dict and len(data_dict)>0:
                            latest_timestamp = data_dict['date_time'][-1].timestamp()
                    else:
                        message_part = {"status_code":404, "message":"No available data for the selected period"}
                        message_text = f'Timestamp:{datetime.now()}\n{message_part}'
                        EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=f'Station: {station['name']['en']} - Sensor: {sensor['measurement']}', message=message_text)
                if latest_timestamp>0:
                    crud.Stations.update_latest_update(station['id'],data_dict['date_time'][-1].timestamp())

    def get_data_historic(self, from_datetime, station_code=None):

        # Initiate an inlux instance
        with open(self.credential_paths['metrica'],'r') as f:
            credentials = json.load(f)

        # Initiate Metrica instance
        metrica = MetricaApi(user=self.username, credentials_metrica=credentials)

        try_log_in = metrica.log_in()
        if try_log_in.get('status_code'):
            if try_log_in['status_code'] == 401:
                return print(try_log_in)

        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all metrica stations
        if station_code:
            stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"latest_update":station.Stations.latest_update,"code":station.Stations.code,"name":station.Stations.name} for station in crud.Stations.get_by_brand(brand='metrica')]
        else:
            stations_info = [{"id":station.id,"date_created":station.date_created,"latest_update":station.latest_update,"code":station.code,"name":station.name} for station in crud.Stations.get_by_code(code=station_code)]       

        # Get Metrica Access Token
        if not try_log_in.get('access_token'):
            [print(try_log_in)]
            return try_log_in
        else:
            for station in stations_info:
                days = (datetime.now() - from_datetime).days
                for i in range(1,days):
                    start = (from_datetime + timedelta(days=i-1))
                    end = start
                    sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,"code":sensor.MonitoredParameters.code,'measurement':sensor.MonitoredParameters.measurement} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
                    for sensor in sensors_info:
                        print(f'\nStation : {station['name']}\n',
                            f'\nSensor: {sensor['measurement']}\n',
                            f'\nStart: {start}\n',
                            f'\nEnd: {end}\n')
                        data = metrica.post_data(access_token=try_log_in['access_token'],datefrom=start.date().strftime('%Y-%m-%d'),
                                                dateto=end.date().strftime('%Y-%m-%d'),timefrom='00:00',timeto='23:55',sensor_id=sensor['code'])
                        if data['measurements'][0]['values']:
                            data_dict = {"date_time":[datetime.strptime(f'{data_step["mdate"]}T{data_step["mtime"]}','%Y-%m-%dT%H:%M:%S') for data_step in data['measurements'][0]['values']],
                                        "values":[data_step['mvalue'] for data_step in data['measurements'][0]['values']]}
                            flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=data_dict)
                            if data_dict and len(data_dict)>0:
                                latest_timestamp = data_dict['date_time'][-1].timestamp()
                        else:
                            print({"code":404,"message":"No available data for the selected period"})    
                if latest_timestamp>0:
                    crud.Stations.update_latest_update(station['id'],data_dict['date_time'][-1].timestamp())

class AdconDataRetriever():

    def __init__(self, username, local_path):
        self.username = username
        with open(f'{local_path}/credentials.json','r') as f:
            self.credential_paths = json.load(f)

    def get_data(self):

        # Initiate ADCON instance
        with open(self.credential_paths['adcon'],'r') as f:
            credentials = json.load(f)

        adcon = addUPI(user=self.username, credentials_adcon=credentials)

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all metrica stations
        stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"latest_update":station.Stations.latest_update,"code":station.Stations.code,"name":station.Stations.name} for station in crud.Stations.get_by_brand(brand='adcon')]

        # Retrieving data
        for station in stations_info:
            latest_timestamp = None
            start = station['latest_update']
            end= datetime.now().timestamp()
            if end - start > 86400:
                end = start + 86400
            elif end - start <=0:
                return print({"error":"","message":"Less than an hour passed since the last update. No new recrods"})

            sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,"code":sensor.MonitoredParameters.code,
                            'measurement':sensor.MonitoredParameters.measurement} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
            for sensor in sensors_info:
                print(f'\nStation : {station['name']['en']}\n',
                    f'\nSensor: {sensor['measurement']}\n',
                    f'\nStart: {datetime.fromtimestamp(start)}\n',
                    f'\nEnd: {datetime.fromtimestamp(end)}\n')
                data = adcon.get_data(sensor_id=sensor['id'], start=datetime.fromtimestamp(start), end=datetime.fromtimestamp(end))
                if data.get('values'):
                    data_dict = {"date_time":[datetime.strptime(f'{data_step["mdate"]}T{data_step["mtime"]}','%Y-%m-%dT%H:%M:%S') for data_step in data['measurements'][0]['values']],
                                "values":[data_step['mvalue'] for data_step in data['measurements'][0]['values']]}
                    flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=data_dict)
                    if data_dict and len(data_dict)>0:
                        latest_timestamp = data_dict['date_time'][-1].timestamp()
                else:
                    message_part = data
                    message_text = f'''Timestamp:{datetime.now()}
                                       Station: {station['name']['en']}
                                       Measurement: {sensor['measurement']}
                                       Sensor ID: {sensor['id']}
                                       Message: {message_part}'''
                    EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=f'Station: {station['name']['en']}', message=message_text)
                if latest_timestamp:
                    crud.Stations.update_latest_update(station['id'],data_dict['date_time'][-1].timestamp())
