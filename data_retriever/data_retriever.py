__version__='0.0.7'
__author__=['Ioannis Tsakmakis']
__date_created__='2024-01-26'
__last_updated__='2024-02-13'

from databases_utils import crud, influx
from data_retriever.external_apis import DavisApi, MetricaApi, addUPI
from datetime import datetime, timedelta
from data_retriever.alarm import EmailAlarm
from data_retriever.email_template import EmailTemplate
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

        if davis.validate_user()['status_code'] != 200:
            return print(davis.log_in())

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all davis stations
        stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"last_communication":station.Stations.last_communication,
                          "code":station.Stations.code, "status":station.Stations.status} for station in crud.Stations.get_by_brand(brand='davis')]

        stations_info_ds = davis.get_stations()
        if stations_info_ds['status_code'] == 200:
            station_ids = [station_id['station_id'] for station_id in stations_info_ds['stations']['stations']]
        else:
            return print(stations_info_ds)

        stations_info_filtered = [station_info for station_info in stations_info if int(station_info.get('code')) in station_ids]

        # Retrieving data
        for station in stations_info_filtered:
            latest_station_update = 0
            start = station['last_communication']
            end= datetime.now().timestamp()
            if end - start <= 3600:
                print({"status_code":200,"message":"Less than an hour passed since the last update. No new recrods"})
                continue
            print(f'\nStart: {datetime.fromtimestamp(start)}\n',f'\nEnd: {datetime.fromtimestamp(end)}\n')
            sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,
                             "code":sensor.MonitoredParameters.code,'measurement':sensor.MonitoredParameters.measurement} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
            data = davis.get_historic(station_id=station['code'],start=int(start),end=int(end))
            if data['status_code'] == 200:
                if data['station_data'].get('sensors'):
                    data = [data_set['data'] for data_set in data['station_data']['sensors'] if data_set.get('sensor_type')==30 or data_set.get('sensor_type')==4 or data_set.get('sensor_type')==31]
                    data_dict = {"date_time":[datetime.fromtimestamp(data_step['ts']) for data_step in data[0]],
                                    "air_temperature":[((data_step['temp_out']-32)*5/9) for data_step in data[0]],
                                    "relative_humidity":[data_step['hum_out'] for data_step in data[0]],
                                    "wind_speed":[data_step['wind_speed_avg'] for data_step in data[0]],
                                    "wind_speed_of_gust":[data_step['wind_speed_hi'] for data_step in data[0]],
                                    "wind_from_direction":[data_step['wind_dir_of_prevail'] for data_step in data[0]],
                                    "downwelling_shortwave_flux_in_air":[data_step['solar_rad_avg'] for data_step in data[0]],
                                    "lwe_thickness_of_precipitation_amount":[data_step['rainfall_mm'] for data_step in data[0]],
                                    "water_evapotranspiration_amount":[data_step['et'] for data_step in data[0]]}
                    latest_station_update = data_dict['date_time'][-1].timestamp()
                    print(f'\nLatest data update at: {datetime.fromtimestamp(latest_station_update)}\n')
                    crud.Stations.update_last_communication(station['id'],latest_station_update)
                    if station['status'] == 'offline':
                        crud.Stations.update_status(station_id=station['id'],current_status='online')
                        msg = EmailTemplate()
                        message = msg.resumed_reporting(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'), station_name=station['name']['en'],
                                                        station_code=station['code'], last_upadted=station['last_communication'])
                        subject = f'Station: {station['name']['en']} Resumed Reporting'
                        EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=subject, message=message)
                    for sensor in sensors_info:
                        input_data = {"date_time":data_dict['date_time'],"values":data_dict[sensor['measurement']]}
                        crud.MonitoredParameters.update_last_communication(sensor['id'], new_datetime=data_dict['date_time'][-1].timestamp())
                        flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=input_data)
                else:
                    message_part = {"status_code":data['code'],"message":f'Station {station["id"]}: {data}'}
                    message_text = f'Timestamp:{datetime.now()}\n{message_part}'
                    EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=f'Station: {station["name"]["en"]}', message=message_text)                 
            else:
                if station['status'] == 'online' and end - start >= 86400:
                    crud.Stations.update_status(station_id=station['id'], current_status='offline')
                    message = msg.stopped_reporting(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'),station_name=station['name']['en'],
                                                    station_code=station['code'],last_upadted=station['last_communication'])
                    subject = f'Station: {station['name']['en']} Stopped Reporting'
                    EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=subject, message=message)
                    continue
                else:
                    print(data)
                    continue
        print(f'\ncronjob - davis ended: {datetime.now()}\n')

    def get_data_historic(self, from_datetime, station_code=None):

        with open(self.credential_paths['davis'],'r') as f:
            credentials = json.load(f)

        # Initiate davis instance
        davis = DavisApi(user=self.username, credentials_davis=credentials)

        if davis.validate_user()['status_code'] != 200:
            return print(davis.log_in())

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all davis stations
        if station_code:
            stations_info = [{"id":station.id,"date_created":station.date_created,"last_communication":station.last_communication,"code":station.code,"name":station.name} for station in crud.Stations.get_by_code(code=station_code)]
        else:
            stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"last_communication":station.Stations.last_communication,"code":station.Stations.code,"name":station.Stations.name} for station in crud.Stations.get_by_brand(brand='davis')]
                 
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
                if data['status_code'] == 200:
                    if data['station_data'].get('sensors'):
                        data = [data_set['data'] for data_set in data['station_data']['sensors'] if data_set.get('sensor_type')==30 or data_set.get('sensor_type')==4 or data_set.get('sensor_type')==31]
                        data_dict = {"date_time":[datetime.fromtimestamp(data_step['ts']) for data_step in data[0]],
                                    "air_temperature":[((data_step['temp_out']-32)*5/9) for data_step in data[0]],
                                    "relative_humidity":[data_step['hum_out'] for data_step in data[0]],
                                    "wind_speed":[data_step['wind_speed_avg'] for data_step in data[0]],
                                    "wind_speed_of_gust":[data_step['wind_speed_hi'] for data_step in data[0]],
                                    "wind_from_direction":[data_step['wind_dir_of_prevail'] for data_step in data[0]],
                                    "downwelling_shortwave_flux_in_air":[data_step['solar_rad_avg'] for data_step in data[0]],
                                    "lwe_thickness_of_precipitation_amount":[data_step['rainfall_mm'] for data_step in data[0]],
                                    "water_evapotranspiration_amount":[data_step['et'] for data_step in data[0]]}
                        crud.Stations.update_last_communication(station['id'],data_dict['date_time'][-1].timestamp())
                        for sensor in sensors_info:
                            input_data = {"date_time":data_dict['date_time'],"values":data_dict[sensor['measurement']]}
                            flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=input_data)
                    else:
                        print({"status_code":data['code'],"message":f'Station {station["id"]}: {data['message']}'})
                else:
                    return print(data)

class MetricaDataRetriever():

    def __init__(self, username, local_path):
        self.username = username
        with open(f'{local_path}/credentials.json','r') as f:
            self.credential_paths = json.load(f)

    def get_station_status(self, station_code=None):

        # Initiate Metrica instance
        with open(self.credential_paths['metrica'],'r') as f:
            credentials = json.load(f)

        metrica = MetricaApi(user=self.username, credentials_metrica=credentials)

        try_log_in = metrica.log_in()
        if try_log_in['status_code'] != 200:
            return print(try_log_in)
        
        stations = metrica.post_stations(access_token=try_log_in['access_token'])

        if stations['status_code'] == 200:
            keys =  stations['stations'].keys()
            stations_info = [stations['stations'][i] for i in keys]
            if station_code:
                station_info = [station_info for station_info in stations_info if station_info.get('id') == station_code]
                station_status = [station_info[0].get('last_update')]
                return {"status_code":200,"station_status":station_status}
            else:
                station_status = [{station_info.get('id'):station_info.get('last_update')} for station_info in stations_info]
                return {"status_code":200,"station_status":station_status}

    def get_data(self, station_code=None):

        # Initiate Metrica instance
        with open(self.credential_paths['metrica'],'r') as f:
            credentials = json.load(f)

        metrica = MetricaApi(user=self.username, credentials_metrica=credentials)

        try_log_in = metrica.log_in()
        if try_log_in['status_code'] != 200:
            return print(try_log_in)

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all metrica stations
        if station_code:
            stations_info = [{"id":station.id,"date_created":station.date_created,"last_communication":station.last_communication,
                              "code":station.code,"name":station.name,"status":station.status} for station in crud.Stations.get_by_code(code=station_code)] 
        else:
            stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,
                              "last_communication":station.Stations.last_communication,"code":station.Stations.code,
                              "name":station.Stations.name,"status":station.Stations.status} for station in crud.Stations.get_by_brand(brand='metrica')] 
 
        # Get Metrica Access Token
        if not try_log_in.get('access_token'):
            return try_log_in
        else:
            # Retrieving data
            for station in stations_info:
                print(f'\nStation : {station['name']['en']}\n')
                start = station['last_communication']
                end= datetime.now().timestamp()
                latest_station_update = start
                if end - start <= 3600:
                    print({"status_code":200,"message":"Less than an hour passed since the last update. No new recrods"})
                    continue             
                sensors_info = [{"id":sensor.MonitoredParameters.id,"unit":sensor.MonitoredParameters.unit,"code":sensor.MonitoredParameters.code,
                                'measurement':sensor.MonitoredParameters.measurement,'status':sensor.MonitoredParameters.status,
                                'last_communication':sensor.MonitoredParameters.last_communication} for sensor in crud.MonitoredParameters.get_by_station_id(station_id=station['id'])]
                for sensor in sensors_info:
                    start = sensor['last_communication']
                    print(f'\nSensor: {sensor['measurement']}\n',
                        f'\nStart: {datetime.fromtimestamp(start)}\n',
                        f'\nEnd: {datetime.fromtimestamp(end)}\n')
                    data = metrica.post_data(access_token=try_log_in['access_token'],
                                            datefrom=datetime.fromtimestamp(start).strftime('%Y-%m-%d'),
                                            dateto=datetime.fromtimestamp(end).strftime('%Y-%m-%d'),
                                            timefrom=datetime.fromtimestamp(start).strftime('%H:%M'),
                                            timeto=datetime.fromtimestamp(end).strftime('%H:%M'),
                                            sensor_id=sensor['code'])
                    if data['status_code'] == 200:
                        if data['sensor_data'][0].get('values'):
                            data_dict = {"date_time":[datetime.strptime(f'{data_step["mdate"]}T{data_step["mtime"]}','%Y-%m-%dT%H:%M:%S') for data_step in data['sensor_data'][0]['values']],
                                        "values":[data_step['mvalue'] for data_step in data['sensor_data'][0]['values']]}
                            crud.MonitoredParameters.update_last_communication(monitored_parameter_id=sensor['id'], new_datetime=data_dict['date_time'][-1].timestamp())
                            if sensor['status'] == 'offline':
                                crud.MonitoredParameters.update_status(monitored_parameter_id=sensor['id'], current_status='online')
                            flux.write_point(measurement=sensor['measurement'],sensor_id=sensor['id'],unit=sensor['unit'], data=data_dict)
                            if data_dict['date_time'][-1].timestamp() > station['last_communication']:
                                latest_station_update = data_dict['date_time'][-1].timestamp()
                        else:
                            if sensor['status'] == 'online' and end-start>86400:
                                msg = EmailTemplate()
                                crud.MonitoredParameters.update_status(monitored_parameter_id=sensor['id'], current_status='offline')
                                message = msg.sensor_stopped_reporting(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                    station_name=station['name']['en'],
                                                                    sensor_id=sensor['id'],
                                                                    measurement=sensor['measurement'],
                                                                    last_communication=datetime.fromtimestamp(station['last_communication']).strftime('%Y-%m-%d %H:%M'))
                                subject = f'Station: {station['name']['en']} Sensor Stopped Reporting'
                                EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=subject, message=message)
                                continue
                            else:
                                print({"status_code":data['status_code'],"message":'No available data for the selected time period'})
                                continue
                    else:
                        msg = EmailTemplate()
                        if sensor['status'] == 'online' and end-start>86400:
                            crud.MonitoredParameters.update_status(monitored_parameter_id=sensor['id'], current_status='offline')
                            message = msg.sensor_stopped_reporting(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'),
                                                                   station_name=station['name']['en'],
                                                                   sensor_id=sensor['id'],
                                                                   measurement=sensor['measurement'],
                                                                   last_upadted=datetime.fromtimestamp(station['last_communication']).strftime('%Y-%m-%d %H:%M'))
                            subject = f'Station: {station['name']['en']} Sensor Stopped Reporting'
                            EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=subject, message=message)
                            continue
                        else:
                            print(data)
                            continue
                if latest_station_update > station['last_communication']:
                    crud.Stations.update_last_communication(station['id'], data_dict['date_time'][-1].timestamp())
                    if station['status'] == 'offline':
                        crud.Stations.update_status(station_id=station['id'], current_status='online')       
                elif station['status'] == 'online' and end-start>86400:
                    crud.Stations.update_status(station_id=station['id'], current_status='offline')

    def get_data_historic(self, from_datetime, station_code=None):

        # Initiate an inlux instance
        with open(self.credential_paths['metrica'],'r') as f:
            credentials = json.load(f)

        # Initiate Metrica instance
        metrica = MetricaApi(user=self.username, credentials_metrica=credentials)

        try_log_in = metrica.log_in()
        if try_log_in['status_code'] != 200:
            return print(try_log_in)

        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all metrica stations
        if station_code:
            stations_info = [{"id":station.id,"date_created":station.date_created,"last_communication":station.last_communication,
                              "code":station.code,"name":station.name,"status":station.status} for station in crud.Stations.get_by_code(code=station_code)] 
        else:
            stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,
                              "last_communication":station.Stations.last_communication,"code":station.Stations.code,
                              "name":station.Stations.name,"status":station.Stations.status} for station in crud.Stations.get_by_brand(brand='metrica')] 
 

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
                    crud.Stations.update_last_communication(station['id'],data_dict['date_time'][-1].timestamp())

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

        if adcon.log_in()['status_code'] != 200:
            return print(adcon.log_in())

        # Initiate an inlux instance
        flux = influx.DataManagement(bucket_name='sensors_meters', organization='envrio', conf_file=self.credential_paths['influx'])

        # Retrieve all metrica stations
        stations_info = [{"id":station.Stations.id,"date_created":station.Stations.date_created,"last_communication":station.Stations.last_communication,"code":station.Stations.code,"name":station.Stations.name} for station in crud.Stations.get_by_brand(brand='adcon')]

        # Retrieving data
        for station in stations_info:
            latest_timestamp = None
            start = station['last_communication']
            end= datetime.now().timestamp()
            if end - start > 86400:
                end = start + 86400
            elif end - start <=86400:
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
                    message_text = f'''
                    Timestamp:{datetime.now()}
                    Station: {station['name']['en']}
                    Measurement: {sensor['measurement']}
                    Sensor ID: {sensor['id']}
                    Message: {message_part}'''
                    EmailAlarm(mail_credentials=self.credential_paths['mail']).send_alarm(subject_text=f'Station: {station['name']['en']}', message=message_text)
                if latest_timestamp:
                    crud.Stations.update_last_communication(station['id'],data_dict['date_time'][-1].timestamp())
