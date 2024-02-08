__version__='0.0.3'
__author__=['Ioannis Tsakmakis']
__date_created__='2024-02-07'
__last_updated__='2024-02-08'

class EmailTemplate():

    @staticmethod 
    def stopped_reporting(timestamp, station_name, station_code, last_upadted):
        html = f'''
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 20px;
                        }}
                        #header {{
                            display: flex;
                            justify-content: space-between;
                            align-items: flex-start;
                            margin-bottom: 20px;
                        }}
                        </style>
                </head>
                <body>
                    <div id="header">
                        <img id="image" src="https://envrio.org/documentation/logo.png" alt="Image Alt Text" style="max-width: 80px; max-height: 80px;">
                        <div style="font-size: 15px; margin-top: 5px;">Created: {timestamp}</div>
                    </div>

                    <table style="width: 35%; border-collapse: collapse; margin-top: 20px;">
                        <tr>
                            <th colspan="2" style="background-color: #4FC660; padding: 10px; text-align: left; border: 1px solid black;">Station Stopped Reporting</th>
                        </tr>
                        <tr>
                            <td style="padding: 10px; text-align: left; border: 1px solid black;">Station</td>
                            <td style="padding: 10px; text-align: left; border: 1px solid black;">{station_name}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; text-align: left; border: 1px solid black;">Station Code</td>
                            <td style="padding: 10px; text-align: left; border: 1px solid black;">{station_code}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px; text-align: left; border: 1px solid black;">Last Updated</td>
                            <td style="padding: 10px; text-align: left; border: 1px solid black;">{last_upadted}</td>
                        </tr>
                    </table>
                </body>
                </html>
            '''
        return html

    @staticmethod
    def empty_values(timestamp, station_name, station_code, measurement, sensor_id):
        html = f'''
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 20px;
                        }}
                        #header {{
                            display: flex;
                            justify-content: space-between;
                            align-items: flex-start;
                            margin-bottom: 20px;
                        }}
                        </style>
            </head>
            <body>
                <div id="header">
                        <img id="image" src="https://envrio.org/documentation/logo.png" alt="Image Alt Text" style="max-width: 80px; max-height: 80px;">
                        <div style="font-size: 15px; margin-top: 5px;">Created: {timestamp}</div>
                </div>

                <table style="width: 35%; border-collapse: collapse; margin-top: 20px;">
                    <tr rowspan="2">
                        <th style="background-color: #4FC660; padding: 10px; text-align: left; border: 1px solid black;">Sensor Returned Empty Values</th>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Station</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{station_name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Station Code</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{station_code}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Measurement</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{measurement}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Sensor ID</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{sensor_id}</td>
                    </tr>
                </table>
            </body>
            </html>
            '''
        return html
    
    @staticmethod
    def data_error(timestamp, station_name, station_code, measurement, sensor_id, error_message):
        html = f'''
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 20px;
                        }}
                        #header {{
                            display: flex;
                            justify-content: space-between;
                            align-items: flex-start;
                            margin-bottom: 20px;
                        }}
                        </style>
            </head>
            <body>
                <div id="header">
                        <img id="image" src="https://envrio.org/documentation/logo.png" alt="Image Alt Text" style="max-width: 80px; max-height: 80px;">
                        <div style="font-size: 15px; margin-top: 5px;">Created: {timestamp}</div>
                </div>

                <table style="width: 35%; border-collapse: collapse; margin-top: 20px;">
                    <tr rowspan="2">
                        <th style="background-color: #4FC660; padding: 10px; text-align: left; border: 1px solid black;">Sensor Returned Empty Values</th>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Station</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{station_name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Station Code</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{station_code}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Measurement</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{measurement}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Sensor ID</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{sensor_id}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">Error Message</td>
                        <td style="padding: 10px; text-align: left; border: 1px solid black;">{error_message}</td>
                    </tr>
                </table>
            </body>
            </html>
            '''
        return html