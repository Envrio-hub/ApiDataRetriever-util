__version__='0.0.4'
__author__=['Ioannis Tsakmakis']
__date_created__='2024-01-26'
__last_updated__='2024-02-05'

import smtplib, json
from email.message import EmailMessage

class EmailAlarm():

    def __init__(self, mail_credentials):
        self.mail_credentials = mail_credentials

    def send_alarm(self, subject_text, message, recipients = ['xylopodaros@yahoo.gr','nikolaoskokkos@gmail.com']):

        with open(self.mail_credentials,'r') as f:
            credentials = json.load(f)

        msg= EmailMessage()

        my_address = credentials[6]['mail']    #sender address

        app_generated_password = credentials[6]['app_pass']    # gmail generated password

        msg["Subject"] = subject_text   #email subject 

        msg["From"] = my_address      #sender address

        msg["To"] =  recipients    #reciver address

        msg.set_content(message)   #message body

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            
            smtp.login(my_address,app_generated_password)    #login gmail account
            
            print("sending mail")
            smtp.send_message(msg)   #send message 
            print("mail has sent")