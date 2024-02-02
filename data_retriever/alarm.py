__version__='0.0.3'
__author__=['Ioannis Tsakmakis']
__date_created__='2024-01-26'
__last_updated__='2024-02-02'

import smtplib, json
from email.message import EmailMessage

with open('credentials.json','r') as f:
    credentials = json.load(f)

with open(credentials['mail'],'r') as f:
    mail_credentials = json.load(f)

class EmailAlarm():

    def send_alarm(subject_text):

        msg= EmailMessage()

        my_address = mail_credentials['email']    #sender address

        app_generated_password = mail_credentials['app_pass']    # gmail generated password

        msg["Subject"] = subject_text   #email subject 

        msg["From"] = my_address      #sender address

        msg["To"] = "xylopodaros@yahoo.gr"     #reciver address

        msg.set_content("This is the body of the email")   #message body

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            
            smtp.login(my_address,app_generated_password)    #login gmail account
            
            print("sending mail")
            smtp.send_message(msg)   #send message 
            print("mail has sent")