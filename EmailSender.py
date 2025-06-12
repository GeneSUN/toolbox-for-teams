import smtplib 
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.utils import COMMASPACE, formatdate 


class EmailSender: 
    def __init__(self, server='vzsmtp.verizon.com'): 
        self.server = server 
    def send(self, text, files=None,
                        send_from ="sassupport@verizon.com", 
                        send_to =["zhe.sun@verizonwireless.com"], 
                        subject = "test", 
                        cc =[] ): 
        assert isinstance(send_to, list) 
        assert isinstance(cc, list) 

        # Create the message 
        msg = MIMEMultipart() 
        msg['From'] = send_from 
        msg['To'] = COMMASPACE.join(send_to) 
        msg['Cc'] = COMMASPACE.join(cc) 
        msg['Date'] = formatdate(localtime=True) 
        msg['Subject'] = subject 

        # Set the text with inline CSS for dark black color  
        text_with_style = f'<div style="color: #200000;">{text}</div>'  
        msg.attach(MIMEText(text_with_style, 'html')) 
 
        # Send the email 
        smtp = smtplib.SMTP(self.server) 
        smtp.sendmail(send_from, send_to + cc, msg.as_string()) 
        smtp.close() 
        """
        # Usage 
        import sys 
        sys.path.append('/usr/apps/vmas/scripts/ZS/OOP_dir') 
        from EmailSender import EmailSender
        mail_sender = EmailSender() 
        mail_sender.send(send_from="sender@example.com", 
                        send_to=["recipient@example.com"], 
                        subject="Test Subject", 
                        cc=["cc@example.com"], 
                        text="This is a test email.") 
        """