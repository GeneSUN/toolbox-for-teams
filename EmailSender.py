import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import COMMASPACE, formatdate
from email import encoders
import os

class MailSender:
    """
    MailSender simplifies the process of sending emails with or without attachments.

    Features:
    - Send HTML or plain text emails
    - Attach files (e.g., CSV, TXT, PDF) to emails
    - CC support

    Example usage:
        mail_sender = MailSender()
        mail_sender.send_html(
            send_from="sender@example.com",
            send_to=["recipient@example.com"],
            subject="Test Subject",
            cc=["cc@example.com"],
            text="This is a <b>test</b> email."
        )
        mail_sender.send_with_attachment(
            send_from="sender@example.com",
            send_to=["recipient@example.com"],
            subject="Report Attached",
            cc=[],
            text="Please see the attached CSV file.",
            files=["data/report.csv"]
        )
    """

    def __init__(self, server='vzsmtp.verizon.com'):
        self.server = server

    def send_html(self, text, files=None,
                  send_from="sassupport@verizon.com",
                  send_to=None,
                  subject="test",
                  cc=None):
        """
        Send an HTML email with optional file attachments.

        Parameters:
        - text (str): HTML content of the email.
        - files (list[str], optional): List of file paths to attach.
        - send_from (str): Sender email address.
        - send_to (list[str]): List of recipient email addresses.
        - subject (str): Email subject.
        - cc (list[str], optional): List of CC addresses.
        """
        if send_to is None:
            send_to = []
        if cc is None:
            cc = []
        assert isinstance(send_to, list)
        assert isinstance(cc, list)

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to)
        msg['Cc'] = COMMASPACE.join(cc)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        # Inline CSS for dark black color
        text_with_style = f'<div style="color: #200000;">{text}</div>'
        msg.attach(MIMEText(text_with_style, 'html'))

        # Attach files, if provided
        if files:
            for path in files:
                if not os.path.isfile(path):
                    continue  # Skip if the file does not exist
                with open(path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="{os.path.basename(path)}"',
                    )
                    msg.attach(part)

        # Send the email
        with smtplib.SMTP(self.server) as smtp:
            smtp.sendmail(send_from, send_to + cc, msg.as_string())

    def send_plain(self, text, files=None,
                   send_from="sassupport@verizon.com",
                   send_to=None,
                   subject="test",
                   cc=None):
        """
        Send a plain text email with optional file attachments.

        Parameters are the same as send_html.
        """
        if send_to is None:
            send_to = []
        if cc is None:
            cc = []
        assert isinstance(send_to, list)
        assert isinstance(cc, list)

        msg = MIMEMultipart()
        msg['From'] = send_from
        msg['To'] = COMMASPACE.join(send_to)
        msg['Cc'] = COMMASPACE.join(cc)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject

        msg.attach(MIMEText(text, 'plain'))

        # Attach files, if provided
        if files:
            for path in files:
                if not os.path.isfile(path):
                    continue
                with open(path, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="{os.path.basename(path)}"',
                    )
                    msg.attach(part)

        # Send the email
        with smtplib.SMTP(self.server) as smtp:
            smtp.sendmail(send_from, send_to + cc, msg.as_string())

    def send(self, *args, **kwargs):
        """
        Default to sending HTML emails for backward compatibility.
        """
        self.send_html(*args, **kwargs)
