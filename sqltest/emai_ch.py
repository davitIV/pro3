
##es ubralod testia emails vagzavi tu ara

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sender_email = "sendtestmsg@gmail.com"
password = "retm mbtk jfrh ovil"
receiver_email = "mdaviti16@gmail.com"

def send_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)

            server.sendmail(sender_email, receiver_email, msg.as_string())

        print(f"Email sent successfully to {receiver_email}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP authentication error: {e}")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    subject = "Test Email"
    body = "This is a test email sent from Python."

    send_email(subject, body)


