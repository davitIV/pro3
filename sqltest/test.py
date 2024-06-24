from email.mime.application import MIMEApplication

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
import psycopg2
import schedule
import time
from datetime import datetime, time as dt_time
import uuid
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import csv
import zipfile

dotenv_path = os.path.join(os.path.dirname(__file__), 'txt.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

sender_email = "sendtestmsg@gmail.com"
password = os.getenv("retm mbtk jfrh ovil")
receiver_email = "mdaviti16@gmail.com"

def send_email(subject, body, attachment_path=None):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        if attachment_path:
            with open(attachment_path, 'rb') as attachment:
                part = MIMEApplication(attachment.read(), Name=os.path.basename(attachment_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
            msg.attach(part)

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        print(f"Email sent successfully to {receiver_email}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"SMTP authentication error: {e}")
    except Exception as e:
        print(f"Failed to send email: {e}")

class Database:
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT')
        }
        self.conn = None
        self.cur = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.db_params['dbname'],
                user=self.db_params['user'],
                password=self.db_params['password'],
                host=self.db_params['host'],
                port=self.db_params['port']
            )
            self.cur = self.conn.cursor()
            print("Connected to the database successfully")
        except Exception as error:
            print(f"Error connecting to the database: {error}")
            send_email("Error connecting to the database", str(error))
            raise

    def insert(self, AccountId, CustomerId, StatementDate, Principal, Interest, OverduePrincipal, OverdueInterest, SysDate, BatchId):
        try:
            insert_query = """
                INSERT INTO AccountStatement (AccountId, CustomerId, StatementDate, Principal, Interest, OverduePrincipal, OverdueInterest, SysDate, BatchId)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cur.execute(insert_query, (AccountId, CustomerId, StatementDate, Principal, Interest, OverduePrincipal, OverdueInterest, SysDate, BatchId))
            self.conn.commit()
            print(f"Inserted data: AccountId={AccountId}, CustomerId={CustomerId}")
        except Exception as error:
            self.conn.rollback()
            print(f"Error inserting data: {error}")
            send_email("Error inserting data", str(error))
            raise

def fetch_and_insert_data():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/dmsk/Downloads/gen-lang-client-0098895328-6c7ff4d52c9c.json', scope)

    try:
        client = gspread.authorize(creds)

        spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1PbAOQ7JmQ70Oy3ujKZ4qXIl0ozYKwgBdcATLrF1wkVg/edit?fbclid=IwZXh0bgNhZW0CMTAAAR2OjZFJfnSRcvQSoOqS3OQtMHmXETNbKnMWFqYbrChaOhiCrq2Sj8Ju1jg_aem_ZmFrZWR1bW15MTZieXRlcw&gid=0#gid=0'
        sheet = client.open_by_url(spreadsheet_url)

        worksheet = sheet.get_worksheet(0)

        data = worksheet.get_all_values()

        db = Database()
        db.connect()

        csv_dir = '/home/dmsk/Desktop/extra_F/'
        if not os.path.exists(csv_dir):
            os.makedirs(csv_dir)

        csv_file = os.path.join(csv_dir, 'fetched_data.csv')
        try:
            with open(csv_file, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(data)
        except Exception as e:
            print(f"Error creating CSV file: {e}")
            send_email("Error creating CSV file", str(e))
            return

        zip_file = os.path.join(csv_dir, 'fetched_data.zip')
        try:
            with zipfile.ZipFile(zip_file, 'w') as zipf:
                zipf.write(csv_file, os.path.basename(csv_file))
        except Exception as e:
            print(f"Error zipping CSV file: {e}")
            send_email("Error zipping CSV file", str(e))
            return

        print("Data has been fetched and saved to a CSV file, then zipped.")

        for idx, row in enumerate(data):
            if idx == 0:
                continue

            try:
                AccountId = int(row[0])
                CustomerId = int(row[1])
                StatementDate = row[2]
                Principal = float(row[3])
                Interest = float(row[4])
                OverduePrincipal = float(row[5])
                OverdueInterest = float(row[6])
                SysDate = row[7]
                BatchId = str(uuid.uuid4())

                db.insert(AccountId, CustomerId, StatementDate, Principal, Interest, OverduePrincipal, OverdueInterest, SysDate, BatchId)
            except IndexError as e:
                print(f"Error processing row {idx + 1}: list index out of range. Row data: {row}")
                send_email(f"Error processing row {idx + 1}", f"list index out of range. Row data: {row}")
            except ValueError as e:
                print(f"Error processing row {idx + 1}: invalid data type. Row data: {row}")
                send_email(f"Error processing row {idx + 1}", f"invalid data type. Row data: {row}")
            except Exception as e:
                print(f"Unexpected error processing row {idx + 1}: {e}. Row data: {row}")
                send_email(f"Unexpected error processing row {idx + 1}", str(e))


    except Exception as e:
        print(f"Error: {e}")
        send_email("Error fetching and inserting data", str(e))

def is_working_hours():
    now = datetime.now().time()
    start_time = dt_time(9, 0)
    end_time = dt_time(23, 59)
    return start_time <= now <= end_time

def job():
    if is_working_hours():
        fetch_and_insert_data()
    else:
        print("It's not working hours. Skipping job execution.")

if __name__ == "__main__":
    if is_working_hours():
        fetch_and_insert_data()
    else:
        print("It's not working hours at startup.")

    schedule.every(5).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(5)
