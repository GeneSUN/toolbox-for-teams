from pyspark.sql import SparkSession 
import argparse 
import requests 
import pandas as pd 
import json
from datetime import timedelta, date , datetime
import argparse 
import requests 
import pandas as pd 
import json
import psycopg2
import sys
import smtplib
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from ftplib import FTP
import pandas as pd

from hdfs import InsecureClient 
import os
import re 

"""
Utility script for generating daily reports and emailing them to designated recipients. 
Easily customizable for various scheduled checks, automated reporting, and notification needs within a team workflow.
"""

class ClassDailyCheckBase:
    global neglect_days
    neglect_days = ["2025-01-21", "2025-01-20", "2025-01-19", "2025-01-18", "2025-01-17", "2025-01-16", "2025-01-15"]
    def __init__(self, name, expect_delay):
        self.name = name
        self.expect_delay = expect_delay
        self.expect_date = date.today() - timedelta( days = self.expect_delay )

class ClassDailyCheckPostgre(ClassDailyCheckBase):
    
    def __init__(self, init_query, *args, **kwargs): 
        super().__init__(*args, **kwargs)
        self.query = init_query 
        self.postgre_latest_date = None 
        self.postgre_delayed_days = None 
        self.set_postgre_date()
        #self.carto_query_dataframe_zs()
    def set_postgre_date(self): 
        self.postgre_latest_date = self.carto_query_dataframe_zs()["date"][0] 
        self.postgre_delayed_days = (self.expect_date - self.postgre_latest_date).days 


    def carto_query_dataframe_zs(self, query = None, pa=None, dbName="cartodb_dev_user_bc97a77f-a8e2-47d1-8a7a-280e12c50c03_db", serverHost="nj51vmaspa9"): 
        if query is None:
            query = self.query
            
        conn = psycopg2.connect(database=dbName, user='postgres', host=serverHost, port="5432", 
                                password='', options="-c statement_timeout=60min") 
        cur = conn.cursor() 
        cur.execute("Set statement_timeout to '60min'") 
        df = pd.read_sql_query(query, conn, params=pa) 
        cur.close() 
        conn.close() 

        return df
        
        
class ClassDailyCheckFTP(ClassDailyCheckBase):
    
    def __init__(self,ftp_dir, pattern ,*args, **kwargs): 
        super().__init__(*args, **kwargs)
        self.ftp_dir = ftp_dir
        self.pattern = pattern
        self.ftp_latest_date = None 
        self.ftp_delayed_days = None 
        self.set_ftp_date()
        self.ftp_miss_days = self.find_ftp_empty()
    def set_ftp_date(self): 
        self.ftp_latest_date = self.find_latest_document()
        self.ftp_delayed_days = (self.expect_date -  datetime.strptime( self.ftp_latest_date, "%Y-%m-%d").date() ).days

    def ftpdocument_exists(self, ftp_dir = None, pattern = None, dt = None):
        
        if ftp_dir is None:
            ftp_dir = self.ftp_dir

        if pattern is None:
            pattern = self.pattern
        
        ftp = FTP("")
        ftp.login(user="", passwd="")    
        ftp.cwd(ftp_dir)
        res = ftp.nlst()
        ftp.close()
        file = pattern.format(dt)

        if file in res:
            return True
        return False
    
    def find_latest_document(self, time_window = None): 
        if time_window is None:
            time_window = time_range # this is a backdoor global variable, i forget to define as an argument of this class, my bad.
            # but i am too lazy to fix this, just do not delete  backdoor global variable in main
        current_date = datetime.now().date()
        
        while current_date >= datetime.now().date() - timedelta(days = time_window):

            if self.ftpdocument_exists(dt = current_date.strftime("%Y-%m-%d")): 
                return current_date.strftime("%Y-%m-%d")
            elif self.ftpdocument_exists(dt = current_date.strftime("%Y%m%d")):
                return current_date.strftime("%Y-%m-%d")
            
            current_date -= timedelta(days=1)
            
        return None
    
    def find_ftp_empty(self, expect_date = None, time_window = None):
        if time_window is None:
            time_window = time_range
        if expect_date == None:
            expect_date = self.expect_date
        
        start_date = expect_date
        end_date = start_date - timedelta(days = time_window)
        cur_date = start_date
        miss_days = []
        
        while cur_date >= end_date:

            if self.ftpdocument_exists(dt = cur_date.strftime("%Y-%m-%d")): 
                pass
            elif self.ftpdocument_exists(dt = cur_date.strftime("%Y%m%d")):
                pass
            else:
                miss_days.append(cur_date)
            
            cur_date -= timedelta(days=1)
            
        return miss_days


class ClassDailyCheckHdfs(ClassDailyCheckBase): 

    def __init__(self,  hdfs_host, hdfs_port, hdfs_folder_path, file_name_pattern, *args, **kwargs): 
        super().__init__(*args, **kwargs) 
        self.hdfs_host = hdfs_host 
        self.hdfs_port = hdfs_port 
        self.hdfs_folder_path = hdfs_folder_path 
        self.file_name_pattern = file_name_pattern 
        self.hdfs_latest_file = None 
        self.hdfs_latest_date = None 
        self.hdfs_delayed_days = None 
        self.find_latest_hdfs_file() 
        self.set_hdfs_date() 
        self.hdfs_miss_days = self.find_all_empty()
        
    def find_latest_hdfs_file(self): 
        client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}') 
        files = client.list(self.hdfs_folder_path) 
        latest_file = None 
        latest_date = None 
        date_pattern = re.compile(self.file_name_pattern) 

        for file_name in files: 
            match = date_pattern.search(file_name) 
            if match: 
                date_str = match.group(0) 
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})': 
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" 
                folder_name_date = datetime.strptime(date_str, '%Y-%m-%d') 
                if latest_date is None or folder_name_date > latest_date: 
                    latest_file = file_name 
                    latest_date = folder_name_date
                    
        if latest_file: 
            self.hdfs_latest_file = latest_file 
            self.hdfs_latest_date = latest_date 
        else: 
            self.hdfs_latest_file = None 
            self.hdfs_latest_date = None
    
    def set_hdfs_date(self): 
        self.hdfs_delayed_days = (self.expect_date -self.hdfs_latest_date.date()).days 
    
    def find_hdfs_file(self, search_date = None): 
        
        client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}') 
        files = client.list(self.hdfs_folder_path) 
        date_pattern = re.compile(self.file_name_pattern) 
    
        for file_name in files: 
            match = date_pattern.search(file_name) 
            if match: 
                date_str = match.group(0) 
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})': 
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" 
    
                folder_name_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if folder_name_date == search_date: 
                    return True

    def find_all_empty(self, expect_date = None, time_window = None):
        if time_window is None:
            time_window = time_range
        if expect_date == None:
            expect_date = self.expect_date
        start_date = expect_date

        end_date = start_date - timedelta(days = time_window)
        cur_date = start_date
        miss_days = []
        while cur_date >= end_date:
            cur_date -= timedelta(days = 1)

            if self.find_hdfs_file(cur_date):
                #print(f"{cur_date} existed")
                pass
            else:
                miss_days.append(f"{cur_date}")
        
        if self.name[:9] == 'snap_data':
            date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in miss_days] 
            filtered_dates = [date.strftime("%Y-%m-%d") for date in date_objects if date.weekday() < 5] 
            filtered_dates = [d for d in filtered_dates if d not in neglect_days]
            return filtered_dates
        else:
            return miss_days
        return miss_days

class ClassDailyCheckDruid(ClassDailyCheckHdfs): 

    def __init__(self, init_payload,miss_payload, API_ENDPOINT, *args, **kwargs): 

        super().__init__(*args, **kwargs)
        self.payload = init_payload
        self.druid_latest_date = None
        self.druid_delayed_days = None
        self.API_ENDPOINT = API_ENDPOINT
        self.set_druid_date()
        self.miss_payload = miss_payload
        
        self.druid_miss_days = self.find_miss_druid()
        
    def set_druid_date(self):
        r = self.query_druid(self.payload).json()
        self.druid_latest_date = datetime.strptime(r[0]['current_date_val'], "%Y-%m-%d").date() 
        self.druid_delayed_days = (self.expect_date -self.druid_latest_date).days
         
    def query_druid(self, payload = None, ): 

        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}   
        payload = json.dumps(payload) 
        response = requests.post(self.API_ENDPOINT, data=payload, headers=headers) 
        return response
    
    def find_miss_druid(self, p = None, task_name = None , time_window = None):
        if p is None:
            p = self.miss_payload
        if task_name is None:
            task_name = self.name
        if time_window is None:
            time_window = time_range
        current_date = datetime.now().date()  - timedelta(days= int(expected_delay[task_name]) )
        #current_date = datetime.now().date()
        target_date_list = [ datetime.strftime( (current_date - timedelta(days=i)), "%Y-%m-%d") for i in range(time_window)]
        
        
        dict_list = self.query_druid( p ).json()
        exist_date_list = [item["existed_date"] for item in dict_list]
    
        missing_dates = [date for date in target_date_list if date not in exist_date_list ]

        
        if task_name[:13] == 'snap_data_pre':
            date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in missing_dates] 
            filtered_dates = [date.strftime("%Y-%m-%d") for date in date_objects if date.weekday() < 5] 
            filtered_dates = [d for d in filtered_dates if d not in neglect_days ]
            return filtered_dates
        else:
            return missing_dates


if __name__ == "__main__":

    spark = SparkSession.builder\
            .appName('HourlyScoreProcessing')\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


    """
    example of using ClassDailyCheckPostgre
    """

    # 1. Define your SQL query for the table you want to check
    sql_query = "SELECT MAX(date) as date FROM your_data_table"

    # 2. Create a PostgreSQL daily check object
    check = ClassDailyCheckPostgre(
        init_query=sql_query,
        name="Data Ingestion Check",
        expect_delay=1,  # e.g., expect yesterday's data to be present today
    )

    # 3. Access key results
    print("Job name:", check.name)
    print("Expected date:", check.expect_date)
    print("Latest date in DB:", check.postgre_latest_date)
    print("Data delay (days):", check.postgre_delayed_days)

    # 4. You can add logic to send email alerts if delay is too high, etc.
    if check.postgre_delayed_days > 2:
        print("Alert: Data is delayed!")
        # check.send_email_alert()   # (If such a method exists in your script)

    """
    Example: HDFS Data Check
    """
    # Define the HDFS path to check for latest file
    hdfs_path = "/data/etl/daily_table/"

    # Create the HDFS check object
    hdfs_check = ClassDailyCheckHDFS(
        hdfs_path=hdfs_path,
        name="Daily HDFS Table Check",
        expect_delay=1,
    )

    # Access results
    print("Job name:", hdfs_check.name)
    print("Expected date:", hdfs_check.expect_date)
    print("Latest date in HDFS:", hdfs_check.hdfs_latest_date)
    print("Data delay (days):", hdfs_check.hdfs_delayed_days)

    if hdfs_check.hdfs_delayed_days > 2:
        print("Alert: HDFS data is delayed!")