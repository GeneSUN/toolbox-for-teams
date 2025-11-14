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
                                password='12345', options="-c statement_timeout=60min") 
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
        
        ftp = FTP("nj51vmasda1v.nss.vzwnet.com")
        ftp.login(user="vmasndl", passwd="Summer2018@")    
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

    def __init__(self, init_payload,miss_payload, API_ENDPOINT="http://njbbvmaspd6.nss.vzwnet.com:8082/druid/v2/sql", *args, **kwargs): 

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


def send_mail(send_from, send_to, subject, cc,df_list, files=None, server='vzsmtp.verizon.com' ):
    assert isinstance(send_to, list)
    assert isinstance(cc, list)
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Cc'] = COMMASPACE.join(cc)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    # Convert DataFrames to HTML tables and add <br> tags 
    html_content = '<br><br>'.join(df.to_html() for df in df_list) 
    msg.attach(MIMEText(html_content, 'html')) 
    
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to + cc, msg.as_string())
    smtp.close

def get_hdfs_missing(hdfs_location):
    hdfs_list = list( hdfs_location.keys() )

    Hdfs_dict = {}
    for Daily_Task_name in hdfs_list:
        try:
            Hdfs_dict[Daily_Task_name] =ClassDailyCheckHdfs(
                                                    hdfs_location[Daily_Task_name]["hdfs_host"], 
                                                    hdfs_location[Daily_Task_name]["hdfs_port"], 
                                                    hdfs_location[Daily_Task_name]["hdfs_folder_path"], 
                                                    hdfs_location[Daily_Task_name]["file_name_pattern"],
                                                    Daily_Task_name, 
                                                    int(expected_delay[Daily_Task_name])
                                                    )
        except:
            print(f"no hdfs of {Daily_Task_name}")

    data_hdfs = [vars( ins ) for ins in list( Hdfs_dict.values() ) ]
    df_hdfs = pd.DataFrame(data_hdfs)[["name","hdfs_latest_date","hdfs_delayed_days","hdfs_miss_days"]]
    return df_hdfs

def get_druid_missing(druid_hdfs_location, payload, miss_date_payload):
    druid_dict = {}
    #for daily_task in payload.keys():
    for daily_task in druid_hdfs_location.keys():
        try:
            if daily_task == "cpe_final_score_v3":
                druid_dict[daily_task] = ClassDailyCheckDruid( 
                                init_payload = payload[daily_task], 
                                miss_payload = miss_date_payload[daily_task],
                                API_ENDPOINT = "http://njbbepapa6.nss.vzwnet.com:8082/druid/v2/sql",
                                hdfs_host = druid_hdfs_location[daily_task]["hdfs_host"], 
                                hdfs_port = druid_hdfs_location[daily_task]["hdfs_port"], 
                                hdfs_folder_path = druid_hdfs_location[daily_task]["hdfs_folder_path"], 
                                file_name_pattern = druid_hdfs_location[daily_task]["file_name_pattern"],
                                name = daily_task, 
                                expect_delay = int(expected_delay[daily_task]) )
            else:
                druid_dict[daily_task] = ClassDailyCheckDruid( 
                                                init_payload = payload[daily_task], 
                                                miss_payload = miss_date_payload[daily_task],
                                                hdfs_host = druid_hdfs_location[daily_task]["hdfs_host"], 
                                                hdfs_port = druid_hdfs_location[daily_task]["hdfs_port"], 
                                                hdfs_folder_path = druid_hdfs_location[daily_task]["hdfs_folder_path"], 
                                                file_name_pattern = druid_hdfs_location[daily_task]["file_name_pattern"],
                                                name = daily_task, 
                                                expect_delay = int(expected_delay[daily_task]) )
        except Exception as e:
            print(e)
    data_druid = [vars( ins ) for ins in list( druid_dict.values() ) ]
    df_druid = pd.DataFrame(data_druid)[["name","druid_latest_date","druid_delayed_days","druid_miss_days","hdfs_latest_date","hdfs_delayed_days","hdfs_miss_days"]]
    return df_druid

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Daily SNA data availability checking')\
                        .config("spark.ui.port","24055")\
                        .getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")
    """
    for hdfs file, update expected_delay and hdfs_location
    for hdfs and druid file, update expected_delay and druid_hdfs_location
    """

    # 1. expected_delay is global variable used in Class
    expected_delay = {  'vue_anomaly': '2', 
                    'Anomaly_user_rtt_v4': '2', 
                    'calldrop_hourly_v1': '0', 
                    'calldrop_daily_v1': '1', 
                    'calldrop_enodeb_daily_v1': '1', 
                    'enodeb_alarm': '1', 
                    'impacted_usr_v1': '1', 
                    'six_dropcall_in_Six_day': '1', 
                    'activation_reports_v5': '7', 
                    "activation_report_vmb": "7",
                    'Historical_RTT': '1', 
                    'mobility_score': '1', 
                    '5g_home_score': '2', 
                    '5g_home_churn_score': '1',
                    'speed_check': '2',
                    "anchor_4G_daily": '1',
                    'snap_data_pre_enb': '2', 
                    'snap_data_post_enb': '2', 
                    'snap_data_pre_sector': '2', 
                    'snap_data_post_sector': '2', 
                    'snap_data_pre_carrier': '2', 
                    'snap_data_post_carrier': '2', 
                    "wifi_score_v3":'1',
                    "cpe_final_score_v3":2
                    }
    time_range = time_window = 20

    hdfs_location = { 

                'Historical_RTT': { 
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/stp_rtt_druid_customer_summary_daily', 
                    "file_name_pattern": r'(\d{4})(\d{2})(\d{2})' 
                },
                'mobility_score': { 
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/kovvuve/stp_rtt_customer_mobility_scores_daily', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                }, 
                "speed_check": {
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/wy/speed_check/with_additional_info/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "anchor_4G_daily": {
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/fwa/anchor_4G_daily/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "activation_report_vmb": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/rohit/activation_report/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
        }
    df_hdfs = get_hdfs_missing(hdfs_location)

    druid_hdfs_location = { 
                'vue_anomaly': { 
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/save/the/score_anomaly/Archive/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'Anomaly_user_rtt_v4': { 
                    "hdfs_host": 'njbbepapa1.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/anomaly_user_rtt/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'calldrop_hourly_v1': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/iris_enb_user_hourly/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'calldrop_daily_v1': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/user_daily', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'calldrop_enodeb_daily_v1': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/enb_daily', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'enodeb_alarm': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/enodeb_alarm/archive', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'impacted_usr_v1': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/impacted_usr', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'six_dropcall_in_Six_day': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/6_dropcall_in_6_day/archive', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'activation_reports_v5': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/Charlotte/activation_report', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'snap_data_pre_enb': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/enodeb/Daily_KPI_14_days_pre_Event', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' ,
                }, 
                'snap_data_post_enb': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Post_tickets_Feature_Date', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}',
                },
                'snap_data_pre_sector': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Sector/Daily_KPI_14_days_pre_Event', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}',
                },
                'snap_data_post_sector': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Sector/Event_Enodeb_Post_tickets_Feature_Date', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}',
                },
                'snap_data_pre_carrier': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Carrier/Daily_KPI_14_days_pre_Event', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}',
                },
                'snap_data_post_carrier': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Carrier/Event_Enodeb_Post_tickets_Feature_Date', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}',
                },
                "wifi_score_v3": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/wifi_score_v3/archive', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}',
                },
                "cpe_final_score_v3": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/5g_homeScore/final_score/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
        }

    tables = list(druid_hdfs_location.keys())
    # check if missing for today ------------------------------------------------------------
    payload_base_query = """ 
        SELECT SUBSTR(CAST(__time AS VARCHAR), 1, 10) as current_date_val, 
            COUNT(*) as row_count  
        FROM {table_name}  
        GROUP BY SUBSTR(CAST(__time AS VARCHAR), 1, 10)  
        ORDER BY current_date_val DESC  
        LIMIT 1 
    """ 
    payload = {table: {"query": payload_base_query.format(table_name=table)} for table in tables} 
    # check missing days ------------------------------------------------------------------------
    query_template = """ 
        SELECT DISTINCT SUBSTR(CAST(__time AS VARCHAR), 1, 10) as existed_date  
        FROM {table_name}  
        ORDER BY existed_date DESC  
        LIMIT {time_window} 
    """ 
    miss_date_payload = {table: {"query": query_template.format(table_name=table, time_window=time_range)} for table in tables} 

    df_druid = get_druid_missing(druid_hdfs_location, payload, miss_date_payload)

# Postgre -----------------------------------------------------------------------------------------    

    def postgre_fun():
        init_query = "select date from fiveg_home_score order by date desc limit 1"

        Postgre_dict = {}
        Postgre_dict["5g_home_score"] = ClassDailyCheckPostgre(init_query , "5g_home_score", int(expected_delay["5g_home_score"]))

        d = (date.today() - timedelta( int(expected_delay["5g_home_score"]) ))
        d = datetime.strftime( d, "%Y-%m-%d")
        query = f"""
                    WITH RECURSIVE DateRange AS ( 
                    SELECT DATE '{d}' AS expect_date, 
                        DATE '{d}' - INTERVAL '20 DAY' AS start_date 
                    UNION ALL 
                    SELECT expect_date, 
                        start_date + INTERVAL '1 DAY' 
                    FROM DateRange 
                    WHERE start_date + INTERVAL '1 DAY' <= expect_date 
                    ) 

                    SELECT start_date 
                    FROM DateRange 
                    LEFT JOIN fiveg_home_score ON start_date = date 
                    WHERE date IS NULL
                """

    #r = Postgre_dict["5g_home_score"].carto_query_dataframe_zs(query = query)

    #data_postgre = [vars( ins ) for ins in list( Postgre_dict.values() ) ]
    #df_postgre = pd.DataFrame(data_postgre)[["name","postgre_latest_date","postgre_delayed_days"]]
    #df_postgre["postgre_missed_days"] = " ".join([datetime.strftime( d, "%Y-%m-%d") for d in r["start_date"].tolist()])
    df_postgre = pd.DataFrame()
# FTP -----------------------------------------------------------------------------------------
    """
    ftp_job = {
                "5g_home_score":    {
                                        "ftp_dir": "/5G_HOME_SCORE/", 
                                        "pattern": "5G_Home_Score_{}.csv"
                                        },
                "5g_home_churn_score":{
                                        "ftp_dir": "/5G_HOME_SCORE/5G_Home_Churn_Score/archive/", 
                                        "pattern": "fiveg_home_churn_score_{}_1.csv"
                                        }
            }


    fpt_dict = {}
    fpt_dict["5g_home_churn_score"] = ClassDailyCheckFTP( ftp_dir = ftp_job["5g_home_churn_score"]["ftp_dir"], 
                                                            pattern = ftp_job["5g_home_churn_score"]["pattern"],
                                                            name = "5g_home_churn_score",
                                                            expect_delay = int(expected_delay["5g_home_churn_score"]),
                                                            )

    data_ftp = [vars( ins ) for ins in list( fpt_dict.values() ) ]

    df_ftp = pd.DataFrame(data_ftp)[["name","ftp_latest_date","ftp_delayed_days", "ftp_miss_days"]]
    """
#-----------------------------------------------------------------------------------------
    try:
        df_list = [ df_druid, df_hdfs ]
    except:
        df_list = [ df_druid, df_hdfs ]
    #send_mail(  'sassupport@verizon.com', ['zhe.sun@verizonwireless.com'], f"Daily SNA data availability checking Q{(datetime.now().month - 1)//3 +1} {datetime.now().year}",[], df_list,files=None, server='vzsmtp.verizon.com' );sys.exit()
    #"""
    
    send_mail(  'sassupport@verizon.com', 
            ['zhe.sun@verizonwireless.com',
                'sue.su@verizonwireless.com',
                'yu.wan1@verizonwireless.com',
                'wenyuan.lu@verizonwireless.com',
                'venkateswarlu.chitte@verizonwireless.com',
                'venkata.kovvuri@verizonwireless.com',
                'rohit.kovvuri@verizonwireless.com',
                'shivakanth.puligilla1@verizon.com',
                "david.you@verizonwireless.com",
                "chanakya.nakka@verizonwireless.com"], 
            f"Daily SNA data availability checking Q{(datetime.now().month - 1)//3 +1} {datetime.now().year}",
            [], 
            df_list, 
            files=None, 
            server='vzsmtp.verizon.com' )
