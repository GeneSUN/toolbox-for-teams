from pyspark.sql import SparkSession
import argparse
import requests
import pandas as pd
import json
from datetime import timedelta, date, datetime
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ftplib import FTP
from hdfs import InsecureClient
import os
import re

"""
Daily File/Data Availability Monitoring Framework
-------------------------------------------------
This script defines reusable checker classes for:
✔ PostgreSQL table freshness
✔ FTP file arrivals
✔ HDFS daily file arrivals
✔ Druid task execution completion

Each checker:
- Computes latest available date
- Measures data delay in days
- Identifies missing days in a window
- Can be tied into alerting/email logic

Class Hierarchy Overview
ClassDailyCheckBase   ←  (stores name, expected date)
│
├── ClassDailyCheckPostgre
│       - queries PostgreSQL
│       - finds latest date
│       - computes delay
│
├── ClassDailyCheckFTP
│       - checks FTP directory
│       - identifies latest file
│       - finds missing days
│
├── ClassDailyCheckHdfs
│       - checks HDFS file system
│       - extracts date from filename
│       - finds missing days
│
└── ClassDailyCheckDruid (inherits ClassDailyCheckHdfs)
        - calls Druid API
        - determines latest ingested date
        - finds missing dates

"""

# ---------------------------------------------------------
# Shared Base Class
# ---------------------------------------------------------
class ClassDailyCheckBase:
    """
    Base class that stores:
    - task name
    - expected delay (how many days behind is acceptable)
    - the derived expected date
    - global "neglect_days" to ignore known outages
    """
    neglect_days = ["2025-01-21", "2025-01-20", "2025-01-19",
                    "2025-01-18", "2025-01-17", "2025-01-16", "2025-01-15"]

    def __init__(self, name, expect_delay):
        self.name = name
        self.expect_delay = expect_delay
        self.expect_date = date.today() - timedelta(days=self.expect_delay)


# ---------------------------------------------------------
# PostgreSQL Checker
# ---------------------------------------------------------
class ClassDailyCheckPostgre(ClassDailyCheckBase):
    """
    Checks latest date in a PostgreSQL table.
    """

    def __init__(self, init_query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = init_query
        self.postgre_latest_date = None
        self.postgre_delayed_days = None
        self.set_postgre_date()

    def set_postgre_date(self):
        df = self.carto_query_dataframe_zs()
        self.postgre_latest_date = df["date"][0]
        self.postgre_delayed_days = (self.expect_date - self.postgre_latest_date).days

    def carto_query_dataframe_zs(self, query=None, pa=None,
                                 dbName="cartodb_dev_user_bc97a77f-a8e2-47d1-8a7a-280e12c50c03_db",
                                 serverHost="nj51vmaspa9"):
        if query is None:
            query = self.query

        conn = psycopg2.connect(
            database=dbName, user='postgres', host=serverHost, port="5432",
            password='', options="-c statement_timeout=60min"
        )
        cur = conn.cursor()
        cur.execute("Set statement_timeout='60min'")
        df = pd.read_sql_query(query, conn, params=pa)
        cur.close()
        conn.close()
        return df


# ---------------------------------------------------------
# FTP Checker
# ---------------------------------------------------------
class ClassDailyCheckFTP(ClassDailyCheckBase):
    """
    Checks latest delivered file in FTP folder and identifies missing days.
    """

    def __init__(self, ftp_dir, pattern, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ftp_dir = ftp_dir
        self.pattern = pattern
        self.ftp_latest_date = None
        self.ftp_delayed_days = None
        self.set_ftp_date()
        self.ftp_miss_days = self.find_ftp_empty()

    def ftpdocument_exists(self, ftp_dir=None, pattern=None, dt=None):
        ftp_dir = ftp_dir or self.ftp_dir
        pattern = pattern or self.pattern

        ftp = FTP("")
        ftp.login(user="", passwd="")
        ftp.cwd(ftp_dir)
        files = ftp.nlst()
        ftp.close()

        file_name = pattern.format(dt)
        return file_name in files

    def set_ftp_date(self):
        latest = self.find_latest_document()
        self.ftp_latest_date = latest
        self.ftp_delayed_days = (
            self.expect_date - datetime.strptime(latest, "%Y-%m-%d").date()
        ).days

    def find_latest_document(self, time_window=None):
        time_window = time_window or time_range
        cur = datetime.now().date()

        while cur >= datetime.now().date() - timedelta(days=time_window):
            if self.ftpdocument_exists(dt=cur.strftime("%Y-%m-%d")):
                return cur.strftime("%Y-%m-%d")
            if self.ftpdocument_exists(dt=cur.strftime("%Y%m%d")):
                return cur.strftime("%Y-%m-%d")
            cur -= timedelta(days=1)
        return None

    def find_ftp_empty(self, expect_date=None, time_window=None):
        time_window = time_window or time_range
        expect_date = expect_date or self.expect_date

        cur = expect_date
        end = cur - timedelta(days=time_window)
        missing = []

        while cur >= end:
            if not (
                self.ftpdocument_exists(dt=cur.strftime("%Y-%m-%d"))
                or self.ftpdocument_exists(dt=cur.strftime("%Y%m%d"))
            ):
                missing.append(cur)
            cur -= timedelta(days=1)

        return missing


# ---------------------------------------------------------
# HDFS Checker
# ---------------------------------------------------------
class ClassDailyCheckHdfs(ClassDailyCheckBase):
    """
    Scans a HDFS folder, extracts dates from file names,
    finds latest available date and missing days.
    """

    def __init__(self, hdfs_host, hdfs_port, hdfs_folder_path, file_name_pattern,
                 *args, **kwargs):
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
        client = InsecureClient(f"http://{self.hdfs_host}:{self.hdfs_port}")
        files = client.list(self.hdfs_folder_path)

        pattern = re.compile(self.file_name_pattern)
        latest_file = None
        latest_date = None

        for file_name in files:
            match = pattern.search(file_name)
            if match:
                ds = match.group(0)
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})':
                    ds = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
                file_date = datetime.strptime(ds, "%Y-%m-%d")

                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file_name

        self.hdfs_latest_file = latest_file
        self.hdfs_latest_date = latest_date

    def set_hdfs_date(self):
        self.hdfs_delayed_days = (
            self.expect_date - self.hdfs_latest_date.date()
        ).days

    def find_hdfs_file(self, search_date=None):
        client = InsecureClient(f"http://{self.hdfs_host}:{self.hdfs_port}")
        files = client.list(self.hdfs_folder_path)
        pattern = re.compile(self.file_name_pattern)

        for file_name in files:
            match = pattern.search(file_name)
            if match:
                ds = match.group(0)
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})':
                    ds = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
                if datetime.strptime(ds, "%Y-%m-%d").date() == search_date:
                    return True
        return False

    def find_all_empty(self, expect_date=None, time_window=None):
        time_window = time_window or time_range
        expect_date = expect_date or self.expect_date

        cur = expect_date
        end = cur - timedelta(days=time_window)
        missing = []

        while cur > end:
            cur -= timedelta(days=1)
            if not self.find_hdfs_file(cur):
                missing.append(str(cur))

        # special filtering
        if self.name.startswith("snap_data"):
            dt = [datetime.strptime(d, "%Y-%m-%d") for d in missing]
            filtered = [
                d.strftime("%Y-%m-%d")
                for d in dt
                if d.weekday() < 5 and d.strftime("%Y-%m-%d") not in self.neglect_days
            ]
            return filtered

        return missing


# ---------------------------------------------------------
# Druid Checker (inherits HDFS)
# ---------------------------------------------------------
class ClassDailyCheckDruid(ClassDailyCheckHdfs):
    """
    Uses Druid API to check:
    - Latest available date
    - Missing days in a time window
    """

    def __init__(self, init_payload, miss_payload, API_ENDPOINT, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.payload = init_payload
        self.miss_payload = miss_payload
        self.API_ENDPOINT = API_ENDPOINT

        self.druid_latest_date = None
        self.druid_delayed_days = None

        self.set_druid_date()
        self.druid_miss_days = self.find_miss_druid()

    def query_druid(self, payload=None):
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        return requests.post(self.API_ENDPOINT, data=json.dumps(payload), headers=headers)

    def set_druid_date(self):
        r = self.query_druid(self.payload).json()
        self.druid_latest_date = datetime.strptime(r[0]['current_date_val'], "%Y-%m-%d").date()
        self.druid_delayed_days = (self.expect_date - self.druid_latest_date).days

    def find_miss_druid(self, p=None, task_name=None, time_window=None):
        p = p or self.miss_payload
        task_name = task_name or self.name
        time_window = time_window or time_range

        cur = datetime.now().date() - timedelta(days=int(expected_delay[task_name]))
        target_dates = [(cur - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(time_window)]

        existed = [d["existed_date"] for d in self.query_druid(p).json()]
        missing = [d for d in target_dates if d not in existed]

        if task_name.startswith("snap_data_pre"):
            dt = [datetime.strptime(d, "%Y-%m-%d") for d in missing]
            filtered = [
                d.strftime("%Y-%m-%d")
                for d in dt
                if d.weekday() < 5 and d.strftime("%Y-%m-%d") not in self.neglect_days
            ]
            return filtered

        return missing


# ---------------------------------------------------------
# Example main usage
# ---------------------------------------------------------
if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("HourlyScoreProcessing")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # PostgreSQL Example
    sql_query = "SELECT MAX(date) as date FROM your_data_table"
    check = ClassDailyCheckPostgre(
        init_query=sql_query,
        name="Data Ingestion Check",
        expect_delay=1
    )

    print("Job:", check.name)
    print("Expected:", check.expect_date)
    print("Latest DB Date:", check.postgre_latest_date)
    print("DB Delay (days):", check.postgre_delayed_days)


    # HDFS Example
    hdfs_check = ClassDailyCheckHdfs(
        hdfs_host="host",
        hdfs_port=50070,
        hdfs_folder_path="/data/etl/daily_table/",
        file_name_pattern=r'(\d{4}-\d{2}-\d{2})',
        name="Daily HDFS Table Check",
        expect_delay=1
    )

    print("Job:", hdfs_check.name)
    print("Expected:", hdfs_check.expect_date)
    print("Latest HDFS:", hdfs_check.hdfs_latest_date)
    print("Delay:", hdfs_check.hdfs_delayed_days)
