from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from pyspark.sql import functions as F 
from pyspark.sql.functions import (from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
from datetime import datetime, timedelta, date 
from dateutil.parser import parse
import tempfile 
import argparse 
import time 
import requests 
import pyodbc 
import numpy as np 
import pandas as pd 
import functools
import json
from operator import add 

from functools import reduce 
from operator import add 
from sklearn.neighbors import BallTree
from math import radians, cos, sin, asin, sqrt


try: 
    from StringIO import StringIO  # for Python 2 
except ImportError: 
    from io import StringIO, BytesIO  # for Python 3

def adjust_date(date_str, del_days, date_format = '%Y-%m-%d', direction='backward'): 
    """ 
    Adjust a given date string by adding or subtracting a specified number of days. 
    Args: 
        date_str (str): The input date string to adjust. 
        del_days (int): The number of days to add (if direction is 'forward') or subtract (if direction is 'backward'). 
        date_format (str): The format of the input date string, e.g., 'YYYY-MM-DD'. 
        direction (str, optional): The direction of adjustment. 'forward' adds days, 'backward' subtracts days. 
            Defaults to 'backward'. 
    Returns: 
        str: The adjusted date string in the specified date format. 
    Raises: 
        ValueError: If the direction argument is not 'forward' or 'backward'. 
    Example: 
        adjust_date('2023-09-19', 5, '%Y-%m-%d', 'forward') returns '2023-09-24'. 
    """ 

    date = datetime.strptime(date_str, date_format) 
    if direction.lower() == 'forward': 
        adjusted_date = date + timedelta(days=del_days) 
    elif direction.lower() == 'backward': 
        adjusted_date = date - timedelta(days=del_days) 
    else: 
        raise ValueError("Invalid direction argument. Use 'forward' or 'backward'.") 
    return adjusted_date.strftime(date_format)

def generate_date_range(current_date = datetime.now().date(), days = 14): 
    # Get the current date 
    if current_date != datetime.now().date():
        current_date = datetime.strptime(current_date, "%Y-%m-%d")
    # Calculate the start date (14 days before the current date) 
    start_date = current_date - timedelta(days=days) 

    # Initialize an empty list to store the date strings 
    date_list = [] 

    # Generate the date range in descending order and format them as strings 
    while start_date <= current_date: 
        date_string = start_date.strftime("%Y-%m-%d") 
        date_list.insert(0, date_string)  # Insert at the beginning to make it descending 
        start_date += timedelta(days=1) 

    return date_list

def pad_six_char(df, column_name="ENODEB"):
    """ 
    Ensures that all values in the specified column have exactly 6 characters. 
    If a value has 5 characters, it adds a '0' in front of it to make it 6 characters. 
    :param df: The DataFrame. 
    :param column_name: The name of the column to process. 
    :return: The DataFrame with the specified column updated. 
    """ 
    df = df.withColumn( 
        column_name, 
        when(length(col(column_name)) < 6, lpad( df[column_name], 6,'0' ) ).otherwise(col(column_name)) 
    ) 
    return df

def union_df_for_date_range(date_range): 
    """ 
    Read and union multiple CSV files for the specified date range and apply filters. 
 
    Args: 
        date_range (list): A list of dates in the string format 'YYYY-MM-DD' for which data needs to be processed. 
        schema (list, optional): A list of column names representing the schema of the DataFrame. Default is column_list.
 
    Returns: 
        DataFrame: A PySpark DataFrame containing the union of data from the specified date range, 
                   filtered and processed according to the given schema. 
    """ 
    column_list = ['DAY', 'MMEPOOL', 'REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB', 'FSM_LTE_CtxDrop%', 'FSM_VOLTE_ERAB_QCI1_Drop%', 'FSM_LTE_CtxSF%', 'S1U_SIP_SC_CallDrop%', 'FSM_LTE_AvgRRCUsers', 'FSM_LTE_DLRLCMBytes', 'FSM_LTE_DL_BurstTputMbps', 'SEA_ContextDropRate_%', 'SEA_ERABDropRateQCI1_%', 'SEA_ContextSetupFailure_%', 'SEA_AvgRRCconnectedUsers_percell', 'SEA_TotalDLRLCLayerDataVolume_MB', 'SEA_DLUserPerceivedTput_Mbps', 'FSM_LTE_DLRLCReTx%', 'FSM_LTE_DLResidBLER%', 'SEA_DLRLCReTx_%', 'SEA_DLResidualBLER_%', 'FSM_LTE_RRCF%', 'SEA_RRCSetupFailure_%', 'FSM_LTE_DataERABDrop%', 'SEA_DefaultERABDropRate_%']
    schema=column_list
    
    # Read the initial CSV file from the first date in date_range 
    file_location = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/rohitkovvuri/nokia_fsm_kpis_updated_v2/NokiaFSMKPIsSNAP_{}.csv".format(date_range[0])
    df_kpis = spark.read.option("header", "true").csv(file_location) 
    
    # Ensures that all values in 'ENODEB' column have exactly 6 characters. If a value has 5 characters, it adds a '0' in front of it
    df_kpis = pad_six_char(df_kpis)
    
    # Check if the DataFrame columns match the provided schema
    # this step is to check if the first dataframe prepared for union is missing,
    # if missing, directly return the warning
    if df_kpis.columns == column_list:
        print(date_range[0],df_kpis.count())
        pass
    else:
        print("There is no data in {}".format(date_range[0]))
        return
    
    # Iterate through a date range  
    for date_val in date_range[1:]: 
        try: 
            # Read the CSV file for the calculated date and remove duplicates 
            df_temp_kpi = spark.read.option("header", "true").csv("hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/rohitkovvuri/nokia_fsm_kpis_updated_v2/NokiaFSMKPIsSNAP_{}.csv".format(date_val)).dropDuplicates() 

            # Union the data from df_temp_kpi with df_kpis and apply filters 
            df_kpis = df_kpis.union(df_temp_kpi.select(df_kpis.columns)).filter(~(F.col("ENODEB") == "*")).filter(F.col("ENODEB").isNotNull()) 
            #print(date_val,df_temp_kpi.count())

        except: 
            # Handle the case where data is missing for the current date_val 
            print("Data missing for {}".format(date_val)) 
    
    #df_kpis
    
    return df_kpis

def rename_FSM_features(df):
    """
    Rename FSM related columns and S1U_SIP_SC_CallDrop%
    """ 
    df = df.withColumnRenamed("FSM_LTE_CtxDrop%", "context_drop_rate")\
            .withColumnRenamed("FSM_LTE_AvgRRCUsers", "avgconnected_users")\
            .withColumnRenamed("FSM_VOLTE_ERAB_QCI1_Drop%", "bearer_drop_voice_rate")\
            .withColumnRenamed("FSM_LTE_DLRLCMBytes", "dl_data_volume")\
            .withColumnRenamed("FSM_LTE_DL_BurstTputMbps", "uptp_user_perceived_throughput")\
            .withColumnRenamed("FSM_LTE_CtxSF%", "bearer_setup_failure_rate")\
            .withColumnRenamed("S1U_SIP_SC_CallDrop%", "sip_dc_rate")\
            .withColumnRenamed("FSM_LTE_DLRLCReTx%", "packet_retransmission_rate")\
            .withColumnRenamed("FSM_LTE_DLResidBLER%", "packet_loss_rate")\
            .withColumnRenamed("FSM_LTE_RRCF%", "rrc_setup_failure")\
            .withColumnRenamed("FSM_LTE_DataERABDrop%", "bearer_drop_rate")\
            .withColumnRenamed("event_date_final", "event_date")\
            .withColumnRenamed("from_event", "days_from_event") 
            
    return df

def rename_SEA_features(df):
    """
    Rename FSM related columns and S1U_SIP_SC_CallDrop%
    """ 
    df = df.withColumnRenamed("SEA_ContextDropRate_%", "context_drop_rate")\
            .withColumnRenamed("SEA_AvgRRCconnectedUsers_percell", "avgconnected_users")\
            .withColumnRenamed("SEA_ERABDropRateQCI1_%", "bearer_drop_voice_rate")\
            .withColumnRenamed("SEA_TotalDLRLCLayerDataVolume_MB", "dl_data_volume")\
            .withColumnRenamed("SEA_DLUserPerceivedTput_Mbps", "uptp_user_perceived_throughput")\
            .withColumnRenamed("SEA_ContextSetupFailure_%", "bearer_setup_failure_rate")\
            .withColumnRenamed("S1U_SIP_SC_CallDrop%", "sip_dc_rate")\
            .withColumnRenamed("SEA_DLRLCReTx_%", "packet_retransmission_rate")\
            .withColumnRenamed("SEA_DLResidualBLER_%", "packet_loss_rate")\
            .withColumnRenamed("SEA_RRCSetupFailure_%", "rrc_setup_failure")\
            .withColumnRenamed("SEA_DefaultERABDropRate_%", "bearer_drop_rate")\

    return df


def round_numeric_columns(df, decimal_places=2): 
    """ 
    Rounds all numeric columns in a PySpark DataFrame to the specified number of decimal places.
    
    Parameters:
        df (DataFrame): The PySpark DataFrame containing numeric columns to be rounded. 
        decimal_places (int, optional): The number of decimal places to round to (default is 2). 
    Returns: 
        DataFrame: A new PySpark DataFrame with numeric columns rounded to the specified decimal places. 
    """ 

    
    # List of numeric column names 
    numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"] 

    # Apply rounding to all numeric columns 
    for col_name in numeric_columns: 
        df = df.withColumn(col_name, round(df[col_name], decimal_places)) 
        
    return df 

def enodeb_mean(df, feature_list): 
    """ 
    Calculates the mean of specified features for each unique ENODEB in a PySpark DataFrame. 
    
    Parameters: 
        df (DataFrame): The PySpark DataFrame containing the data. 
        feature_list (list, optional): A list of feature column names to calculate the mean for. 
 
    Returns: 
        DataFrame: A new PySpark DataFrame with the mean values of the specified features for each ENODEB. 
    """ 

    # Define aggregate functions (calculating mean) 
    aggregate_functions = [F.avg] 

    # Create expressions to calculate the mean for each specified feature 
    expressions = [agg_func(F.col(col_name)).alias(col_name) for agg_func in aggregate_functions for col_name in feature_list] 

    # Aggregate the DataFrame by ENODEB and calculate the mean for specified features 
    # we can also use col_imp_stringcols = ['ENODEB', 'MMEPOOL','REGION','MARKET_DESC', 'SITE']
    # To be noticed. one enodeb might have two different 'REGION' (might be other features)
    df_avg_features = df.groupBy('ENODEB').agg(*expressions) 

    # Round the numeric columns to 2 decimal places (you can adjust as needed) 
    df_avg_features = round_numeric_columns(df_avg_features, decimal_places=2) 

    return df_avg_features

def lower_case_col_names(df,lower_case_cols_list):
    for col_name in lower_case_cols_list: 
        if col_name in df.columns: 
            df = df.withColumnRenamed(col_name, col_name.lower())
    return df

def fill_allday_zero_with_NA(df, features_list):

    # step 1. find samples (enodeb and day) features values are all zero
    fill_zero_na_df = df.withColumn("FSM_result", reduce(add, [col(x) for x in features_list])).filter(col('FSM_result') == 0 ).select(df.columns)
    for column in features_list: 
        if column != "enodeb": 
            fill_zero_na_df = fill_zero_na_df.withColumn(column, lit(None)) 
            
    # step 2. remove null_samples from original dataframe
    df_without_null = df.join(fill_zero_na_df,['day','enodeb'], "left_anti").select(df.columns)
    
    # step 3. union two dataframe together
    df_return = df_without_null.union(fill_zero_na_df)
    
    return df_return

def calculate_metrics(date_before_td, date_val): 

    """ 
    Calculate metrics based on FSM KPI data for event enodeb before maintenance (pre-event) and SEAM KPI data post-maintenance (post-event). 

    Parameters: 
        - date_before_td (str): The target date for post-event data in the format 'YYYY-MM-DD'. 
        - date_val (str): The reference date for pre-event data in the format 'YYYY-MM-DD'. 
        - date_val is used for enodeb_list when maintenance happend.
        - for example, "08-31-2023" (date_before_td) daily SEA KPI (post-event), the enodeb comes form enodeb maintained at date_val
        - which is 14 days before "09-01-2023" from "08-31-2023" to "08-18-2023"

    Returns: 
        - DataFrame: A PySpark DataFrame containing calculated metrics for each feature. 

    Steps: 

    1. Obtain the list of enodebs maintained at the given reference date. Event_Enodeb_List_{date_val}
    2. Retrieve post-event FSM KPI data for enodebs maintained one day before the target date. NokiaFSMKPIsSNAP_{date_before_td}.csv"
    3. Extract SEA_SIU columns and exclude FSM features. 
    4. Obtain pre-event features for the same enodebs, including feature means and standard deviations. 
    5. Join post-event and pre-event data, and calculate change rates, thresholds, and alerts for each feature. 
    6. Force the 'packet_loss_rate_alert' column to be 0. 
    7. Calculate the total number of abnormal KPIs and create a 'has_abnormal_kpi' column indicating abnormality presence. 

    Example Usage: 
    -------------- 
    df_metrics = calculate_metrics('2023-09-15', '2023-09-14') 
    df_metrics.show() 
    """ 

    # step 1. obtain event_enodeb_list at date_val
    file_path = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/Event_Enodeb_List_Date/Event_Enodeb_List_{}.json".format(date_val) 
    
    # Read the JSON file into a DataFrame 
    json_df = spark.read.json(file_path) 
    dictionary_list = json_df.collect() 
    enodeb_maintained_dict = dictionary_list[0].asDict()
    enodeb_maintained_list = list(enodeb_maintained_dict.values())[0]
    
    # step 2. obtain post feature of enodeb in event_enodeb_list at date_before_td
    
    file_location = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/rohitkovvuri/nokia_fsm_kpis_updated_v2/NokiaFSMKPIsSNAP_{}.csv".format(date_before_td)
    df_kpis = spark.read.option("header", "true").csv(file_location)
    df_kpis = pad_six_char(df_kpis)
    df_kpis_cur_list =df_kpis.filter(col('ENODEB').isin(enodeb_maintained_list) )
    df_kpis_cur_list = fill_allday_zero_with_NA(df_kpis_cur_list, features_list = SEA_list)
    # only consider SEA_SIU features, so exclude FSM_list
    exclude_columns = FSM_list
    selected_columns = [column for column in df_kpis_cur_list.columns if column not in exclude_columns]
    df_kpis_cur_list = df_kpis_cur_list.select(selected_columns)
    df_kpis_cur_list.columns
    
    # step 3. obtain pre-event features for enodeb in event_enodeb_list, including enodeb, feature mean, features std
    file_location = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/Event_Enodeb_Pre_Feature_Date/Event_Enodeb_Pre_{}.csv".format(date_val)
    df_kpis_pre = spark.read.option("header", "true").csv(file_location)
    
    # step 4. join two tables together, and compare post single-day feature and pre features, including feature mean, features std, threshold
    df_pre_post = df_kpis_cur_list.join(df_kpis_pre, on = 'ENODEB', how = 'inner' )
    df_pre_post = rename_SEA_features(df_pre_post)
    
    
    for feature in features_list_increase: 
        avg_feature = f"{feature}_avg"
        std_feature = f"{feature}_std"
        change_rate_column_name = f"{feature}_change_rate" 
        df_pre_post = df_pre_post.withColumn(change_rate_column_name, ( col(avg_feature) - col(feature) ) / col(avg_feature)) 
        #flag new code
        df_pre_post = df_pre_post.withColumn(change_rate_column_name, when(col(change_rate_column_name) < -1, -1).otherwise(col(change_rate_column_name))) 
        threshold_avg = f"{feature}_threshold_avg" 
        df_pre_post = df_pre_post.withColumn(threshold_avg, (col(avg_feature) + 0.1*col(avg_feature)) ) 
    
        threshold_std = f"{feature}_threshold_std" 
        df_pre_post = df_pre_post.withColumn(threshold_std, (col(avg_feature) + 2*col(std_feature)) )
        
        threshold_max = f"{feature}_threshold_max"
        df_pre_post = df_pre_post.withColumn(threshold_max, when(col(threshold_std) > col(threshold_avg), col(threshold_std)).otherwise(col(threshold_avg)))
        
        alert_feature = f"{feature}_alert"
        df_pre_post = df_pre_post.withColumn(alert_feature, when( col(threshold_max) < col(feature), 1).otherwise(0) )
    
    for feature in features_list_decrease: 
        avg_feature = f"{feature}_avg"
        std_feature = f"{feature}_std"
        change_rate_column_name = f"{feature}_change_rate" 
        df_pre_post = df_pre_post.withColumn(change_rate_column_name, (col(feature) - col(avg_feature)) / col(avg_feature)) 
        #flag new code
        df_pre_post = df_pre_post.withColumn(change_rate_column_name, when(col(change_rate_column_name) > 1, 1).otherwise(col(change_rate_column_name))) 
        
        threshold_avg = f"{feature}_threshold_avg" 
        df_pre_post = df_pre_post.withColumn(threshold_avg, (col(avg_feature) - 0.1*col(avg_feature)) ) 
    
        threshold_std = f"{feature}_threshold_std" 
        df_pre_post = df_pre_post.withColumn(threshold_std, (col(avg_feature) - 2*col(std_feature)) )
        
        threshold_max = f"{feature}_threshold_max"
        df_pre_post = df_pre_post.withColumn(threshold_max, when(col(threshold_std) < col(threshold_avg), col(threshold_std)).otherwise(col(threshold_avg)))
        
        alert_feature = f"{feature}_alert"
        df_pre_post = df_pre_post.withColumn(alert_feature, when( col(threshold_max) > col(feature), 1).otherwise(0) )
    
    # Force the entire column "packet_loss_rate_alert" to 0 
    df_pre_post = df_pre_post.withColumn("packet_loss_rate_alert", lit(0)) 
    
    alert_list = [f"{feature}_alert" for feature in features_list]
    
    df_pre_post = df_pre_post.withColumn("abnormal_kpi", reduce(add, [col(x) for x in alert_list])  )
    df_pre_post = df_pre_post.withColumn('has_abnormal_kpi', when(df_pre_post['abnormal_kpi'] > 0, 1).otherwise(0))
    
    # while calculating change_rate, the denominator(average_feature might be zero)
    df_pre_post = df_pre_post.fillna(0)
    df_pre_post = df_pre_post.withColumn('DAY',  F.from_unixtime(F.unix_timestamp('DAY', 'MM/dd/yyy'))  ).withColumn('DAY', F.date_format('DAY', 'yyyy-MM-dd'))
    return df_pre_post

def move_column_forward(df, column_list):
    try:
        selected_columns = df.columns
        for i in range(len(column_list)):
            selected_columns.remove( column_list[i] )
        
        for i in range(len(column_list)):
            selected_columns.insert(6, column_list[i] )
        df = df.select(selected_columns)
    except:
        print("move_column_forward() does not work")
        return df
    return df

def haversine(lat2,lon2, lat1, lon1):
    if lat2 is not None and lat1 is not None and lon1 is not None and lon2 is not None :
        """
        Calculate the great circle distance in kilometers between two points 
        on the earth (specified in decimal degrees)
        """
        # meter per sce to mph
        # convert decimal degrees to radians 
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
        # haversine formula 
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371.7 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.

        return c * r
    else:
        return 1000.0

def get_enodeb_tickets_nrb(df_enb_list, date_val, df_enb_cord):
    window_recentTickets=Window().partitionBy("trouble_id","status").orderBy(F.desc("MODIFIED_DATE"),F.desc("create_date_nrb"),F.desc("NRB_ASSIGNED_DATE"),F.desc("lat"),F.desc("lng"))
    window_dist=Window().partitionBy("trouble_id","status").orderBy("distance_from_enb")
    
    tickets_path='hdfs://njbbepapa1.nss.vzwnet.com:9000/user/kovvuve/epa_tickets/epa_tickets_{}-*.csv.gz'
    df_tickets = spark.read.option("header","true").option("delimiter", "|}{|").csv(tickets_path.format(date_val)).dropDuplicates()\
                        .filter(F.col("lat").isNotNull()).filter(F.col("lng").isNotNull())\
                        .withColumnRenamed("COPY_OF_STATUS", "status")\
                        .withColumn("unix_date_mdified",F.from_unixtime("modified_unix_time","MM-dd-yyyy"))\
                        .select( 'trouble_id', 'status', F.to_date('NRB_ASSIGNED_DATE').alias("NRB_ASSIGNED_DATE"), F.to_date('create_date').alias("create_date_nrb"), F.to_date('MODIFIED_DATE').alias("MODIFIED_DATE"), 'lat', 'lng')\
                        .dropDuplicates()\
                        .withColumn("recent_tickets_row",F.row_number().over(window_recentTickets))\
                        .filter(F.col("recent_tickets_row")==1)\
                        .withColumn("id_lat",F.col("lat").cast("double"))\
                        .withColumn("id_lng",F.col("lng").cast("double"))\
                        .withColumn("id_lat",(F.col("lat")*F.lit(1.0)).cast("int"))\
                        .withColumn("id_lng",(F.col("lng")*F.lit(1.0)).cast("int")).select("trouble_id","status","create_date_nrb",F.col("lat").cast("double"),F.col("lng").cast("double"))
    df_tickets_agg =  df_tickets.groupby("trouble_id","status","create_date_nrb","lat","lng").count()
    df_tickets_agg.cache()
    df_tickets_agg.count()
    
    df_tickets_open =df_tickets_agg.filter(F.lower(F.col("status"))=="open")
    df_tickets_notopen =df_tickets_agg.filter(~(F.lower(F.col("status"))=="open"))
    
    df_tickets_2 = df_tickets_open.join(df_tickets_notopen,df_tickets_notopen.trouble_id == df_tickets_open.trouble_id,"left_anti").select(df_tickets_open['*'])\
                                    .filter(F.col("lat").isNotNull() & F.col("lng").isNotNull())
    df_enb_comb =df_enb_cord.join(broadcast(df_enb_list),F.lpad(df_enb_list.enodeb_event,6,'0') ==F.lpad(df_enb_cord.ENODEB,6,'0'),"right")
    df_enb_comb = df_tickets_2.crossJoin(broadcast(df_enb_comb)).withColumn("includeTicket",F.when(F.col("create_date_nrb")>=F.to_date("event_date_final"),F.lit("Y")).otherwise(F.lit("N"))).filter(F.col("includeTicket")=="Y")
    #--------------------------------------------------------------------------------------------------
    # cross this logic once again..
    df_enb_comb_dist = df_enb_comb\
                        .withColumn("distance_from_enb",get_haversine_udf(df_enb_comb.LATITUDE,df_enb_comb.LONGITUDE,df_enb_comb.lat,df_enb_comb.lng).cast("double")/F.lit(1.60934))\
                        .filter(F.col("distance_from_enb")<=5.0)\
                        .withColumn("trouble_id_row",F.row_number().over(window_dist))\
                        .filter(F.col("trouble_id_row")==1).filter(F.lower(F.col("status"))=="open")\
                        .withColumn("po_box_address",F.lit(0))\
                        .withColumn("ticket_source",F.lit("nrb"))\
                        .select("enodeb_event",F.col("LATITUDE").alias("enb_lat"),F.col("LONGITUDE").alias('enb_lng'),"trouble_id","event_date_final",F.col("create_date_nrb").alias("create_date"),"po_box_address","ticket_source")\
                        .sort("enodeb_event","event_date_final","create_date")
    df_enb_tickets = df_enb_comb_dist.groupby("enodeb_event").agg(F.count("trouble_id").alias('nrb_ticket_counts'))\
                                    .select(F.col('enodeb_event').alias("ENODEB"), F.col('nrb_ticket_counts') )

    return df_enb_tickets

def get_enodeb_tickets_w360(df_enb_list_dup,date_start,daysBack):
    df_enb_list = df_enb_list_dup.groupby("enodeb_event").agg(F.first("event_date_final").alias("event_date_final"))
    window_recentTickets=Window().partitionBy('NOC_TT_ID', 'status').orderBy(F.desc("event_start_date"),F.desc("event_end_date"),F.desc("lat_enb"),F.desc("lng_enb"))


    #date_start = adjust_date(date_today,1,"%Y-%m-%d")
    
    date_val = date_start
    df_kpis_w360 = spark.read.option("header","true").csv("hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/workorder_oracle/DT={}".format(date_start)).dropDuplicates().withColumn("data_date",F.lit(date_start))
    print("date_start:",date_start)
    print("hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/workorder_oracle/DT={}".format(date_val))
    for idate in range(1,daysBack):
        date_val = adjust_date(date_start,idate,"%Y-%m-%d")
        
        try:
            df_temp_kpi = spark.read.option("header","true").csv("hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/workorder_oracle/DT={}".format(date_val)).dropDuplicates().withColumn("data_date",F.lit(date_val))
            df_kpis_w360 = df_kpis_w360.union(df_temp_kpi.select(df_kpis_w360.columns))
            #print("hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/workorder_oracle/DT={}".format(date_val))
        except:
            print("data missing for {}".format(date_val))
            
    df_tickets_w360 =  df_kpis_w360.withColumnRenamed("STATUS", "status")\
                    .withColumn("event_start_date",F.to_date(F.substring(F.col("EVENT_START_DATE"),1,10)))\
                    .withColumn("event_end_date",F.to_date(F.substring(F.col("EVENT_END_DATE"),1,10)))\
                    .withColumn("CELL_ID",F.col("CELL__"))\
                    .withColumn("enb_id_w360",F.when(F.length("CELL_ID")>8,F.expr("substring(CELL_ID,1,length(CELL_ID)-4)") ).otherwise(F.col("CELL_ID")) )\
                    .filter(F.col("CELL_ID").isNotNull())\
                    .filter(F.lower(F.col("status")).isin("open"))\
                    .select( 'NOC_TT_ID', 'status',"enb_id_w360","CELL_ID","event_start_date","event_end_date", F.col('LAT').alias("lat_enb"), F.col('LONG_X').alias("lng_enb"))\
                    .dropDuplicates()\
                    .withColumn("recent_tickets_row",F.row_number().over(window_recentTickets))\
                    .filter(F.col("recent_tickets_row")==1)\
                    .select('NOC_TT_ID', 'status',"enb_id_w360","CELL_ID","event_start_date","event_end_date",F.col("lat_enb").cast("double"),F.col("lng_enb").cast("double"))
    
    df_tickets_agg =  df_tickets_w360.groupby('NOC_TT_ID', 'status',"enb_id_w360","event_start_date","event_end_date","lat_enb","lng_enb").count()
    
    df_w360 = df_enb_list.join(df_tickets_agg,F.lpad(df_enb_list.enodeb_event,6,'0') ==F.lpad(df_tickets_agg.enb_id_w360,6,'0'))\
                            .withColumn("includeTicket",F.when(F.col("event_start_date")>=F.to_date("event_date_final"),F.lit("Y")).otherwise(F.lit("N"))).filter(F.col("includeTicket")=="Y")\
                            .withColumn("ticket_source",F.lit("w360"))\
                            .select("enodeb_event",F.col("lat_enb").alias("enb_lat"),F.col("lng_enb").alias('enb_lng'),F.col("NOC_TT_ID").alias("trouble_id"),"event_date_final",F.col("event_start_date").alias("create_date"),"ticket_source")\
                            .sort("enodeb_event","event_date_final","create_date")
    df_w360_tickets = df_w360.groupby("enodeb_event").agg(F.count("ticket_source").alias('w360_ticket_counts'))\
                                .select(F.col('enodeb_event').alias("ENODEB"),F.col('w360_ticket_counts') )
    return df_w360_tickets

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('Post_Event_Features').enableHiveSupport().getOrCreate()
    
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")
    #parser.add_argument("--date", default=datetime.today().strftime('%Y%m%d'), help="Date for Wifi Scores")
    #parser.add_argument("--lookback", default=0, help="Days for Back filling WifiScores")
    #parser.add_argument("--dg_dir", default=0, help="path to device group data")
    #parser.add_argument("--sh_dir", default=0, help="path to station history path")
    #parser.add_argument("--detail_dir", default=0, help="path to detailed data path")
    #parser.add_argument("--write_dir", default=0, help="path to write path")

    args = parser.parse_args()
    # input as start_date_str and end_date_str
    get_haversine_udf = F.udf(haversine, DoubleType()) 
    column_list = ['DAY', 'MMEPOOL', 'REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB', 'FSM_LTE_CtxDrop%', 'FSM_VOLTE_ERAB_QCI1_Drop%', 'FSM_LTE_CtxSF%', 'S1U_SIP_SC_CallDrop%', 'FSM_LTE_AvgRRCUsers', 'FSM_LTE_DLRLCMBytes', 'FSM_LTE_DL_BurstTputMbps', 'SEA_ContextDropRate_%', 'SEA_ERABDropRateQCI1_%', 'SEA_ContextSetupFailure_%', 'SEA_AvgRRCconnectedUsers_percell', 'SEA_TotalDLRLCLayerDataVolume_MB', 'SEA_DLUserPerceivedTput_Mbps', 'FSM_LTE_DLRLCReTx%', 'FSM_LTE_DLResidBLER%', 'SEA_DLRLCReTx_%', 'SEA_DLResidualBLER_%', 'FSM_LTE_RRCF%', 'SEA_RRCSetupFailure_%', 'FSM_LTE_DataERABDrop%', 'SEA_DefaultERABDropRate_%']

    FSM_list = [element for element in column_list if element[:3] == "FSM"]
    SEA_list = [element for element in column_list if element[:3] == "SEA"]

    FSM_SIU_list = FSM_list.copy()
    FSM_SIU_list.append("S1U_SIP_SC_CallDrop%")

    SEA_SIU_list = SEA_list.copy()
    SEA_SIU_list.append("S1U_SIP_SC_CallDrop%")

    String_typeCols_List = ['DAY', 'MMEPOOL','REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB']

    features_list = ['sip_dc_rate', 'context_drop_rate', 'bearer_drop_voice_rate', 'bearer_setup_failure_rate', 'avgconnected_users', 'dl_data_volume', 'uptp_user_perceived_throughput', 'packet_retransmission_rate', 'packet_loss_rate', 'rrc_setup_failure', 'bearer_drop_rate']
    #features_list.remove('packet_loss_rate')
    features_list.sort()

    features_list_increase = ['sip_dc_rate', 'context_drop_rate', 'bearer_drop_voice_rate', 'bearer_setup_failure_rate','packet_loss_rate',  'packet_retransmission_rate', 'rrc_setup_failure', 'bearer_drop_rate']

    features_list_decrease = ['avgconnected_users', 'dl_data_volume','uptp_user_perceived_throughput']

    avg_features_list = []
    std_features_list = []
    change_rate_feature_list = []
    threshold_std_feature_list = []
    threshold_avg_feature_list = []
    threshold_max_feature_list = []
    alert_feature_list = []

    for feature in features_list:
        avg_features_list.append(f"{feature}_avg" )
        std_features_list.append(f"{feature}_std" )
        change_rate_feature_list.append(f"{feature}_change_rate" )
        threshold_std_feature_list.append(f"{feature}_threshold_std" )
        threshold_avg_feature_list.append(f"{feature}_threshold_avg" )
        threshold_max_feature_list.append(f"{feature}_threshold_max" )
        alert_feature_list.append(f"{feature}_alert" )

    # read enodeb-coordinates ----------------------------------------------------------------------------------------
    oracle_file = "hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/atoll_oracle_daily/date=2023-10-08"
    df_enb_cord_original = spark.read.format("com.databricks.spark.csv").option("header", "True").load(oracle_file)\
                        .filter(F.col("LATITUDE_DEGREES_NAD83").isNotNull()).filter(F.col("LONGITUDE_DEGREES_NAD83").isNotNull()).filter(F.col("ENODEB_ID").isNotNull())
    df_enb_cord =  df_enb_cord_original.groupby("ENODEB_ID","LATITUDE_DEGREES_NAD83","LONGITUDE_DEGREES_NAD83").count().select( col('ENODEB_ID').alias('ENODEB'), F.col('LATITUDE_DEGREES_NAD83').cast("double").alias('LATITUDE'), F.col('LONGITUDE_DEGREES_NAD83').cast("double").alias('LONGITUDE'))
    df_enb_cord = pad_six_char(df_enb_cord, column_name="ENODEB" )
    df_enb_cord.cache()
    df_enb_cord.count()

    # only consider September Data----------------------------------------------------------------------------------------
    date_range_1 = generate_date_range('2023-10-01', days = 30)

    for i in range(len(date_range_1)):
        
        print(i, date_range_1[i],'\n')
        
        date_td = date_range_1[i]
        date_before_td = adjust_date(date_td,1)
        date_ticket = date_before_td
        date_range_2 = generate_date_range( current_date =date_range_1[i], days =15 )
        
        for j in range(1,len(date_range_2)):
            date_val = date_range_2[j]
            date_event = date_val
            try:
                
                df_pre_post = calculate_metrics(date_before_td, date_val)
                
                exclude_columns =  threshold_avg_feature_list + threshold_std_feature_list + threshold_max_feature_list
                selected_columns = [column for column in df_pre_post.columns if column not in exclude_columns]
                selected_columns.sort()
                selected_columns.remove("has_abnormal_kpi")  # Remove the item from its current position 
                selected_columns.insert(7, "has_abnormal_kpi")  # Insert the item at the beginning of the list
                df_pre_post = df_pre_post.select(selected_columns)
                # add coordinates --------------------------------------------------------
                df_pre_post = df_pre_post.withColumn('event_day', lit(date_val))
                joined_df = df_enb_cord.join(broadcast(df_pre_post), 'ENODEB', "right") 
                joined_df = move_column_forward(joined_df, ['LATITUDE','LONGITUDE'])
                
                # Get tickets of event enodeb----------------------------------------------------------------
                file_path = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/Event_Enodeb_List_Date/Event_Enodeb_List_{}.json".format(date_event) 
                json_df = spark.read.json(file_path) 
                dictionary_list = json_df.collect() 
                enodeb_maintained_dict = dictionary_list[0].asDict()
                enodeb_maintained_list = list(enodeb_maintained_dict.values())[0]
                
                # Create a DataFrame from the list of Row objects
                rows = [Row(enodeb_event = enodeb) for enodeb in enodeb_maintained_list] 
                df_enb_list1 = spark.createDataFrame(rows)
                df_enb_list1 =df_enb_list1.withColumn('event_date_final',lit(date_event))
                
                # get nrb tickets
                df_enb_tickets_nrb = get_enodeb_tickets_nrb(df_enb_list1, date_ticket, df_enb_cord)
                joined_df = joined_df.join(broadcast(df_enb_tickets_nrb), 'ENODEB', "left")
        
                
                # get w360 tickets-----------------------------------------------------------------
                # Convert the date strings to datetime objects 
                date_format = "%Y-%m-%d" 
                date1 = datetime.strptime(date_before_td, date_format) 
                date2 = datetime.strptime(date_event, date_format) 
                
                date_difference = (date1 - date2).days 
                
                df_w360_tickets = get_enodeb_tickets_w360(df_enb_list1, date_td, date_difference)
                joined_df = joined_df.join(broadcast(df_w360_tickets), 'ENODEB', "left")
                

                #Finalize and output dataframe-----------------------------------------------------------------------
                # round columns
                joined_df = round_numeric_columns(joined_df,decimal_places=4)
                joined_df = joined_df.fillna(0)
                joined_df = lower_case_col_names( joined_df, String_typeCols_List)
                joined_df = lower_case_col_names( joined_df, ['LATITUDE','LONGITUDE'])
                # repartition and write
                joined_df = joined_df.repartition(1)
                
                # output post feature dataframe
                output_path = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/Event_Enodeb_Post_tickets_Feature_Date/{}_tickets_Post_Feature_of_Enodeb/{}_tickets_Post_feature_maintained_{}.csv".format(date_before_td, date_before_td, date_val )
                joined_df.write.csv(output_path, header=True, mode="overwrite")
                
                print(output_path,"\n")
            except Exception as e:
                print("error:{}".format(e))
                print("no event at {}".format(date_val))
            #break # flag
        print('')
    
        
    
