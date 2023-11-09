from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from pyspark.sql import functions as F 
from pyspark.sql.functions import (from_unixtime,lpad,broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 
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
def union_df_from_date_start(date_start, forward_day = 16): 
    """ 
    Union data from multiple CSV files within a date range into a single DataFrame. 
    Parameters: 
    - date_start (str): The starting date for data retrieval in the format 'YYYY-MM-DD'. 
    - forward_day (int, optional): The number of days forward from 'date_start' to include in the date range. 
    - schema (list, optional): A list of column names representing the schema of the DataFrame. 
    
    Returns: 
    - DataFrame: A PySpark DataFrame containing data from multiple CSV files within the specified date range. 
 
    This function reads an initial CSV file for the given 'date_start' and then iterates through a date range 
    (1 to 'forward_day' days before 'date_start'). For each date in the range, it reads a corresponding CSV file, removes 
    duplicate rows, and unions the data with the initial DataFrame. Filters are applied to exclude rows with 
    "ENODEB" equal to "*" and null values in the "ENODEB" column. The function returns the combined DataFrame. 
    Example: 
    -------- 
    # Read data starting from '2023-08-01' and union data for the next 15 days 
    combined_data = union_df_from_date_start('2023-08-01') 
    combined_data.show() 
    """ 
    column_list = ['DAY', 'MMEPOOL', 'REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB', 'FSM_LTE_CtxDrop%', 'FSM_VOLTE_ERAB_QCI1_Drop%', 'FSM_LTE_CtxSF%', 'S1U_SIP_SC_CallDrop%', 'FSM_LTE_AvgRRCUsers', 'FSM_LTE_DLRLCMBytes', 'FSM_LTE_DL_BurstTputMbps', 'SEA_ContextDropRate_%', 'SEA_ERABDropRateQCI1_%', 'SEA_ContextSetupFailure_%', 'SEA_AvgRRCconnectedUsers_percell', 'SEA_TotalDLRLCLayerDataVolume_MB', 'SEA_DLUserPerceivedTput_Mbps', 'FSM_LTE_DLRLCReTx%', 'FSM_LTE_DLResidBLER%', 'SEA_DLRLCReTx_%', 'SEA_DLResidualBLER_%', 'FSM_LTE_RRCF%', 'SEA_RRCSetupFailure_%', 'FSM_LTE_DataERABDrop%', 'SEA_DefaultERABDropRate_%']
    schema = column_list
    
    # Read the initial CSV file for the calculated date_start 
    
    df_kpis = spark.read.option("header", "true").csv("hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/rohitkovvuri/nokia_fsm_kpis_updated_v2/NokiaFSMKPIsSNAP_{}.csv".format(date_start)) 
    # Ensures that all values in 'ENODEB' column have exactly 6 characters. If a value has 5 characters, it adds a '0' in front of it
    df_kpis = pad_six_char(df_kpis)
    
    # Check if the DataFrame columns match the provided schema
    # this step is to check if the first dataframe prepared for union is missing,
    # if missing, directly return the warning
    if df_kpis.columns == column_list:
        #print(date_start,df_kpis.count())
        pass
    else:
        print("There is no data in {}".format(date_start))
        return
    
    # Iterate through a date range (1 to 15 days before date_start) 
    for idate in range(1, forward_day): 
        # Calculate the date value for the current iteration, idate days before date_start 
        date_val = adjust_date(date_start, idate, "%Y-%m-%d") 
        try: 
            # Read the CSV file for the calculated date and remove duplicates 
            df_temp_kpi = spark.read.option("header", "true").csv("hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/rohitkovvuri/nokia_fsm_kpis_updated_v2/NokiaFSMKPIsSNAP_{}.csv".format(date_val)).dropDuplicates() 
            df_temp_kpi = pad_six_char(df_temp_kpi)
            # Union the data from df_temp_kpi with df_kpis and apply filters 
            # After the iteration, we should get all 15-days data from date_start 
            df_kpis = df_kpis.union(df_temp_kpi.select(df_kpis.columns)).filter(~(F.col("ENODEB") == "*")).filter(F.col("ENODEB").isNotNull()) 
            #print(date_val,df_temp_kpi.count()) 
        except: 
            # Handle the case where data is missing for the current date_val 
            print("Data missing for {}".format(date_val)) 
    
    #df_kpis
    
    return df_kpis
#below code is to test the performance of function 
   
#date_td = '2023-09-19'
#date_start = adjust_date(date_td, 1, "%Y-%m-%d") 
#print("date_start:", date_start)
#df_kpis = union_df_from_date_start(date_start, forward_day = 16)
 
def convert_string_numerical(df, String_typeCols_List=['DAY', 'MMEPOOL','REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB']): 
    """ 
    This function takes a PySpark DataFrame and a list of column names specified in 'String_typeCols_List'. 
    It casts the columns in the DataFrame to double type if they are not in the list, leaving other columns 
    as they are. 
    Parameters: 
    - df (DataFrame): The PySpark DataFrame to be processed. 
    - String_typeCols_List (list): A list of column names not to be cast to double. 
    Returns: 
    - DataFrame: A PySpark DataFrame with selected columns cast to double. 
    """ 
    # Cast selected columns to double, leaving others as they are 
    df = df.select([F.col(column).cast('double') if column not in String_typeCols_List else F.col(column) for column in df.columns]) 
    return df
#below code is to test the performance of function
#String_typeCols_List = ['DAY', 'MMEPOOL','REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB']
#df_kpis = convert_string_numerical(df_kpis, String_typeCols_List)
#df_kpis.dtypes
def get_event_list(df_kpis):
    
    """ 
    Identify and return a set of 'ENODEB' values representing network nodes 
    that experience change from Nokia to Samsung 
    Args: 
    df_kpis (DataFrame): A PySpark DataFrame containing network performance data. including enodeb, DAY, and FSM/SEA related features
    Returns: 
    set: A set of 'ENODEB' values that meet the criteria. 
    This function performs the following steps: 
    1. Converts the 'DAY' column to a date format and computes the sum of Samsung 
       and Nokia features for each network node. 
       
    2. Filters network nodes that have Samsung data at the first date within their 
       respective time windows, indicating maintenance has already been conducted. 
    3. Filters network nodes where the sum of Samsung features is 0. which means the enodeb does not have maintenance the entire time window. 
    4. Calculates the remaining network nodes by subtracting excluded nodes from 
       the total nodes. 
    """ 
    # for simplicity, we sum up all samsung/nokia feature together.
    # since there is no negative feature. sum feature indicate which device is using.
    df = df_kpis.withColumn("DAY",F.to_date(F.col("DAY"),"MM/dd/yyyy"))\
                        .withColumn("nokia", reduce(add, [col(x) for x in FSM_list]))\
                        .withColumn("samsung", reduce(add, [col(x) for x in SEA_list])) 

    #----------------------------------------------------------------------------------
    # step 1. filter enodeb have SEA data at the first date
    # Define a window specification to partition by 'ENODEB' and order by 'DAY' 
    window_spec = Window.partitionBy("ENODEB").orderBy("DAY") 
    # Add a new column 'min_date' with the minimum date within each 'ENODEB' partition 
    df = df.withColumn("min_date", min(col("DAY")).over(window_spec)) 
    # Filter rows with the minimum date 
    result_df = df.filter(col("DAY") == col("min_date")) 
    # Select the relevant columns ('ENODEB' and 'samsung') 
    result_df = result_df.select("ENODEB", "samsung")
    
    # at the first of the current time window, there exist samsung data.
    # this means maitenance already conducted
    enodeb_SEA_from_begin = result_df.filter(col('samsung')!=0).select('ENODEB').rdd.map(lambda row: row[0]).collect()
    #----------------------------------------------------------------------------------
    # step 2. Filter 'ENODEB' values where the samsung features is 0
    # Sum up all samsung features across all days of each ENODEB 
    sum_df = df.groupBy("ENODEB").agg(
                                sum('nokia').alias("sum_nokia"),
                                sum('samsung').alias("sum_samsung")
                                ) 
    
    verify_list2 = sum_df.filter(col("sum_samsung") == 0).select("ENODEB").rdd.map(lambda row: row[0]).collect()
    verify_list3 = sum_df.filter(col("sum_nokia") == 0).select("ENODEB").rdd.map(lambda row: row[0]).collect()
    #----------------------------------------------------------------------------------
    # step 3. total case - two excluded case together = remaining window case
    
    # Combine 'ENODEB' values excluded in step 1 and step 2
    exclude_enodeb_list = set(enodeb_SEA_from_begin).union(set(verify_list2)).union(set(verify_list3))
    
    # Obtain the total list of 'ENODEB' values 
    total_enodeb_list = df.select('ENODEB').distinct().rdd.map(lambda row: row[0]).collect()
    
    # Calculate the remaining 'ENODEB' values
    remain_enodeb_list = set(total_enodeb_list) - exclude_enodeb_list
    # Return the set of remaining 'ENODEB' values     
    return set(remain_enodeb_list)
def get_enodeb_date(df,remain_enodeb_set):
    """ 
    Extract the first positive Samsung date per ENODEB from a DataFrame and return a DataFrame 
    containing the ENODEB and its corresponding first positive Samsung date. 
    The first positive Samsung data indicates the event date, because Samsung data only show up after event.
    :param df: The DataFrame containing the data to be filtered and processed. 
    :param remain_enodeb_set: A set of ENODEB values to filter the data. 
    :return: A DataFrame with two columns: 'enodeb_event' and 'event_date_final'. 
    """ 
    
    # step 1: find all the data where enodeb is in the list
    filtered_df = df.filter(col('ENODEB').isin(list(remain_enodeb_set)))
    # -------------------------------------
    # step 2: find the first positive SEA date
    # Aggregate sum samsung data together
    filtered_df_sum = filtered_df.withColumn("DAY",F.to_date(F.col("DAY"),"MM/dd/yyyy"))\
                        .withColumn("nokia", reduce(add, [col(x) for x in FSM_list]))\
                        .withColumn("samsung", reduce(add, [col(x) for x in SEA_list])) 
                        
    # Define a window specification to partition by 'ENODEB' and order by 'DAY' 
    window_spec = Window.partitionBy("ENODEB").orderBy("DAY") 
    
    # Create a new column 'first_positive_samsung_day' that contains the first positive 'DAY' per 'ENODEB' 
    result_df = filtered_df_sum.withColumn( 
        "first_positive_samsung_day", 
        first(when(col("samsung") > 0, col("DAY")), ignorenulls=True).over(window_spec) 
    ) 
    # Filter out rows with null values in 'first_positive_samsung_day' 
    result_df = result_df.filter(col('first_positive_samsung_day').isNotNull()) 
    # Select the resulting columns and rename them
    distinct_df = result_df.select('ENODEB','first_positive_samsung_day').distinct()
    distinct_df = distinct_df.withColumnRenamed("ENODEB", "enodeb_event").withColumnRenamed("first_positive_samsung_day", "event_date_final")
    
    return distinct_df
def get_enodeb_features(new_event_enodeb, df_kpis):
    """ 
    Perform an inner join between the 'df_kpis' DataFrame and the 'new_event_enodeb' DataFrame 
    based on the 'ENODEB' and 'enodeb_event' columns respectively, and return the resulting DataFrame. 
    """ 
    # Broadcast the smaller DataFrame 
    broadcast_new_event_enodeb = broadcast(new_event_enodeb) 
    
    # Specify the columns to join on 
    join_condition = df_kpis["ENODEB"] == broadcast_new_event_enodeb["enodeb_event"] 
    
    # Perform the inner join 
    new_event_enodeb_features = df_kpis.join(broadcast_new_event_enodeb, join_condition, "inner")
    
    return new_event_enodeb_features
def get_date_range(start_date_str, end_date_str): 
    """ 
    Generate a date range between the given start and end dates (inclusive). 
    Args: 
        start_date_str (str): The start date in string format (e.g., "2023-08-01"). 
        end_date_str (str): The end date in string format (e.g., "2023-09-19"). 
    Returns: 
        list of datetime.date: A list of date objects representing the date range between 
        the start and end dates (inclusive). 
    """ 
    # Define the start date and end date 
    start_date = parse(start_date_str).date() 
    end_date = parse(end_date_str).date() 
    # Calculate the date range between the start and end dates 
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)] 
    return date_range 
#d_r = get_date_range('2023-08-01' , '2023-08-31')
#print(d_r[0], d_r[-1])
    
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
    # Rename the columns to match the original feature names 
    df_avg_features = rename_FSM_features(df_avg_features) 
    
    columns_to_rename = df_avg_features.columns 
    # Iterate through columns and rename except ENODEB 
    for column in columns_to_rename: 
        if column != "ENODEB": 
            df_avg_features = df_avg_features.withColumnRenamed(column, column + "_avg") 
    
    return df_avg_features 
def enodeb_stddev(df, feature_list=None): 
    """ 
    Calculates the standard deviation of specified features for each unique ENODEB in a PySpark DataFrame. 
    Parameters: 
        df (DataFrame): The PySpark DataFrame containing the data. 
        feature_list (list, optional): A list of feature column names to calculate the standard deviation for. 
                                      If None, it uses the predefined FSM_SIU_list (if defined). 
    Returns: 
        DataFrame: A new PySpark DataFrame with the standard deviation values of the specified features for each ENODEB. 
    """ 
    # Check if FSM_SIU_list is defined in the global environment 
    if "FSM_SIU_list" not in globals() and feature_list is None: 
        raise ValueError("The default feature_list, FSM_SIU_list, is not defined in the global environment.") 
    # If feature_list is not provided, use the predefined FSM_SIU_list (if defined) 
    if feature_list is None: 
        feature_list = globals().get("FSM_SIU_list", None) 
    # Check if feature_list is still None 
    if feature_list is None: 
        raise ValueError("feature_list is None, and FSM_SIU_list is not defined in the global environment.") 
    # Define aggregate functions (calculating standard deviation) 
    aggregate_functions = [F.stddev] 
    # Create expressions to calculate the standard deviation for each specified feature 
    expressions = [agg_func(F.col(col_name)).alias(col_name) for agg_func in aggregate_functions for col_name in feature_list] 
    # Aggregate the DataFrame by ENODEB and calculate the standard deviation for specified features 
    df_stddev_features = df.groupBy('ENODEB').agg(*expressions) 
    # Round the numeric columns to 2 decimal places (you can adjust as needed) 
    df_stddev_features = round_numeric_columns(df_stddev_features, decimal_places=2) 
    # Rename the columns to match the original feature names 
    df_stddev_features = rename_FSM_features(df_stddev_features)
    
    columns_to_rename = df_stddev_features.columns 
    # Iterate through columns and rename except ENODEB 
    for column in columns_to_rename: 
        if column != "ENODEB": 
            df_stddev_features = df_stddev_features.withColumnRenamed(column, column + "_std") 
    return df_stddev_features
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
if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('Snap_Pre_Event').enableHiveSupport().getOrCreate()
    
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")
    #parser.add_argument("--date", default=datetime.today().strftime('%Y%m%d'), help="Date for Wifi Scores")
    #parser.add_argument("--lookback", default=0, help="Days for Back filling WifiScores")
    #parser.add_argument("--dg_dir", default=0, help="path to device group data")
    #parser.add_argument("--sh_dir", default=0, help="path to station history path")
    #parser.add_argument("--detail_dir", default=0, help="path to detailed data path")
    #parser.add_argument("--write_dir", default=0, help="path to write path")
    args = parser.parse_args()
    # input as start_date_str and end_date_str-----------------------------------------------------------------------------------
    start_date_str = "2023-09-01"
    end_date_str = "2023-09-30"
    date_range = get_date_range(start_date_str, end_date_str)
    #----------------------------------------------------------------------------------------------------------------------------
    date_range =[datetime.now().date()]
    #----------------------------------------------------------------------------------------------------------------------------
    column_list = ['DAY', 'MMEPOOL', 'REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB', 'FSM_LTE_CtxDrop%', 'FSM_VOLTE_ERAB_QCI1_Drop%', 'FSM_LTE_CtxSF%', 'S1U_SIP_SC_CallDrop%', 'FSM_LTE_AvgRRCUsers', 'FSM_LTE_DLRLCMBytes', 'FSM_LTE_DL_BurstTputMbps', 'SEA_ContextDropRate_%', 'SEA_ERABDropRateQCI1_%', 'SEA_ContextSetupFailure_%', 'SEA_AvgRRCconnectedUsers_percell', 'SEA_TotalDLRLCLayerDataVolume_MB', 'SEA_DLUserPerceivedTput_Mbps', 'FSM_LTE_DLRLCReTx%', 'FSM_LTE_DLResidBLER%', 'SEA_DLRLCReTx_%', 'SEA_DLResidualBLER_%', 'FSM_LTE_RRCF%', 'SEA_RRCSetupFailure_%', 'FSM_LTE_DataERABDrop%', 'SEA_DefaultERABDropRate_%']
    FSM_list = [element for element in column_list if element[:3] == "FSM"]
    SEA_list = [element for element in column_list if element[:3] == "SEA"]
    FSM_SIU_list = FSM_list.copy()
    FSM_SIU_list.append("S1U_SIP_SC_CallDrop%")
    SEA_SIU_list = SEA_list.copy()
    SEA_SIU_list.append("S1U_SIP_SC_CallDrop%")
    # Define a list of string columns that should be cast to double 
    String_typeCols_List = ['DAY', 'MMEPOOL','REGION', 'MARKET', 'MARKET_DESC', 'SITE', 'ENODEB']
    features_list = ['sip_dc_rate', 'context_drop_rate', 'bearer_drop_voice_rate', 'bearer_setup_failure_rate', 'avgconnected_users', 'dl_data_volume', 'uptp_user_perceived_throughput', 'packet_retransmission_rate', 'packet_loss_rate', 'rrc_setup_failure', 'bearer_drop_rate']
    #for-loop start----------------------------------------------------------------------------------------------------------
    for date_td in date_range:
        #date_td = date_range[1] #replace by for date_td in date_range:
        date_start = date_td - timedelta(days = 1)
        date_start_str = str(date_start)
        
        df_kpis = union_df_from_date_start(date_start_str, forward_day = 15)
        
        # leave columns in String_typeCols_List as they were, cast other columns to numercial(double)
        df_kpis = convert_string_numerical(df_kpis, String_typeCols_List)

        #rows_set = capture_eventday(df_kpis)
        remain_enodeb_set = get_event_list(df_kpis)
        
        # only focus on most recent maintenance just before date_start
        distinct_df = get_enodeb_date(df_kpis, remain_enodeb_set)
        
        from datetime import datetime 
        # Parse the original date string '%Y-%m-%d' to '%m/%d/%Y, to match df_kpis 'DAY' format
        #original_date = datetime.strptime(date_start_str, '%Y-%m-%d') 
        formatted_date_string = date_start.strftime('%m/%d/%Y') 
        print(formatted_date_string, date_start)
        
        new_event_enodeb = distinct_df.filter( col("event_date_final")==date_start_str )
        
        # get 14 days features before maintenance
        new_event_enodeb_features = get_enodeb_features(new_event_enodeb, df_kpis)
        new_event_enodeb_features =new_event_enodeb_features.filter( col('DAY')!=formatted_date_string )
        #print("Total number of enodeb maintained", distinct_df.count())
        #print("Number of enodeb recently maintained at {}:".format(date_start), new_event_enodeb.count())
        #print("Number of days ",new_event_enodeb_features.count())
        
        # add-----------------------------------------------------------------------------------------------
        exclude_columns = SEA_list
        selected_columns = [column for column in new_event_enodeb_features.columns if column not in exclude_columns]
        selected_columns.remove('enodeb_event')
        df_output = new_event_enodeb_features.select(selected_columns).fillna(0)
        df_output = rename_FSM_features(df_output)
        df_output =df_output.withColumn('DAY',  F.from_unixtime(F.unix_timestamp('DAY', 'MM/dd/yyy'))  ).withColumn('DAY', F.date_format('DAY', 'yyyy-MM-dd')) 
        df_output = lower_case_col_names( df_output, String_typeCols_List)
        df_output = fill_allday_zero_with_NA(df_output, features_list = ['context_drop_rate', 'bearer_drop_voice_rate', 'bearer_setup_failure_rate', 'avgconnected_users', 'dl_data_volume', 'uptp_user_perceived_throughput', 'packet_retransmission_rate', 'packet_loss_rate', 'rrc_setup_failure', 'bearer_drop_rate'] )
        df_output = df_output.repartition(1)
        df_output_path = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/user/ZheS/Daily_KPI_14_days_pre_Event/Daily_KPI_14_days_pre_{}_event.csv".format(str(date_start))
        df_output.write.csv(df_output_path, header=True, mode="overwrite") 
        # --------------------------------------------------------------------------------------------------
        count_days_enodeb = new_event_enodeb_features.groupBy('ENODEB').agg(countDistinct("DAY")).orderBy('count(DAY)')
        
        # this list contains the most recent maintenance just before date_start
        maintain_enodeb_list = count_days_enodeb.select('ENODEB').rdd.map(lambda row: row[0]).collect()
        enodeb_maintained_dict ={}
        if date_start not in enodeb_maintained_dict:
            enodeb_maintained_dict[str(date_start)] = maintain_enodeb_list
        else:
            print("duplicated day")
        
        # Base file path 
        file_path = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000//user/ZheS/Event_Enodeb_List_Date/Event_Enodeb_List.json" 
        
        # Concatenate date_string to file_path 
        result_path = file_path.replace(".json", "_{}.json".format(str(date_start))) 
        
        try:
            spark_df = spark.createDataFrame([enodeb_maintained_dict])
            spark_df.write.mode('overwrite').json(result_path)
            #print(result_path)
        except Exception as e:
            print("error:{}".format(e))
            print("no event at {}".format(date_td))
        
        event_enodeb_avg_features = enodeb_mean(new_event_enodeb_features, FSM_SIU_list)
        event_enodeb_std_features  = enodeb_stddev(new_event_enodeb_features, FSM_SIU_list)
        event_enodeb_features = event_enodeb_avg_features.join(event_enodeb_std_features, on = "ENODEB", how = "inner")
        
        # calculate average of 14 days pre feature values and standard deviation of features
        output_path = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000//user/ZheS/Event_Enodeb_Pre_Feature_Date/Event_Enodeb_Pre_{}.csv".format(str(date_start))             
        event_enodeb_features.write.csv(output_path, header=True, mode="overwrite") 
        #print("")