from pyspark.sql import SparkSession 
from functools import reduce
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F 
from pyspark.sql.functions import ( 
    abs, avg, broadcast, count, col, concat_ws, countDistinct, desc, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round, sum, to_date, udf, when, 
) 

class HDFSFileAggregator: 
    def __init__(self, file_path_pattern, date_strings): 
        """ 
        Initializes the HDFSFileAggregator with a file path pattern and a list of date strings. 
        :param file_path_pattern: A string representing the pattern for file paths. 
        :param date_strings: A list of strings representing dates formatted according to the expected pattern. 
        """ 
        self.file_paths = [file_path_pattern.format(date) for date in date_strings] 
        #self.df = self.aggregate_files() 

    def read_and_process_file(self, file_path): 
        """ 
        Reads a file and processes it if necessary. 
        :param file_path: Full path to the file. 
        :return: DataFrame loaded from the file, or None if an error occurs. 
        """ 
        try: 
            df = self.load_file(file_path) 
            return df 
        except Exception as e: 
            print(f"Failed to read {file_path}: {e}") 
            return None 
 
    def load_file(self, file_path): 
        """ 
        Loads a parquet file. Can be modified to load other types of files or include processing. 
        :param file_path: Full path to the parquet file. 
        :return: DataFrame loaded from the file. 
        """ 
        df = spark.read.parquet(file_path) 
        return df
 
    def aggregate_files(self): 

        """ 
        Aggregates data from multiple files into a single DataFrame. 
        :return: A DataFrame containing all aggregated data. 
        """ 

        df_list = [self.read_and_process_file(path) for path in self.file_paths] 
        df_list = list(filter(None, df_list))  # Remove None entries due to failed reads 
        if df_list: 
            return reduce(lambda df1, df2: df1.union(df2), df_list) 
        return None 

class CSVFileAggregator(HDFSFileAggregator): 

    def load_file(self, file_path): 
        """ 
        Specialized loader for CSV files with predefined options. 
        """ 
        options = {'header': True, 'inferSchema': True} 
        return self.spark.read.options(**options).csv(file_path) 

class daysFeature():
    global hdfs_pd
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    def __init__(self, date_val):
        self.date_val = date_val
        self.date_str = date_val.strftime('%Y-%m-%d') 
        self.main_path = hdfs_pd + f"/user/ZheS/wifi_score_v3/installExtenders/{self.date_str}"
        self.feature_path = hdfs_pd + "/user/ZheS/wifi_score_v3/homeScore_dataframe/{}"

        self.df_main = spark.read.parquet(self.main_path)\
                        .select( "serial_num","date_string","before_home_score","after_home_score" )

    def read_single_day_feafure(self,d_str):
        routers = ["G3100","CR1000A","XCI55AX","ASK-NCQ1338FA","CR1000B","CR1000B","WNC-CR200A","ASK-NCQ1338","FSNO21VA","ASK-NCQ1338E"]
        other_routers = ["FWA55V5L","FWF100V5L","ASK-NCM1100E","ASK-NCM1100"]
        df_features = spark.read.parquet(hdfs_pd + f"/user/ZheS/wifi_score_v3/homeScore_dataframe/{d_str}")\
                .filter( col("dg_model_indiv").isin(routers+other_routers) )\
                .drop("mdn","cust_id","dg_model","Rou_Ext","date","dg_model_indiv")\
                .dropDuplicates()

        for c in ["num_station","poor_rssi","poor_phyrate","home_score"]:
            df_features = df_features.withColumnRenamed( c, c + f"_{d_str}" )

    def read_days_feature(self, date_val):
        date_val = self.date_val
        df_main = self.df_main

        date_range = [ ( date_val - timedelta(i) ).strftime('%Y-%m-%d') for i in range(1,8) ]
        dfs = list(map(self.read_single_day_feafure(), date_range)) 
        dfs= list(filter(None, dfs)) 
        result_df = reduce(lambda df1, df2: df1.join(df2,"serial_num"), dfs) 

        return df_main.join(result_df,"serial_num")
    

if __name__ == "__main__":

    spark = SparkSession.builder\
                        .appName('ZheS_TrueCall_enb')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    file_path_pattern = "/user/ZheS/5g_homeScore/final_score/{}" 
    date_strings = [ ( date.today() - timedelta(i) ).strftime('%Y-%m-%d') for i in range(2,4)]
    hdfs_aggregator = HDFSFileAggregator(file_path_pattern, date_strings) 
    hdfs_aggregator.aggregate_files().show()

    def custom_load_file(self, file_path): 
        df = spark.read.parquet(file_path) 
        return df.select("sn")

    import types
    hdfs_aggregator.load_file = types.MethodType(custom_load_file,hdfs_aggregator)
    hdfs_aggregator.aggregate_files().show()