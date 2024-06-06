from pyspark.sql import SparkSession 
from functools import reduce
from datetime import datetime, timedelta, date

class HDFSFileAggregator: 
    def __init__(self, file_path_pattern, date_strings): 
        """ 
        Initializes the HDFSFileAggregator with a file path pattern and a list of date strings. 
        :param file_path_pattern: A string representing the pattern for file paths. 
        :param date_strings: A list of strings representing dates formatted according to the expected pattern. 
        """ 
        self.file_paths = [file_path_pattern.format(date) for date in date_strings] 
        self.df = self.aggregate_files() 

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



if __name__ == "__main__":

    spark = SparkSession.builder\
                        .appName('ZheS_TrueCall_enb')\
                        .config("spark.sql.adapative.enabled","true")\
                        .getOrCreate()
    file_path_pattern = "/user/ZheS/5g_homeScore/final_score/{}" 
    date_strings = [ ( date.today() - timedelta(i) ).strftime('%Y-%m-%d') for i in range(2,4)]
    hdfs_aggregator = HDFSFileAggregator(file_path_pattern, date_strings) 

    if hdfs_aggregator.df is not None: 

        hdfs_aggregator.df.show() 

    class CustomHDFSFileAggregator(HDFSFileAggregator): 
        def load_file(self, file_path): 
            df = self.spark.read.options(header=True, inferSchema=True).csv(file_path) 
            df = df.filter(df['someColumn'] > 0)  # Example processing step 

            return df 