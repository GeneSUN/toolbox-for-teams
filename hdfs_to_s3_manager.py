from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import subprocess
import traceback
import os

class HDFS2S3Manager:
    """
    Class to handle HDFS file operations and push files from HDFS to S3.
    """

    def __init__(self, spark=None):
        if spark is None:
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = spark

    @staticmethod
    def rename_hdfs_file(source_path, target_path):
        """
        Rename a file in HDFS.
        """
        try:
            mv_cmd = f"hdfs dfs -mv {source_path} {target_path}"
            process = subprocess.Popen(mv_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                print(f"Error renaming file in HDFS: {stderr.decode('utf-8')}")
            else:
                print(f"File renamed successfully from {source_path} to {target_path}")
        except Exception as e:
            print(f"Exception occurred: {str(e)}")

    @staticmethod
    def check_hdfs_files(path, filename): 
        """
        Check if a file exists in HDFS.
        """
        ls_proc = subprocess.Popen(
            ['hdfs', 'dfs', '-du', path], 
            stdout=subprocess.PIPE
        ) 
        ls_proc.wait()
        ls_lines = ls_proc.stdout.readlines()
        all_files = [line.split()[-1].decode("utf-8").split('/')[-1] for line in ls_lines]
        return filename in all_files

    @staticmethod
    def check_aws_files(path, filename): 
        """
        Check if a file exists in S3.
        """
        path_adj = "s3" + path[3:] 
        try: 
            ls_proc = subprocess.Popen(
                ['aws', 's3', 'ls', path_adj], 
                stdout=subprocess.PIPE
            ) 
            ls_proc.wait()
            ls_lines = ls_proc.stdout.readlines() 
            all_files = [line.split()[-1].decode("utf-8").split('/')[0] for line in ls_lines]
            return filename in all_files
        except Exception as e: 
            print(str(traceback.format_exc()) + "\n" + str(e) + "\n")
            return False

    @staticmethod
    def push_hdfs_to_s3(hdfs_path, hdfs_file, s3_path, s3_file):
        """
        Push a file from HDFS to S3 if it doesn't already exist in S3.
        """
        # Construct full HDFS path
        full_hdfs_path = f"{hdfs_path.rstrip('/')}/{hdfs_file.lstrip('/')}"
        base_cmd = (
            "hadoop distcp "
            "-Dfs.s3a.access.key=AKIAQDQI7W3D5XKRKXRF "
            "-Dfs.s3a.secret.key=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 "
            "-Dfs.s3a.fast.upload=true -update -bandwidth 10 -m 20 -strategy dynamic "
        )

        if not HDFS2S3Manager.check_hdfs_files(hdfs_path, hdfs_file):
            print(f"{full_hdfs_path} doesn't exist")
            return

        if HDFS2S3Manager.check_aws_files(s3_path, s3_file):
            print(f"File {s3_file} already exists in S3")
            return

        full_cmd = f"{base_cmd} {full_hdfs_path}/* {s3_path.rstrip('/')}/{s3_file}/"
        try:
            process = subprocess.Popen(full_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                print(f"Got an error running {full_cmd} {stderr.decode('utf-8')}")
            else:
                print(f"Uploaded {s3_file} successfully")
        except Exception as e:
            print(f"Exception occurred: {str(e)}")

    def filter_and_write_csv(self, parquet_file, output_path, models):
        """
        Read Parquet file, filter by models, write as single CSV to output_path.
        """
        self.spark.read.parquet(parquet_file)\
            .filter(F.col("model_name").isin(models))\
            .coalesce(1).write.csv(output_path, header=True, mode="overwrite")

    @staticmethod
    def get_csv_file(output_path):
        """
        Find the CSV file in output_path and remove _SUCCESS file if exists.
        """
        result = subprocess.run(["hdfs", "dfs", "-ls", output_path], capture_output=True, text=True)
        file_list = result.stdout.splitlines()
        csv_file, success_file = None, None
        for line in file_list:
            if line.endswith(".csv"):
                csv_file = line.split()[-1]
            elif line.endswith("_SUCCESS"):
                success_file = line.split()[-1]
        if success_file:
            subprocess.run(["hdfs", "dfs", "-rm", success_file])
        print(f"CSV file: {csv_file}")
        return csv_file

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_wifiscore_Test').config("spark.ui.port","24045").getOrCreate()

    hdfs_pd = ""
    date = "2024-10-08"
    date_compact = "20241008"

    parquet_file = hdfs_pd + f"/user/ZheS/wifi_score_v4/KPI/{date}"
    output_path = hdfs_pd + f"/user/ZheS/wifi_score_v4/aws/{date_compact}"
    target_path = hdfs_pd + f"/user/ZheS/wifi_score_v4/aws/{date_compact}/wifiscore.csv"

    hdfs_path = hdfs_pd + "/user/ZheS/wifi_score_v4/aws/"
    hdfs_file = f"{date_compact}"
    s3_path = ""
    s3_file = f"{date_compact}"

    models = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'WNC-CR200A', "CR1000A", "CR1000B"]

    manager = HDFS2S3Manager(spark=spark)
    manager.filter_and_write_csv(parquet_file, output_path, models)

    csv_file = manager.get_csv_file(output_path)
    manager.rename_hdfs_file(csv_file, target_path)

    print(manager.check_hdfs_files(hdfs_path, hdfs_file))
    print(manager.check_aws_files(s3_path, s3_file))

    manager.push_hdfs_to_s3(hdfs_path, hdfs_file, s3_path, s3_file)
    print(manager.check_aws_files(s3_path, s3_file))
