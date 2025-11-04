
from pyspark.sql import functions as F 
from pyspark.sql.functions import concat, lit, col , struct, to_json
from pyspark.sql import SparkSession 
from pulsar import Client, AuthenticationTLS, ConsumerType, InitialPosition

from datetime import datetime, timedelta, date 
import argparse 
import json

from hdfs import InsecureClient 
from pyspark.sql import SparkSession 

from hdfs import InsecureClient 
import os

class PulsarJob:

    def __init__(self, pulsar_topic ,vmb_host, cetpath, keypath, capath,dir_files=None, hdfs_location= None): 
        
        self.pulsar_topic = pulsar_topic
        self.vmb_host = vmb_host
        self.cetpath = cetpath 
        self.keypath = keypath 
        self.capath = capath 
        self.consumer = None 
        self.dir_files = dir_files
        self.hdfs_location = hdfs_location
        #self.archive_path  = '/'.join(self.dir_files.split('/') [:-1])  + '/archive/' +self.dir_files.split('/')[-1]
        self.client = self.setup_client()

    def setup_client(self, vmb_host=None, capath=None, cetpath=None, keypath=None):
        if vmb_host is None:
            vmb_host = self.vmb_host
        if capath is None:
            capath = self.capath
        if cetpath is None:
            cetpath = self.cetpath
        if keypath is None:
            keypath = self.keypath
        
        client = Client(vmb_host,
                        tls_trust_certs_file_path = capath,
                        tls_allow_insecure_connection = False,
                        authentication = AuthenticationTLS(cetpath, keypath) ,
                        operation_timeout_seconds=3000)
        return client
        
    def setup_producer(self, hdfs_location=None, pulsar_topic=None, dir_files =None, client =None):
        if hdfs_location is None:
            hdfs_location = self.hdfs_location
        if pulsar_topic is None:
            pulsar_topic = self.pulsar_topic
        if dir_files is None:
            dir_files = self.dir_files
        if client is None:
            client = self.client
        
        producer = client.create_producer( pulsar_topic,
                                block_if_queue_full=True,
                                batching_enabled=True,
                                batching_max_publish_delay_ms=120000,
                                send_timeout_millis=3000000,
                                max_pending_messages=5000)
        hdfs_client = InsecureClient(hdfs_location)

        try:
            with hdfs_client.read(dir_files ) as reader:
                producer.send(reader.read())
        except:
            print("failure")

        producer.close()
        client.close()

    def setup_consumer(self, client= None, pulsar_topic=None):
        if pulsar_topic is None:
            pulsar_topic = self.pulsar_topic
        if client is None:
            client = self.client
            
        consumer = client.subscribe(pulsar_topic, 'vmas_test_zhes',consumer_type = ConsumerType.Shared)

        msg = consumer.receive()
        try:
            parsed = msg.data()
            data=json.loads(parsed)
            print(data)
        except Exception as e:
            consumer.negative_acknowledge(msg)
            print(e)
        
        consumer.close()
        client.close()

        return data
    
    def move_file_to_archive(self, current_path=None, archive_path=None,hdfs_location =None): 

        if current_path is None:
            current_path = self.dir_files
        if archive_path is None:
            archive_path =  '/'.join(self.dir_files.split('/') [:-1])  + '/archive/' +self.dir_files.split('/')[-1]
        if hdfs_location is None:
            hdfs_location = self.hdfs_location

        hdfs_client = InsecureClient(hdfs_location)

        try: 
            hdfs_client.rename(current_path, archive_path) 
            if not hdfs_client.status(current_path, strict=False): 
                print(f"File has been moved to the archive directory: {archive_path}") 
                return f"File has been moved to the archive directory: {archive_path}"
            else: 
                print("Error: File move failed.") 
                return "Error: File move failed."
        except Exception as e: 
            print(f"Error moving file: {e}") 
            return f"Error moving file: {e}"


class SparkToPulsar: 

    def __init__(self, file_path, pulsar_topic, vmb_host): 
        self.spark = SparkSession.builder.appName('VMB-wifi-score').getOrCreate()
        self.file_path = file_path 
        self.pulsar_topic = pulsar_topic 
        self.vmb_host = vmb_host 
        self.main_path = "/usr/apps/vmas/cert/cktv/"
        self.cert_path = self.main_path + "cktv.cert.pem"
        self.key_path = self.main_path + "cktv.key-pk8.pem"
        self.ca_path = self.main_path + "ca.cert.pem"

    def read_data(self): 
        
        df = self.spark.read.parquet(self.file_path) 
        return df 
 
    def process_data(self, df): 

        return df 

    def write_data(self, df): 

        df.write.format("pulsar")\
            .option("service.url", self.vmb_host)\
            .option("pulsar.client.authPluginClassName","org.apache.pulsar.client.impl.auth.AuthenticationTls")\
            .option("pulsar.client.authParams",f"tlsCertFile:{self.cert_path},tlsKeyFile:{self.key_path}")\
            .option("pulsar.client.tlsTrustCertsFilePath",self.ca_path)\
            .option("pulsar.client.useTls","true")\
            .option("pulsar.client.tlsAllowInsecureConnection","false")\
            .option("pulsar.client.tlsHostnameVerificationenable","false")\
            .option("topic", self.pulsar_topic)\
            .save()

    def run(self): 
        df = self.read_data()
        df = self.process_data(df) 
        df.show()
        self.write_data(df) 

    def consume_data(self):
        from Pulsar_Class import PulsarJob
        job_nonprod = PulsarJob( self.pulsar_topic ,
                                    self.vmb_host, 
                                    self.cert_path , 
                                    self.key_path, 
                                    self.ca_path
                                )
        data = job_nonprod.setup_consumer()

        return data

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('Casual Test').enableHiveSupport().getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")

#----------------------------------------------------------------------------------------------------------------------------

    hdfs_location = "http://njbbvmaspd11.nss.vzwnet.com:9870"
    
    # rename this file ------------------------------------
    dir_files = "/user/ZheS/SNAP_Enodeb/VMB/json_abnormal_enodeb-2023-11-27.json"
    #--------------------------------------------------------

    pulsar_topic = "persistent://cktv/post-snap-maintenance-alert/VMAS-Post-SNAP-Maintenance-Alert"
    vmbHost_np = os.getenv('VMB_EAST_NONPROD',"pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/")
    cetpath = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/cktv.cert.pem"
    keypath = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/cktv.key-pk8.pem"
    capath = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/ca.cert.pem"

    job1 = PulsarJob( pulsar_topic ,
                        vmbHost_np, 
                        cetpath, 
                        keypath, 
                        capath,
                        dir_files, 
                        hdfs_location
                    )
    #job1.setup_producer()
    #job1.setup_consumer()
    #job1.move_file_to_archive()
