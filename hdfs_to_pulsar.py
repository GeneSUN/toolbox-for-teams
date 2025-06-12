from pulsar import Client, AuthenticationTLS
from hdfs import InsecureClient
import os

class HDFSFileToPulsar:
    """
    Upload a file from HDFS to a Pulsar topic using TLS authentication.
    """

    def __init__(
        self, 
        pulsar_topic, 
        pulsar_url, 
        cert_path, 
        key_path, 
        ca_path, 
        hdfs_url, 
        hdfs_file
    ):
        self.pulsar_topic = pulsar_topic
        self.pulsar_url = pulsar_url
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_path = ca_path
        self.hdfs_url = hdfs_url
        self.hdfs_file = hdfs_file

    def upload(self):
        """
        Reads the specified HDFS file and sends its content to the Pulsar topic.
        """
        # Setup HDFS client
        hdfs_client = InsecureClient(self.hdfs_url)
        # Read file from HDFS
        try:
            with hdfs_client.read(self.hdfs_file) as reader:
                file_data = reader.read()
        except Exception as e:
            print(f"[ERROR] Reading file from HDFS: {e}")
            return False

        # Setup Pulsar client with TLS
        try:
            client = Client(
                self.pulsar_url,
                tls_trust_certs_file_path=self.ca_path,
                tls_allow_insecure_connection=False,
                authentication=AuthenticationTLS(self.cert_path, self.key_path),
                operation_timeout_seconds=3000
            )
            producer = client.create_producer(
                self.pulsar_topic,
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=120000,
                send_timeout_millis=3000000,
                max_pending_messages=5000
            )
        except Exception as e:
            print(f"[ERROR] Connecting to Pulsar: {e}")
            return False

        # Send the file content
        try:
            producer.send(file_data)
            print(f"[SUCCESS] Uploaded {self.hdfs_file} to topic {self.pulsar_topic}")
        except Exception as e:
            print(f"[ERROR] Sending file to Pulsar: {e}")
            return False
        finally:
            producer.close()
            client.close()
        return True

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

# --- Usage Example ---

if __name__ == "__main__":
    pulsar_topic = "persistent://cktv/post-snap-maintenance-alert/VMAS-Post-SNAP-Maintenance-Alert"
    pulsar_url = os.getenv(
        "VMB_EAST_NONPROD",
        "pulsar+ssl://vmb-aws-us-east-1-nonprod.verizon.com:6651/"
    )
    cert_path = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/cktv.cert.pem"
    key_path = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/cktv.key-pk8.pem"
    ca_path = "/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/ca.cert.pem"
    hdfs_url = "http://njbbvmaspd11.nss.vzwnet.com:9870"
    hdfs_file = "/user/ZheS/SNAP_Enodeb/VMB/json_abnormal_enodeb-2023-11-27.json"

    uploader = HDFSFileToPulsar(
        pulsar_topic=pulsar_topic,
        pulsar_url=pulsar_url,
        cert_path=cert_path,
        key_path=key_path,
        ca_path=ca_path,
        hdfs_url=hdfs_url,
        hdfs_file=hdfs_file
    )
    uploader.upload()
