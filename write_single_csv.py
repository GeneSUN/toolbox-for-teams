
import subprocess
import os
from pyspark.sql import DataFrame

def write_single_csv(df: DataFrame, output_path: str, filename: str, header: bool = True, mode: str = "overwrite"):
    """
    Write a PySpark DataFrame as a single CSV file to HDFS with a custom filename.

    Parameters
    ----------
    df : DataFrame
        PySpark DataFrame to write
    output_path : str
        HDFS directory where the file should be placed
    filename : str
        Desired CSV filename (e.g., 'mydata.csv')
    header : bool
        Include header in the CSV (default: True)
    mode : str
        Save mode: 'overwrite', 'append', etc. (default: overwrite)
    """
    temp_path = os.path.join(output_path, "_temp_single_csv")

    # Step 1: Write DataFrame into temporary folder as one part file
    (
        df.coalesce(1)
          .write
          .mode(mode)
          .option("header", str(header).lower())
          .csv(temp_path)
    )

    # Step 2: Find the part file inside the temporary folder
    ls_out = subprocess.check_output(["hdfs", "dfs", "-ls", temp_path]).decode("utf-8")
    part_file = [line.split()[-1] for line in ls_out.strip().split("\n") if "part-" in line][0]

    # Step 3: Move and rename to final filename
    final_path = os.path.join(output_path, filename)
    subprocess.check_call(["hdfs", "dfs", "-mv", part_file, final_path])

    # Step 4: Clean up temp folder
    subprocess.check_call(["hdfs", "dfs", "-rm", "-r", temp_path])

    print(f"✅ DataFrame written to {final_path}")

# write_single_csv( df_cap_hour_pd,  "/user/ZheS/upload_data", "df_cap_hour_pd")
