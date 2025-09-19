# =============================
# Standard Python Libraries
# =============================
import time
from datetime import date, timedelta, datetime
from functools import reduce
from typing import List, Tuple
import sys
# =============================
# Third-Party Libraries
# =============================
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.neighbors import KernelDensity
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from statsmodels.tsa.arima.model import ARIMA
# =============================
# PySpark Libraries
# =============================
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lag, last, lit, row_number, when
)
from pyspark.sql.types import (
    BooleanType, DoubleType, FloatType, StringType,
    StructField, StructType, TimestampType
)
from pyspark.sql.window import Window


# =============================
# Config
# =============================
hdfs_namenode = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
base_dir = "/user/kovvuve/owl_history_v3/date="
TIME_COL = "time"
feature_groups = {
    "signal_quality": ["4GRSRP", "4GRSRQ", "SNR", "4GSignal", "BRSRP", "RSRQ", "5GSNR", "CQI"],
    "throughput_data": [
        "LTEPDSCHPeakThroughput", "LTEPDSCHThroughput",
        "LTEPUSCHPeakThroughput", "LTEPUSCHThroughput",
        "TxPDCPBytes", "RxPDCPBytes",
        "TotalBytesReceived", "TotalBytesSent",
        "TotalPacketReceived", "TotalPacketSent"
    ],
}
ALL_FEATURES = feature_groups["signal_quality"] + feature_groups["throughput_data"]


# ======================================================================
# Classes / Functions (kept identical in content; only ordering changed)
# ======================================================================
from datetime import datetime, timedelta
from pyspark.sql import functions as F, types as T
from pyspark.sql.utils import AnalysisException

from pyspark.sql.utils import AnalysisException

def hdfs_hour_path_exists( dt, base_path = None):
    """
    Build an HDFS output path for a given datetime and check if it exists.
    """
    if base_path is None:
        base_path = "/user/ZheS/owl_anomaly/autoencoder/data"
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("hr=%H")
    out_path = f"{base_path}/{date_str}/{hour_str}"

    try:
        spark.read.parquet(out_path).limit(1)
        return out_path, True
    except AnalysisException:
        return out_path, False

   
def convert_string_numerical(
    df: DataFrame,
    cols_to_cast: List[str],
    decimal_places: int = 2
) -> DataFrame:
    """
    Casts selected columns to DoubleType and rounds them to a specified
    number of decimal places.

    Args:
        df: The input PySpark DataFrame.
        cols_to_cast: A list of column names to cast and round.
        decimal_places: The number of decimal places to round to. Defaults to 2.

    Returns:
        A new DataFrame with the specified columns cast and rounded.
    """
    for c in cols_to_cast:
        if c in df.columns:

            df = df.withColumn(
                c,
                F.round(F.col(c).cast(DoubleType()), decimal_places)
            )
    return df


class HourlyIncrementProcessor:
    """
    Steps:
      - 'hourly' : hourly averages (processed columns) + carry-through of ALL other columns via FIRST()
      - 'incr'   : replace processed features with smoothed increments
      - 'log'    : replace processed features with log1p(increments)
      - 'fill'   : forward-fill zeros in the (current) processed feature columns
    Notes:
      • All columns not listed in `columns` are preserved. During the 'hourly' aggregation,
        they are reduced with FIRST(ignorenulls=True) within each (partition_cols, time_col) group.
        Adjust that policy if you prefer MIN/MAX/LAST/etc.
    """

    def __init__(self, df: DataFrame, columns: List[str], partition_col=("sn",), time_col: str = TIME_COL):
        self.df = df
        self.columns = list(columns)
        self.partition_cols = list(partition_col) if isinstance(partition_col, (list, tuple)) else [partition_col]
        self.time_col = time_col

        # identify carry-through columns (everything except keys + processed columns)
        key_cols = set(self.partition_cols + [self.time_col])
        self.other_cols = [c for c in self.df.columns if c not in self.columns and c not in key_cols]

        self.df_hourly = None
        self._done = set()

    def compute_hourly_average(self):
        # averages for processed columns
        agg_proc = [F.round(F.avg(c), 2).alias(c) for c in self.columns]
        # carry-through for all remaining columns (FIRST non-null within the hour)
        agg_other = [F.first(col(c), ignorenulls=True).alias(c) for c in self.other_cols]

        self.df_hourly = (
            self.df
            .groupBy(*self.partition_cols, self.time_col)
            .agg(*agg_proc, *agg_other)
        )
        self._done.add("hourly")

    def compute_increments(self, partition_cols=None, order_col=None):
        if "hourly" not in self._done:
            self.compute_hourly_average()

        partition_cols = self.partition_cols if partition_cols is None else partition_cols
        order_col = self.time_col if order_col is None else order_col
        w = Window.partitionBy(*partition_cols).orderBy(order_col)

        for c in self.columns:
            prev = lag(col(c), 1).over(w)
            raw_incr = col(c) - prev
            prev_incr = lag(raw_incr, 1).over(w)

            incr = when(col(c) < prev, when(prev_incr.isNotNull(), prev_incr).otherwise(lit(0))).otherwise(raw_incr)
            prev_smooth = lag(incr, 1).over(w)
            smoothed = when(incr < 0, prev_smooth).otherwise(incr)
            self.df_hourly = self.df_hourly.withColumn(c, F.round(smoothed, 2))

        # drop rows where increments are null (first row per partition)
        self.df_hourly = self.df_hourly.na.drop(subset=self.columns)
        self._done.add("incr")

    def apply_log_transform(self):
        if "incr" not in self._done:
            self.compute_increments()

        for c in self.columns:
            self.df_hourly = self.df_hourly.withColumn(
                c, F.round(F.log1p(F.when(col(c) < 0, lit(0)).otherwise(col(c))), 2)
            )
        self._done.add("log")

    def fill_zero_with_previous(self, partition_cols=None, order_col=None):
        if "log" not in self._done and "incr" not in self._done and "hourly" not in self._done:
            self.compute_hourly_average()

        partition_cols = self.partition_cols if partition_cols is None else partition_cols
        order_col = self.time_col if order_col is None else order_col

        w_ffill = (
            Window.partitionBy(*partition_cols)
            .orderBy(order_col)
            .rowsBetween(Window.unboundedPreceding, 0)
        )

        for c in self.columns:
            tmp = f"__{c}_nz"
            self.df_hourly = (
                self.df_hourly
                .withColumn(tmp, when(col(c) != 0, col(c)))
                .withColumn(c, last(tmp, ignorenulls=True).over(w_ffill))
                .drop(tmp)
            )
        self._done.add("fill")

    def run(self, steps=("hourly", "incr", "log", "fill")):
        wanted = list(steps)
        for step in wanted:
            if step == "incr" and "hourly" not in self._done:
                self.compute_hourly_average()
            if step == "log" and "incr" not in self._done:
                self.compute_increments()
            if step == "fill" and not ({"hourly", "incr", "log"} & self._done):
                self.compute_hourly_average()

            if step == "hourly" and "hourly" not in self._done:
                self.compute_hourly_average()
            elif step == "incr" and "incr" not in self._done:
                self.compute_increments()
            elif step == "log" and "log" not in self._done:
                self.apply_log_transform()
            elif step == "fill" and "fill" not in self._done:
                self.fill_zero_with_previous()
        return self


def forward_fill(df: DataFrame, cols_to_process: List[str], partition_col: str, order_col: str) -> DataFrame:
    """
    Performs a forward fill on specified columns of a PySpark DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        cols_to_process (List[str]): A list of column names to apply forward fill.
        partition_col (str): The column to partition the window by (e.g., 'sn').
        order_col (str): The column to order the window by (e.g., 'time').

    Returns:
        DataFrame: The DataFrame with specified columns forward-filled.
    """
    window_spec = Window.partitionBy(partition_col).orderBy(order_col) \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # Calculate the mean of each column to use as a fallback value
    mean_values = df.select([F.mean(col_name).alias(f"mean_{col_name}") for col_name in cols_to_process]).first()
    for col_name in cols_to_process:
        # Step 1: Replace 0 with nulls
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == 0, F.lit(None)).otherwise(F.col(col_name))
        )
        # Step 2: Forward fill the nulls
        df = df.withColumn(
            col_name,
            F.last(F.col(col_name), ignorenulls=True).over(window_spec)
        )
        # Step 3: Fill any remaining nulls (e.g., at the beginning of the partition) with the mean
        if mean_values is not None and mean_values[f"mean_{col_name}"] is not None:
            df = df.fillna({col_name: mean_values[f"mean_{col_name}"]})
    return df

# --- schema with new slice_id ---
out_schema = T.StructType([
    T.StructField("sn", T.StringType(), False),
    T.StructField("time", T.TimestampType(), False),
    T.StructField("feature", T.StringType(), False),
    T.StructField("value", T.StringType(), True),
    T.StructField("slice_id", T.StringType(), False),
])

def _latest_24_unpivot_with_slice(pdf):
    n = 24
    import pandas as pd

    # if group has fewer than n rows → skip
    if len(pdf) < n:
        return pd.DataFrame(columns=["sn", "time", "feature", "value", "slice_id"])

    # sort by time desc, take last n
    pdf = pdf.sort_values("time", ascending=False).head(n)

    # unpivot
    long_pdf = pd.melt(
        pdf,
        id_vars=["sn", "time"],
        value_vars=[c for c in pdf.columns if c not in ["sn", "time"]],
        var_name="feature",
        value_name="value"
    ).dropna(subset=["value"])

    # compute slice_id = sn + "_" + last_time
    last_time = pdf["time"].max()
    slice_id = f"{pdf['sn'].iloc[0]}_{last_time.strftime('%Y%m%d%H%M%S')}"
    long_pdf["slice_id"] = slice_id

    # keep value as string (safe for huge counters)
    long_pdf["value"] = long_pdf["value"].astype(str)

    return long_pdf[["sn", "time", "feature", "value", "slice_id"]]


class LatestUnpivotProcessor:
    def __init__(self, hdfs_namenode, base_dir, datetime_now, all_features, time_col="time"):
        self.hdfs_namenode = hdfs_namenode
        self.base_dir = base_dir
        self.datetime_now = datetime_now
        self.all_features = all_features
        self.time_col = time_col

        # define output schema once
        self.out_schema = T.StructType([
            T.StructField("sn", T.StringType(), False),
            T.StructField("time", T.TimestampType(), False),
            T.StructField("feature", T.StringType(), False),
            T.StructField("value", T.StringType(), True),
            T.StructField("slice_id", T.StringType(), False),
        ])


    def build_file_paths(self, hours_back=6):
        """Build HDFS paths for last `hours_back` hours"""
        now = self.datetime_now
        paths = []
        for i in range(hours_back):
            dt = now - timedelta(hours=i)
            date_str = dt.strftime("%Y-%m-%d")
            hour_str = dt.strftime("hr=%H")
            paths.append(f"{self.hdfs_namenode}/{self.base_dir}{date_str}/{hour_str}")
        return paths

    def read_data(self, file_paths):
        """Read raw data and basic preprocessing"""
        df = (
            spark.read.option("header", "true").csv(file_paths)
            .withColumnRenamed("mdn", "MDN")
            .withColumn("MDN", F.regexp_replace(F.col("MDN"), '"', ''))
            .withColumn(self.time_col, F.from_unixtime(F.col("ts") / 1000.0).cast("timestamp"))
            .filter(col("ModelName") == "ASK-NCM1100E") 
            .select(["sn", "MDN", self.time_col] + self.all_features)
            .dropDuplicates()
        )

        df = convert_string_numerical(df, ALL_FEATURES)

        zero_list = ["RSRQ", "4GRSRQ", "4GRSRP", "BRSRP"]
        df = forward_fill(df, zero_list, "sn", "time")
        df = df.orderBy("sn", "time")

        # Throughput features → hourly increments pipeline
        proc = (
            HourlyIncrementProcessor(df, feature_groups["throughput_data"], partition_col=["sn"])
            .run(steps=('incr', 'log', 'fill'))
        )
        df = proc.df_hourly

        return df

    def transform(self, df):
        """Apply groupby + unpivot"""
        return (
            df.select(["sn", "time"] + self.all_features)
              .groupby("sn")
              .applyInPandas(_latest_24_unpivot_with_slice, schema=self.out_schema)
        )

    def write_data(self, df_long, output_base ="/user/ZheS/owl_anomaly/autoencoder/data/", dt=None):
        """Write parquet output by date/hour"""
        if dt is None:
            dt = self.datetime_now
        date_str = dt.strftime("%Y-%m-%d")
        hour_str = dt.strftime("hr=%H")
        out_path = f"{output_base}/{date_str}/{hour_str}"
        df_long.write.mode("overwrite").parquet(out_path)
        return out_path

    def run(self,  hours_back=6, sn_filter=None):
        """Full pipeline: read → transform → write"""
        paths = self.build_file_paths(hours_back)
        df_raw = self.read_data(paths)

        if sn_filter:
            df_raw = df_raw.filter(F.col("sn").isin(sn_filter))

        df_long = self.transform(df_raw)
        out_path = self.write_data(df_long)
        return out_path
 
# =============================
# UDF Schema + Pandas UDF
# =============================

# =============================
# Main
# =============================
if __name__ == "__main__":
    # Spark
    spark = (
        SparkSession.builder
        .appName('LatestUnpivotProcessor_Zhe')
        .config("spark.sql.adapative.enabled", "true")
        .config("spark.sql.shuffle.partitions", 1000)\
        .getOrCreate()
    )
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    from pyspark.sql.functions import lit
    import datetime
    hdfs_pa= 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    df_sh = spark.read.parquet(hdfs_pa+"/sha_data/StationHistory/date=20250918")

    window_spec = Window.partitionBy("sn", "rowkey").orderBy(F.desc("count"))
    df_connectType = df_sh.select( "rowkey",
                                                                    col("Station_Data_connect_data.station_name").alias("station_name"),
                                                                    col("Station_Data_connect_data.connect_type").alias("connect_type"), 
                                                                        )\
                                                .withColumn("sn", F.regexp_extract("rowkey", r"([A-Z]+\d+)", 1))\
                                                .withColumn(
                                                        "connect_type",
                                                        F.when(F.col("connect_type").like("2.4G%"), "2_4G")
                                                        .when(F.col("connect_type").like("5G%"), "5G")
                                                        .when(F.col("connect_type").like("6G%"), "6G")
                                                        .otherwise(F.col("connect_type"))  
                                                    )\
                                                .filter(F.col("station_name").isNotNull() & F.col("connect_type").isNotNull())\
                                                .groupBy("sn", "rowkey", "connect_type", "station_name").count()\
                                                .withColumn("rank", F.rank().over(window_spec))\
                                                .filter(F.col("rank") == 1).drop("rank", "count")


    from pyspark.sql.functions import lit
    import datetime

    def backup_and_update(base_path: str, df_connectType, date_str: str):
        """
        Backup original parquet, join with df_connectType, add date column, and overwrite original.
        """
        # Create timestamped backup path under /backup/
        backup_path = (
            base_path.replace("/df_", "/backup/df_") 
            + "_" 
            + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        )

        # Step 1. Read original data
        df_orig = spark.read.parquet(base_path)

        # Step 2. Backup original
        (
            df_orig.write
            .mode("overwrite")
            .parquet(backup_path)
        )
        print(f"✅ Backup created at: {backup_path}")

        # Step 3. Apply join and add date column
        df_updated = (
            df_orig
            .join(df_connectType, ["sn", "rowkey"], "left")
            .withColumn("date", lit(date_str))
        )

        # Step 4. Overwrite original
        (
            df_updated.write
            .mode("overwrite")
            .parquet(base_path)
        )
        print(f"✅ Updated data written back to: {base_path}")


    # --- Apply for both tables ---
    date_str = "2025-09-18"

    backup_and_update("/user/ZheS/wifi_score_v4/df_rowkey_rssi_category/2025-09-18", df_connectType, date_str)
    backup_and_update("/user/ZheS/wifi_score_v4/df_rowkey_phyrate_category/2025-09-18", df_connectType, date_str)
