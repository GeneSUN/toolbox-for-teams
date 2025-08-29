
# -----------------------------
# Config
# -----------------------------

hdfs_namenode = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
base_dir = "/user/kovvuve/owl_history_v3/date="

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
TIME_COL = "time"    

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.neighbors import KernelDensity

class FeaturewiseKDENoveltyDetector:
    def __init__(self, 
                 df, 
                 feature_col="avg_4gsnr", 
                 time_col="hour", 
                 bandwidth=0.5,
                 train_idx="all", 
                 new_idx="all", 
                 filter_percentile=100, 
                 threshold_percentile=99,
                 anomaly_direction="low"):
        """
        Parameters:
            df (pd.DataFrame): Input data.
            feature_col (str): Column containing values to evaluate.
            time_col (str): Time column for plotting.
            bandwidth (float): Bandwidth for KDE.
            train_idx (slice, list, int, or "all"): Indices for training data. "all" uses the entire DataFrame.
            new_idx (slice, list, int, or "all"): Indices for test data. "all" uses the entire DataFrame.
            filter_percentile (float): Percentile for filtering out high-end outliers in training set.
            threshold_percentile (float): Percentile to apply directional outlier threshold.
            anomaly_direction (str): One of {"both", "high", "low"} to control direction of anomaly detection.
        
        Example Usage:
        detector = FeaturewiseKDENoveltyDetector(
                                                df=your_df,
                                                feature_col="avg_5gsnr",
                                                time_col="hour",
                                                train_idx=slice(0, 1068),
                                                new_idx=slice(-26, None),
                                                filter_percentile = 100,
                                                threshold_percentile=95,
                                                anomaly_direction="both"  # can be "low", "high", or "both"
                                                )
        result = detector.fit()
        """
        self.df = df
        self.feature_col = feature_col
        self.time_col = time_col
        self.bandwidth = bandwidth
        self.train_idx = train_idx
        self.new_idx = new_idx
        self.filter_percentile = filter_percentile
        self.threshold_percentile = threshold_percentile
        self.anomaly_direction = anomaly_direction
        self.kde = None
        self.threshold = None
        self.outlier_mask = None

    def _filter_train_df(self, train_df):
        """
        Filters training data by removing extreme values from both directions
        based on filter_percentile.
        
        If filter_percentile < 100:
            - Keeps the central filter_percentile% of the data.
            - Example: 95 keeps 2.5% on each tail removed.
        """
        if self.filter_percentile < 100:
            lower_p = (100 - self.filter_percentile) / 2
            upper_p = 100 - lower_p
            
            lower = np.percentile(train_df[self.feature_col], lower_p)
            upper = np.percentile(train_df[self.feature_col], upper_p)
            
            train_df = train_df[
                (train_df[self.feature_col] >= lower) & 
                (train_df[self.feature_col] <= upper)
            ]
        return train_df

    def fit(self):
        # Handle "all" option for training and testing index
        if self.train_idx == "all":
            train_df = self.df.copy()
        else:
            train_df = self.df.iloc[self.train_idx]
        train_df = self._filter_train_df(train_df)

        if self.new_idx == "all":
            new_df = self.df.copy()
            new_indices = self.df.index
        else:
            new_df = self.df.iloc[self.new_idx]
            new_indices = self.df.iloc[self.new_idx].index

        # Fit KDE on training data
        X_train = train_df[self.feature_col].values.reshape(-1, 1)
        X_new = new_df[self.feature_col].values.reshape(-1, 1)

        self.kde = KernelDensity(kernel='gaussian', bandwidth=self.bandwidth)
        self.kde.fit(X_train)

        # Compute densities
        dens_train = np.exp(self.kde.score_samples(X_train))
        self.threshold = np.quantile(dens_train, 0.01)

        dens_new = np.exp(self.kde.score_samples(X_new))
        outlier_mask_kde = dens_new < self.threshold

        # Directional anomaly logic based on percentiles
        new_values = new_df[self.feature_col].values
        lower_threshold = np.percentile(train_df[self.feature_col], 100 - self.threshold_percentile)
        upper_threshold = np.percentile(train_df[self.feature_col], self.threshold_percentile)

        if self.anomaly_direction == "low":
            direction_mask = new_values < lower_threshold
        elif self.anomaly_direction == "high":
            direction_mask = new_values > upper_threshold
        else:  # both
            direction_mask = (new_values < lower_threshold) | (new_values > upper_threshold)

        # Final anomaly mask
        final_outlier_mask = outlier_mask_kde & direction_mask
        self.outlier_mask = final_outlier_mask

        is_outlier_col = pd.Series(False, index=self.df.index)
        is_outlier_col.loc[new_indices] = final_outlier_mask
        self.df["is_outlier"] = is_outlier_col

        return self.df[self.df["is_outlier"]][["sn", self.time_col, self.feature_col, "is_outlier"]]


from datetime import date, timedelta
from functools import reduce
from typing import List
import time

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, FloatType, DoubleType,BooleanType
)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col


# -----------------------------
# Helpers
# -----------------------------
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

from pyspark.sql.functions import col, lag, when, lit, last


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


import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BooleanType

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BooleanType
import pandas as pd
import numpy as np

# 1) Schema for the Pandas UDF output
anomaly_schema = StructType([
    StructField("sn", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("feature", StringType(), True),
    StructField("value", FloatType(), True),
    StructField("is_outlier", BooleanType(), True),
])

# 2) Pandas UDF (per (sn, feature) group)
def kde_detect_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns: sn, time, feature, value
    Returns ONLY the anomalous rows for this (sn, feature) group.
    """
    # Ensure dtypes
    pdf = pdf.copy()
    pdf["time"] = pd.to_datetime(pdf["time"], errors="coerce")
    pdf["value"] = pd.to_numeric(pdf["value"], errors="coerce")

    # Sort by time
    pdf = pdf.sort_values("time").reset_index(drop=True)

    # Too few points → nothing to return
    if len(pdf) < 10:
        return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])

    try:
        det = FeaturewiseKDENoveltyDetector(
            df=pdf,
            feature_col="value",
            time_col="time",
            train_idx="all",
            new_idx=slice(-1, None),           
            filter_percentile=99,
            threshold_percentile=95,
            anomaly_direction="low",
        )
        out = det.fit()  # returns only anomalies: ['sn','time','value','is_outlier']

        if out.empty:
            return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])

        # Add 'feature' back and ensure column order
        out = out.merge(pdf[["sn", "time", "feature", "value"]], on=["sn", "time", "value"], how="left")
        out = out[["sn", "time", "feature", "value", "is_outlier"]]
        return out

    except Exception:
        # On error, return empty (schema-compatible)
        return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])


from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List

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

def unpivot_wide_to_long(df, time_col, feature_cols):
    # Build a stack(expr) for unpivot: (feature, value)
    n = len(feature_cols)
    expr = "stack({n}, {pairs}) as (feature, value)".format(
        n=n,
        pairs=", ".join([f"'{c}', `{c}`" for c in feature_cols])
    )
    return df.select("sn", time_col, F.expr(expr))

if __name__ == "__main__":
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    spark = SparkSession.builder.appName('kdeDetectionPipeline_zhe')\
                                .config("spark.sql.adapative.enabled","true")\
                                .config("spark.sql.shuffle.partitions", 3000)\
                                .getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")



    start_time = time.perf_counter()
    

    # Build list of HDFS paths and read once
    file_path = hdfs_namenode + base_dir + (date.today() - timedelta(days=4)).strftime('%Y-%m-%d')
    
    model_name = "ASK-NCM1100"
    df = spark.read.option("header", "true").csv(file_path)\
                                .withColumnRenamed("mdn", "MDN")\
                                .withColumn("MDN", F.regexp_replace(F.col("MDN"), '"', ''))\
                                .withColumn(TIME_COL, F.from_unixtime(F.col("ts") / 1000.0).cast("timestamp"))\
                                .select(["sn", "MDN", TIME_COL] + ALL_FEATURES)\
                                .dropDuplicates()\
                                #.filter( col("ModelName")==model_name )

    # Preprocess
    df = convert_string_numerical(df, ALL_FEATURES)

    zero_list = ["RSRQ","4GRSRQ", "4GRSRP", "BRSRP"]
    df = forward_fill(df, zero_list , "sn", "time")
    df = df.orderBy("sn","time")

    proc = HourlyIncrementProcessor(df, feature_groups["throughput_data"], partition_col = ["sn"] )\
                    .run( steps = ('incr','log','fill') )
    df = proc.df_hourly

    df_long = unpivot_wide_to_long(df, time_col=TIME_COL, feature_cols=ALL_FEATURES)

    # Run anomaly detection in parallel
    df_anomaly_all = df_long.groupBy("sn", "feature")\
                    .applyInPandas(kde_detect_pdf, schema=anomaly_schema)
    df_anomaly_all.write.mode("overwrite").parquet(f"/user/ZheS/owl_anomaly/test_time/kde")


    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    print(f"kde ran in {elapsed_time:.1f} seconds")
