

# =============================
# Config & Imports
# =============================
from typing import List, Tuple, Union
from datetime import date, timedelta
import time
import itertools

import numpy as np
import pandas as pd

from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, FloatType, DoubleType, BooleanType
)
from pyspark.sql.window import Window

# -----------------------------
# Paths & Columns
# -----------------------------
HDFS_NAMENODE = "hdfs://njbbepapa1.nss.vzwnet.com:9000"
BASE_DIR = "/user/kovvuve/owl_history_v3/date="   # final path: HDFS_NAMENODE + BASE_DIR + YYYY-MM-DD
TIME_COL = "time"

feature_groups = {
    "signal_quality": ["4GRSRP", "4GRSRQ", "SNR", "4GSignal", "BRSRP", "RSRQ", "5GSNR", "CQI"],
    "throughput_data": [
        "LTEPDSCHPeakThroughput", "LTEPDSCHThroughput",
        "LTEPUSCHPeakThroughput", "LTEPUSCHThroughput",
        "TxPDCPBytes", "RxPDCPBytes",
        "TotalBytesReceived", "TotalBytesSent",
        "TotalPacketReceived", "TotalPacketSent",
    ],
}
ALL_FEATURES: List[str] = feature_groups["signal_quality"] + feature_groups["throughput_data"]

# sensible “similar” pairs (you asked for curated combos, not Cartesian products)
COMBOS: List[Tuple[str, str]] = [
    ("4GRSRP", "4GRSRQ"),
    ("5GSNR", "SNR"),
    ("BRSRP", "RSRQ"),
    ("4GSignal", "CQI"),
    ("LTEPDSCHPeakThroughput", "LTEPDSCHThroughput"),
    ("LTEPUSCHPeakThroughput", "LTEPUSCHThroughput"),
    ("TxPDCPBytes", "RxPDCPBytes"),
    ("TotalBytesReceived", "TotalBytesSent"),
    ("TotalPacketReceived", "TotalPacketSent"),
]

# columns that legitimately may contain zeros and should be forward-filled
ZERO_FILL_CANDIDATES = {"RSRQ", "4GRSRQ", "4GRSRP", "BRSRP"}


# =============================
# Helpers
# =============================
def convert_string_numerical(df: DataFrame, cols_to_cast: List[str]) -> DataFrame:
    """Cast selected columns to DoubleType if they exist."""
    for c in cols_to_cast:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))
    return df


def forward_fill(df: DataFrame, cols_to_process: List[str], partition_col: str, order_col: str) -> DataFrame:
    """
    Forward fill zeros as nulls, then last non-null. If still null at partition head, fill with column mean.
    """
    if not cols_to_process:
        return df

    window_spec = (
        Window.partitionBy(partition_col)
        .orderBy(order_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Pre-compute means as fallback
    mean_row = df.select([F.mean(c).alias(f"mean_{c}") for c in cols_to_process]).first()
    for c in cols_to_process:
        df = df.withColumn(c, F.when(F.col(c) == 0, F.lit(None)).otherwise(F.col(c)))
        df = df.withColumn(c, F.last(F.col(c), ignorenulls=True).over(window_spec))
        mean_val = mean_row[f"mean_{c}"]
        if mean_val is not None:
            df = df.fillna({c: float(mean_val)})
    return df


# =============================
# HourlyIncrementProcessor (multi-partition aware)
# =============================
from pyspark.sql.functions import col, lag, when, lit, last

class HourlyIncrementProcessor:
    """
    Steps:
      - 'hourly' : hourly averages (original column names)
      - 'incr'   : replace features with smoothed increments (same names)
      - 'log'    : replace features with log1p(increments) (same names)
      - 'fill'   : forward-fill zeros in the (current) feature columns (same names)
    """

    def __init__(self, df: DataFrame, columns: List[str], partition_col=("sn",), time_col: str = TIME_COL):
        self.df = df
        self.columns = list(columns)
        self.partition_cols = list(partition_col) if isinstance(partition_col, (list, tuple)) else [partition_col]
        self.time_col = time_col

        self.df_hourly = None
        self._done = set()

    def compute_hourly_average(self):
        agg_exprs = [F.round(F.avg(c), 2).alias(c) for c in self.columns]
        self.df_hourly = self.df.groupBy(*self.partition_cols, self.time_col).agg(*agg_exprs)
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


# =============================
# DBSCAN Outlier Detector (pandas)
# =============================
class DBSCANOutlierDetector:
    """
    DBSCAN-based novelty/outlier scoring using a train/test window.
    - Train on train_X to learn core samples
    - Label test rows as outliers if min distance to any core sample > eps
    """

    def __init__(
        self,
        df: pd.DataFrame,
        features: List[str],
        eps: float = 0.5,
        min_samples: int = 5,
        train_idx: Union[int, str] = "all",
        recent_window_size: Union[int, str] = 24,
        time_col: str = TIME_COL,
        scale: bool = False,
        filter_percentile: float = 100.0,
    ):
        self.df = df.copy()
        self.features = features
        self.eps = eps
        self.min_samples = min_samples
        self.train_idx = train_idx
        self.test_idx = recent_window_size
        self.time_col = time_col
        self.scale = scale
        self.filter_percentile = filter_percentile

        self.scaler = StandardScaler() if scale else None
        self.train_X = None
        self.test_X = None
        self.test_indices = None
        self.labels_ = None
        self.outlier_mask = None
        self.dbscan_model = None

    def _apply_percentile_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.filter_percentile >= 100:
            return df
        low_p = (100 - self.filter_percentile) / 2
        high_p = 100 - low_p
        for col in self.features:
            lo = np.percentile(df[col], low_p)
            hi = np.percentile(df[col], high_p)
            df = df[(df[col] >= lo) & (df[col] <= hi)]
        return df

    def _apply_scaling(self, X_train: np.ndarray, X_test: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        if not self.scale:
            return X_train, X_test
        self.scaler.fit(X_train)
        return self.scaler.transform(X_train), self.scaler.transform(X_test)

    def _split_data(self):
        # train slice
        df_train = self.df if self.train_idx == "all" else self.df.iloc[: int(self.train_idx)]
        # test slice (recent window)
        df_test = self.df if self.test_idx == "all" else self.df.iloc[-int(self.test_idx) :]

        df_train = self._apply_percentile_filter(df_train)
        self.test_indices = df_test.index

        X_train = df_train[self.features].values
        X_test = df_test[self.features].values
        self.train_X, self.test_X = self._apply_scaling(X_train, X_test)

    def fit(self) -> pd.DataFrame:
        self._split_data()

        self.dbscan_model = DBSCAN(eps=self.eps, min_samples=self.min_samples).fit(self.train_X)
        core_samples = self.train_X[self.dbscan_model.core_sample_indices_]

        # label: -1 if far from all core samples
        self.labels_ = np.array(
            [-1 if np.min(np.linalg.norm(core_samples - x, axis=1)) > self.eps else 0 for x in self.test_X]
        )
        self.outlier_mask = self.labels_ == -1

        # decorate original df with is_outlier (only for test rows)
        self.df["is_outlier"] = False
        self.df.loc[self.test_indices[self.outlier_mask], "is_outlier"] = True

        return self.df


# =============================
# Main
# =============================
if __name__ == "__main__":
    desired_partition_number = 1200
    spark = (
        SparkSession.builder.appName("split_time_series_200_zhe")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    # --------- Load once ----------
    file_path = HDFS_NAMENODE + BASE_DIR + (date.today() - timedelta(days=4)).strftime("%Y-%m-%d")
    model_name = "ASK-NCM1100"
    df_base = (
        spark.read.option("header", "true").csv(file_path)
        .withColumnRenamed("mdn", "MDN")
        .withColumn("MDN", F.regexp_replace(F.col("MDN"), '"', ""))
        .withColumn(TIME_COL, F.from_unixtime(F.col("ts") / 1000.0).cast("timestamp"))
        .select(["sn", "MDN", TIME_COL] + ALL_FEATURES)
        .dropDuplicates()
        .filter( col("ModelName")==model_name )
    )
    df_base = convert_string_numerical(df_base, ALL_FEATURES)

    # --------- UDF factory (define once) ----------
    def make_dbscan_novelty_detector(feature_a: str, feature_b: str, time_col: str = TIME_COL):
        """
        Returns a function(pdf) -> pdf applying DBSCANOutlierDetector to (feature_a, feature_b).
        """
        def udf_detect(pdf: pd.DataFrame) -> pd.DataFrame:
            if len(pdf) < 10:
                return pd.DataFrame(columns=["sn", time_col, feature_a, feature_b, "is_outlier"])
            try:
                pdf = pdf.sort_values(time_col)
                det = DBSCANOutlierDetector(
                    df=pdf,
                    features=[feature_a, feature_b],
                    eps=2.0,
                    min_samples=3,
                    recent_window_size=1,
                    scale=False,                 # keep False unless features are on very different scales
                    # filter_percentile=99.0,    # optionally enable symmetric trimming
                    time_col=time_col,
                )
                res = det.fit()
                return res[["sn", time_col, feature_a, feature_b, "is_outlier"]]
            except Exception:
                return pd.DataFrame(columns=["sn", time_col, feature_a, feature_b, "is_outlier"])
        return udf_detect

    # --------- Schema builder (once) ----------
    def make_schema(feature_a: str, feature_b: str, time_col: str = TIME_COL) -> StructType:
        return StructType(
            [
                StructField("sn", StringType(), False),
                StructField(time_col, TimestampType(), True),
                StructField(feature_a, FloatType(), True),
                StructField(feature_b, FloatType(), True),
                StructField("is_outlier", BooleanType(), True),
            ]
        )

    # --------- Process each curated combo ----------
    for FEATURE_A, FEATURE_B in COMBOS:
        try:    
            start = time.perf_counter()

            # start each combo from the base DF
            df = df_base

            # Forward-fill zero-prone columns (do both in one pass if needed)
            cols_to_ffill = [c for c in (FEATURE_A, FEATURE_B) if c in ZERO_FILL_CANDIDATES]
            if cols_to_ffill:
                df = forward_fill(df, cols_to_ffill, partition_col="sn", order_col=TIME_COL).orderBy("sn", TIME_COL)

            # Hourly->Incr->Log->Fill for any throughput features present in the pair (run once passing both)
            throughput_cols = [c for c in (FEATURE_A, FEATURE_B) if c in feature_groups["throughput_data"]]
            if throughput_cols:
                proc = HourlyIncrementProcessor(df, throughput_cols, partition_col=("sn",), time_col=TIME_COL)
                proc.run(steps=("incr", "log", "fill"))
                # join back with the untouched columns (sn, MDN, time + the other feature if needed)
                keep_cols = ["sn", "MDN", TIME_COL] + [c for c in (FEATURE_A, FEATURE_B) if c not in throughput_cols]
                df = proc.df_hourly.join(df.select(*keep_cols), on=["sn", TIME_COL], how="left")

            # Build applyInPandas pieces
            detect_fn = make_dbscan_novelty_detector(FEATURE_A, FEATURE_B, TIME_COL)
            schema = make_schema(FEATURE_A, FEATURE_B, TIME_COL)

            # Run detector per sn
            df_anomaly = (
                df.groupBy("sn")
                .applyInPandas(detect_fn, schema=schema)
            )

            # Write out (clean double-slash and keep per-combo dir)
            out_path = f"/user/ZheS/owl_anomaly/test_time/outlier_{FEATURE_A}_{FEATURE_B}/dbscan"
            df_anomaly.write.mode("overwrite").parquet(out_path)

            elapsed = time.perf_counter() - start
            print(f"{FEATURE_A}_{FEATURE_B} ran in {elapsed:.2f} seconds")
        except Exception as e:
            print(f"{FEATURE_A}_{FEATURE_B}",e)
