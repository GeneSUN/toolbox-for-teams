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
ZERO_LIST = ["RSRQ", "4GRSRQ", "4GRSRP", "BRSRP"]
ALL_FEATURES = feature_groups["signal_quality"] + feature_groups["throughput_data"]

# http://njbbvmaspd13:18080/#/notebook/2M3W7SX7Z
# ======================================================================
# Classes / Functions (kept identical in content; only ordering changed)
# ======================================================================

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


class EWMAAnomalyDetector:
    """
    EWMA-based anomaly detector with optional scaling and flexible recent window evaluation.

    Parameters:
        df (pd.DataFrame): Input time series data.
        feature (str): Target feature to detect anomalies on.
        recent_window_size (int or str): 'all' or integer; number of recent points to evaluate in scoring.
        window (int): Span for EWMA and rolling std.
        no_of_stds (float): Control limit multiplier.
        n_shift (int): Shift to prevent leakage.
        anomaly_direction (str): One of {'both', 'high', 'low'}.
        scaler (str or object): Optional scaler: 'standard', 'minmax', or custom scaler with fit_transform and inverse_transform.
        min_std_ratio (float): Minimum rolling std as a ratio of |feature| to avoid near-zero std (default: 0.01).
    """

    def __init__(
        self,
        df,
        feature,
        timestamp_col="time",
        recent_window_size="all",
        window=100,
        no_of_stds=3.0,
        n_shift=1,
        anomaly_direction="low",
        scaler=None,
        min_std_ratio=0.01,
        use_weighted_std=False
    ):
        assert anomaly_direction in {"both", "high", "low"}
        assert scaler in {None, "standard", "minmax"} or hasattr(scaler, "fit_transform")
        assert isinstance(recent_window_size, (int, type(None), str))

        self.df_original = df.copy()
        self.feature = feature
        self.timestamp_col = timestamp_col
        self.window = window
        self.no_of_stds = no_of_stds
        self.n_shift = n_shift
        self.recent_window_size = recent_window_size
        self.anomaly_direction = anomaly_direction
        self.df_ = None
        self.scaler_type = scaler
        self._scaler = None
        self.min_std_ratio = min_std_ratio
        self.use_weighted_std = use_weighted_std

    def _apply_scaler(self, df):
        df = df.copy()
        if self.scaler_type is None:
            df['feature_scaled'] = df[self.feature]
        else:
            if self.scaler_type == "standard":
                self._scaler = StandardScaler()
            elif self.scaler_type == "minmax":
                self._scaler = MinMaxScaler()
            else:
                self._scaler = self.scaler_type
            df['feature_scaled'] = self._scaler.fit_transform(df[[self.feature]])
        return df

    def _inverse_scaler(self, series):
        if self._scaler is None:
            return series
        return self._scaler.inverse_transform(series.values.reshape(-1, 1)).flatten()

    def _weighted_std_ewm(self, series, span):
        """
        Calculate exponentially weighted standard deviation.

        Formula:
            σ_w = sqrt( Σ wᵢ (xᵢ - μ_w)² / Σ wᵢ )

        Where:
            - xᵢ: input values in the rolling window
            - wᵢ: exponential weights (more recent points have higher weight)
            - μ_w: weighted mean = Σ wᵢ xᵢ / Σ wᵢ

        Parameters:
            series (pd.Series): Input series to compute weighted std on
            span (int): EWMA span (same as for EMA)

        Returns:
            pd.Series: weighted std aligned with EMA
        """
        import numpy as np
        alpha = 2 / (span + 1)
        weights = np.array([(1 - alpha) ** i for i in reversed(range(span))])
        weights /= weights.sum()

        x = series.values
        stds = []
        for i in range(len(x)):
            if i < span:
                stds.append(np.nan)
            else:
                window = x[i - span + 1:i + 1]
                mu_w = np.sum(weights * window)
                var_w = np.sum(weights * (window - mu_w) ** 2)
                stds.append(np.sqrt(var_w))
        return pd.Series(stds, index=series.index)


    def _add_ewma(self):
        
        df = self._apply_scaler(self.df_original)

        target = df['feature_scaled'].shift(self.n_shift)
        
        df['EMA'] = target.ewm(span=self.window, adjust=False).mean()
        if self.use_weighted_std:
            df['rolling_std'] = self._weighted_std_ewm(target, span=self.window)
        else:
            df['rolling_std'] = target.rolling(window=self.window).std()
        
        # Impose a lower bound on std to avoid degenerate control limits
        min_std = self.min_std_ratio * df['feature_scaled'].abs()
        df['rolling_std'] = df['rolling_std'].where(df['rolling_std'] >= min_std, min_std)

        df['UCL'] = df['EMA'] + self.no_of_stds * df['rolling_std']
        df['LCL'] = df['EMA'] - self.no_of_stds * df['rolling_std']
        
        return df

    def _detect_anomalies(self, df):
        if self.anomaly_direction == "high":
            df['is_outlier'] = df['feature_scaled'] > df['UCL']
        elif self.anomaly_direction == "low":
            df['is_outlier'] = df['feature_scaled'] < df['LCL']
        else:
            df['is_outlier'] = (df['feature_scaled'] > df['UCL']) | (df['feature_scaled'] < df['LCL'])
        
        df.loc[df.index[:self.window], 'is_outlier'] = False
        
        return df

    def fit(self):
        df = self._add_ewma()
        df = self._detect_anomalies(df)
        df_clean = df.dropna(subset=["EMA", "UCL", "LCL", "feature_scaled"])

        if self.recent_window_size in [None, "all"]:
            recent_df = df_clean
        else:
            recent_df = df_clean.tail(self.recent_window_size)

        self.df_ = df
        return recent_df[recent_df["is_outlier"]][["sn", self.timestamp_col, self.feature, "is_outlier"]]
    
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


def unpivot_wide_to_long(df, time_col, feature_cols):
    # Build a stack(expr) for unpivot: (feature, value)
    n = len(feature_cols)
    expr = "stack({n}, {pairs}) as (feature, value)".format(
        n=n,
        pairs=", ".join([f"'{c}', `{c}`" for c in feature_cols])
    )
    return df.select("sn", time_col, F.expr(expr))


def split_into_subseries(
    df: DataFrame,
    length: int,
    shift: int,
    sn_col: str = "sn",
    time_col: str = "time",
    id_col: str = "series_id",
    time_fmt: str = "yyyyMMddHHmmss"
) -> DataFrame:
    """
    Slice each SN's time series into overlapping windows of `length` rows,
    advancing by `shift` rows, ordered by `time_col`.

    Returns the original rows plus a new `id_col` that is the concatenation of
    sn and the sub-series start time formatted with `time_fmt`.
    """
    assert length > 0 and shift > 0, "length and shift must be positive integers"

    # 1) Order within each sn and assign row numbers
    w_rn = Window.partitionBy(sn_col).orderBy(time_col)
    df_rn = df.withColumn("rn", F.row_number().over(w_rn))

    # 2) For each sn, compute max rn and generate valid window starts: 1, 1+shift, ..., <= N-length+1
    max_rn = df_rn.groupBy(sn_col).agg(F.max("rn").alias("N"))
    starts = (
        max_rn
        .withColumn(
            "start_rn",
            F.when(
                F.col("N") >= F.lit(length),
                F.expr(f"sequence(1, N - {length} + 1, {shift})")  # array of start indices
            )
        )
        .select(sn_col, F.explode("start_rn").alias("start_rn"))  # one row per window start
    )

    # 3) Assign rows to windows where rn ∈ [start_rn, start_rn + length - 1]
    df_windows = (
        df_rn.join(starts, on=sn_col, how="inner")
             .filter((F.col("rn") >= F.col("start_rn")) & (F.col("rn") < F.col("start_rn") + length))
    )

    # 4) Compute sub-series start time per (sn, start_rn) and build series_id
    w_win = Window.partitionBy(sn_col, "start_rn")
    df_windows = df_windows.withColumn("series_start_time", F.min(time_col).over(w_win))
    df_windows = df_windows.withColumn(
        id_col,
        F.concat_ws("_", F.col(sn_col), F.date_format(F.col("series_start_time"), time_fmt))
    )

    # 5) Return rows annotated with the sub-series id (drop helpers)
    return df_windows.drop("rn", "N", "start_rn", "series_start_time")


# =============================
# UDF Schema + Pandas UDF
# =============================
anomaly_schema = StructType([
    StructField("sn", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("feature", StringType(), True),
    StructField("value", FloatType(), True),
    StructField("is_outlier", BooleanType(), True),
])

anomaly_schema_wide = StructType([
    StructField("sn", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("feature", StringType(), True),
    StructField("value", FloatType(), True),
    StructField("is_outlier_kde", BooleanType(), True),
    StructField("is_outlier_ewma", BooleanType(), True),
])


def groupwise_novelty_kde(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns: sn, time, feature, value
    Returns ONLY the anomalous rows for this (sn, feature) group.
    """
    pdf = pdf.copy()
    pdf["time"] = pd.to_datetime(pdf["time"], errors="coerce")
    pdf["value"] = pd.to_numeric(pdf["value"], errors="coerce")

    pdf = pdf.sort_values("time").reset_index(drop=True)

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
        out = det.fit()
        if out.empty:
            return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])

        # Add 'feature' back and ensure column order
        out = out.merge(pdf[["sn", "time", "feature", "value"]], on=["sn", "time", "value"], how="left")
        out = out[["sn", "time", "feature", "value", "is_outlier"]]
        return out

    except Exception:
        return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])


def groupwise_novelty_ewma(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Expects columns: sn, time, feature, value
    Returns ONLY the anomalous rows for this (sn, feature) group.
    """
    pdf = pdf.copy()
    pdf["time"] = pd.to_datetime(pdf["time"], errors="coerce")
    pdf["value"] = pd.to_numeric(pdf["value"], errors="coerce")

    pdf = pdf.sort_values("time").reset_index(drop=True)

    if len(pdf) < 10:
        return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])

    try:
        detector = EWMAAnomalyDetector(
            df=pdf,
            feature="value",
            timestamp_col="time",
            recent_window_size=1,
            window=200,
            no_of_stds=3.0,
            n_shift=1,
            anomaly_direction="low",
            scaler=None,
            min_std_ratio=0.01,
            use_weighted_std=False,
        )
        out = detector.fit()
        if out.empty:
            return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])

        # Add 'feature' back and ensure column order
        out = out.merge(pdf[["sn", "time", "feature", "value"]], on=["sn", "time", "value"], how="left")
        out = out[["sn", "time", "feature", "value", "is_outlier"]]
        return out

    except Exception:
        return pd.DataFrame(columns=["sn", "time", "feature", "value", "is_outlier"])


def groupwise_novelty_both(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Apply both KDE and EWMA novelty detectors to a single (sn, feature) group.
    Returns only rows where at least one detector marks the observation as an outlier.
    """
    pdf = pdf.copy()
    pdf["time"] = pd.to_datetime(pdf["time"], errors="coerce")
    pdf["value"] = pd.to_numeric(pdf["value"], errors="coerce")
    pdf = pdf.sort_values("time").reset_index(drop=True)

    if len(pdf) < 10:
        return pd.DataFrame(columns=["sn","time","feature","value","is_outlier_kde","is_outlier_ewma"])

    try:
        # KDE detector
        kde = FeaturewiseKDENoveltyDetector(
            df=pdf,
            feature_col="value",
            time_col="time",
            train_idx="all",
            new_idx=slice(-1, None),
            filter_percentile=99,
            threshold_percentile=99,
            anomaly_direction="low",
        )
        out_kde = kde.fit()[["sn","time","value","is_outlier"]].rename(
            columns={"is_outlier":"is_outlier_kde"}
        )

        # EWMA detector
        ewma = EWMAAnomalyDetector(
            df=pdf,
            feature="value",
            timestamp_col="time",
            recent_window_size=1,
            window=100,
            no_of_stds=3.0,
            n_shift=1,
            anomaly_direction="low",
            scaler=None,
            min_std_ratio=0.01,
            use_weighted_std=False,
        )
        out_ewma = ewma.fit()[["sn","time","value","is_outlier"]].rename(
            columns={"is_outlier":"is_outlier_ewma"}
        )

        # Join results
        base = pdf[["sn","time","feature","value"]]
        out = (
            base.merge(out_kde, on=["sn","time","value"], how="left")
                .merge(out_ewma, on=["sn","time","value"], how="left")
        )

        out[["is_outlier_kde","is_outlier_ewma"]] = (
            out[["is_outlier_kde","is_outlier_ewma"]].fillna(False)
        )

        out = out[(out["is_outlier_kde"]) | (out["is_outlier_ewma"])]

        return out[["sn","time","feature","value","is_outlier_kde","is_outlier_ewma"]]

    except Exception:
        return pd.DataFrame(columns=["sn","time","feature","value","is_outlier_kde","is_outlier_ewma"])

if __name__ == "__main__":
    # Spark
    spark = (
        SparkSession.builder
        .appName('kdeDetectionPipeline_zhe')
        .config("spark.sql.adapative.enabled", "true")
        .config("spark.sql.shuffle.partitions", 1200)\
        .getOrCreate()
    )
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")    

    file_path = hdfs_namenode + base_dir + (date.today() - timedelta(days=2)).strftime('%Y-%m-%d')
    model_name = "ASK-NCM1100"

    now = datetime.now()
    #now = datetime(2025, 8, 28, 10, 30, 0) # Year, Month, Day, Hour, Minute, Second

    file_paths = []

    for i in range(200):
        current_hour_dt = now - timedelta(hours=i)
        
        date_str = current_hour_dt.strftime("%Y-%m-%d")
        hour_str = current_hour_dt.strftime("hr=%H")
        
        path = f"{hdfs_namenode}/{base_dir}{date_str}/{hour_str}"
        file_paths.append(path)

    sn_list = ['ACR50220495', 'ACR50219952', 'ACR45123236', 'ACR50709744', 'ACR45127066', 'ACR50407638', 'ACR51109908', 'ACR52417251', 'ACR51317239', 'ACR44810858', 'ACR43301951', 'ACR43103903', 'ACR43105974', 'ACR44214489', 'ACR52212239', 'ACR44717227', 'ACR50111657', 'ACR51112474', 'ACR44000230', 'ACR52505377', 'ACR45011967', 'ACR50210814', 'ACR43712925', 'ACR44700139', 'ACR50401575', 'ACR51312404', 'ACR52605358', 'ACR50204281', 'ACR44713139', 'ACR52304552', 'ACR50705978', 'ACR44510528', 'ACR43714196', 'ACR44909542', 'ACR52301175', 'ACR44406975', 'ACR44518289', 'ACR43403518', 'ACR44902646', 'ACR44003303', 'ACR51110264', 'ACR45105556', 'ACR42006080', 'ACR52601816', 'ACR44700010', 'ACR51519291', 'ACR51701149', 'ACR43513827', 'ACR50204843', 'ACR42812887', 'ACR44700266', 'ACR50719917', 'ACR43100493', 'ACR51106604', 'ACR43310012', 'ACR51505149', 'ACR50423435', 'ACR50906565', 'ACR43109313', 'ACR44723610', 'ACR51717554', 'ACR43308279', 'ACR44715171', 'ACR45004304', 'ACR44522300', 'ACR45125537', 'ACR51314147', 'ACR44902044', 'ACR50419211', 'ACR43400537', 'ACR51508875', 'ACR50907524', 'ACR42802896', 'ACR43103268', 'ACR44516105', 'ACR44801791', 'ACR50211956', 'ACR42807055', 'ACR45122687', 'ACR51304508', 'ACR44810561', 'ACR44007959', 'ACR43511767', 'ACR45100534', 'ACR45120057', 'ACR44902278', 'ACR51315781', 'ACR42407111', 'ACR50709571', 'ACR50205333', 'ACR44810509', 'ACR50115055', 'ACR50706528', 'ACR44005591', 'ACR44701895', 'ACR50208010', 'ACR42201924', 'ACR44010952', 'ACR51506275', 'ACR44900466']
    #sn_list = sn_list[:40]

    df = spark.read.option("header","true").csv(file_paths)

    df = (
        df
        .withColumnRenamed("mdn", "MDN")
        .withColumn("MDN", F.regexp_replace(F.col("MDN"), '"', ''))
        .withColumn(TIME_COL, F.from_unixtime(F.col("ts") / 1000.0).cast("timestamp"))
        .select(["sn", "MDN", TIME_COL] + ALL_FEATURES)
        .dropDuplicates()
        .filter( col("sn").isin(sn_list) )
        .filter(col("ModelName") == model_name)  # Include ModelName in select() above if you enable this
    )

# 2.Preprocess
    df = convert_string_numerical(df, ALL_FEATURES)

    
    df = forward_fill(df, ZERO_LIST, "sn", "time")
    df = df.orderBy("sn", "time")

    # Throughput features → hourly increments pipeline
    proc = (
        HourlyIncrementProcessor(df, feature_groups["throughput_data"], partition_col=["sn"])
        .run(steps=('incr', 'log', 'fill'))
    )
    df = proc.df_hourly


    df_slice = split_into_subseries(df, length=200, shift=1, sn_col="sn", time_col="time")
    df_slice = df_slice.drop("sn")\
                        .withColumnRenamed("series_id", "sn")
    df_slice.write.mode("overwrite").parquet("/user/ZheS/owl_anomaly/processed_ask-ncm1100_hourly_features/data")
    """    """
    df_slice=spark.read.parquet("/user/ZheS/owl_anomaly/processed_ask-ncm1100_hourly_features/data")

    #df_long = unpivot_wide_to_long(df_slice, time_col=TIME_COL, feature_cols=ZERO_LIST)
    df_long = unpivot_wide_to_long(df_slice, time_col=TIME_COL, feature_cols=ALL_FEATURES)


#3. Distributed Modeling
    df_result = df_long.groupBy("sn","feature")\
                        .applyInPandas(groupwise_novelty_both, schema=anomaly_schema_wide)

    df_result.write.mode("overwrite").parquet("/user/ZheS/owl_anomaly/processed_ask-ncm1100_hourly_features/results")

#sn_list = ['ACR50220495', 'ACR50219952', 'ACR45123236', 'ACR50709744', 'ACR45127066', 'ACR50407638', 'ACR51109908', 'ACR52417251', 'ACR51317239', 'ACR44810858', 'ACR43301951', 'ACR43103903', 'ACR43105974', 'ACR44214489', 'ACR52212239', 'ACR44717227', 'ACR50111657', 'ACR51112474', 'ACR44000230', 'ACR52505377', 'ACR45011967', 'ACR50210814', 'ACR43712925', 'ACR44700139', 'ACR50401575', 'ACR51312404', 'ACR52605358', 'ACR50204281', 'ACR44713139', 'ACR52304552', 'ACR50705978', 'ACR44510528', 'ACR43714196', 'ACR44909542', 'ACR52301175', 'ACR44406975', 'ACR44518289', 'ACR43403518', 'ACR44902646', 'ACR44003303', 'ACR51110264', 'ACR45105556', 'ACR42006080', 'ACR52601816', 'ACR44700010', 'ACR51519291', 'ACR51701149', 'ACR43513827', 'ACR50204843', 'ACR42812887', 'ACR44700266', 'ACR50719917', 'ACR43100493', 'ACR51106604', 'ACR43310012', 'ACR51505149', 'ACR50423435', 'ACR50906565', 'ACR43109313', 'ACR44723610', 'ACR51717554', 'ACR43308279', 'ACR44715171', 'ACR45004304', 'ACR44522300', 'ACR45125537', 'ACR51314147', 'ACR44902044', 'ACR50419211', 'ACR43400537', 'ACR51508875', 'ACR50907524', 'ACR42802896', 'ACR43103268', 'ACR44516105', 'ACR44801791', 'ACR50211956', 'ACR42807055', 'ACR45122687', 'ACR51304508', 'ACR44810561', 'ACR44007959', 'ACR43511767', 'ACR45100534', 'ACR45120057', 'ACR44902278', 'ACR51315781', 'ACR42407111', 'ACR50709571', 'ACR50205333', 'ACR44810509', 'ACR50115055', 'ACR50706528', 'ACR44005591', 'ACR44701895', 'ACR50208010', 'ACR42201924', 'ACR44010952', 'ACR51506275', 'ACR44900466']
    