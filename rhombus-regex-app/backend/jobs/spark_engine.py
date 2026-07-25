"""
The distributed transformation engine. Given an uploaded CSV/Excel file, a
validated regex, and a replacement value, this:

  1. Reads the file into a Spark DataFrame (partitioned, not single-machine).
  2. Applies the regex as a Spark SQL `regexp_replace` transformation across
     the target column -- this runs on every partition in parallel, so it
     scales horizontally as row count grows, instead of iterating row-by-row
     in pandas on one core.
  3. Writes the result out as multiple Parquet part-files (one per
     partition) rather than a single collected blob, so nothing has to be
     pulled into the driver's memory.
  4. Reports coarse-grained progress back to the caller via a callback, using
     a Spark Accumulator that each partition increments as it finishes.

Partitioning choice: we repartition to `settings.SPARK_SHUFFLE_PARTITIONS`
before the transform. Too few partitions under-uses available cores; too
many adds scheduling overhead for not much benefit on a single-node dev
cluster. In a real multi-worker cluster this would instead be tuned relative
to total core count and target partition size (~128MB/partition is a common
rule of thumb), which we call out in the README as a production trade-off.
"""

import logging
import os
import threading
import time
from dataclasses import dataclass

import pandas as pd
from django.conf import settings
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

logger = logging.getLogger(__name__)

_spark_session: SparkSession | None = None


def get_spark() -> SparkSession:
    global _spark_session
    if _spark_session is None:
        _spark_session = (
            SparkSession.builder.appName("rhombus-regex-engine")
            .master(settings.SPARK_MASTER)
            .config("spark.sql.shuffle.partitions", settings.SPARK_SHUFFLE_PARTITIONS)
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
    return _spark_session


@dataclass
class ReplacementResult:
    result_dir: str
    row_count: int
    matched_count: int


def _read_input(spark: SparkSession, file_path: str):
    if file_path.lower().endswith(".csv"):
        # true distributed read: Spark splits the file across partitions itself
        return spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

    # Excel has no native distributed reader in vanilla PySpark, so we parse
    # it with pandas first and hand it to Spark. This is a known trade-off:
    # for very large .xlsx files, converting to CSV upstream (or using the
    # spark-excel connector) would be the production-grade choice.
    pdf = pd.read_excel(file_path)
    return spark.createDataFrame(pdf.astype(str).where(pd.notnull(pdf), None))


def run_replacement(
    file_path: str,
    target_column: str,
    pattern: str,
    replacement_value: str,
    output_dir: str,
    progress_callback=None,
) -> ReplacementResult:
    spark = get_spark()
    df = _read_input(spark, file_path)

    if target_column not in df.columns:
        raise ValueError(
            f"Column '{target_column}' not found. Available columns: {', '.join(df.columns)}"
        )

    df = df.repartition(settings.SPARK_SHUFFLE_PARTITIONS)
    df.persist()
    total_rows = df.count()  # materializes the partitioning; also our progress denominator

    processed_acc = spark.sparkContext.accumulator(0)
    matched_acc = spark.sparkContext.accumulator(0)

    def _progress_poller(stop_event: threading.Event):
        while not stop_event.is_set():
            if total_rows > 0 and progress_callback:
                pct = min(99, int((processed_acc.value / total_rows) * 100))
                progress_callback(pct)
            time.sleep(0.5)

    stop_event = threading.Event()
    poller = threading.Thread(target=_progress_poller, args=(stop_event,), daemon=True)
    poller.start()

    match_col = F.col(target_column).rlike(pattern)

    def _count_partition(rows):
        local_processed = 0
        local_matched = 0
        for row in rows:
            local_processed += 1
            value = row[target_column]
            if value is not None:
                import re

                if re.search(pattern, str(value)):
                    local_matched += 1
        processed_acc.add(local_processed)
        matched_acc.add(local_matched)
        return iter(())

    # foreachPartition purely to drive accumulator-based progress; the actual
    # column transformation below is the real (idempotent, declarative) Spark
    # transformation applied to produce the output.
    try:
        df.foreachPartition(_count_partition)

        transformed = df.withColumn(
            target_column,
            F.regexp_replace(F.col(target_column), pattern, replacement_value),
        )

        os.makedirs(output_dir, exist_ok=True)
        transformed.write.mode("overwrite").parquet(output_dir)
    finally:
        stop_event.set()
        poller.join(timeout=2)
        df.unpersist()

    if progress_callback:
        progress_callback(100)

    return ReplacementResult(
        result_dir=output_dir,
        row_count=total_rows,
        matched_count=matched_acc.value,
    )
