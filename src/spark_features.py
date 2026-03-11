"""
PySpark feature engineering — replicates preprocess.add_features() using
PySpark DataFrames instead of pandas.

Run from the project root:
    python3 src/spark_features.py

Output:
    data/processed/spark_features.parquet/
"""

import os
import sys

# Make sure JAVA_HOME is set for PySpark
if 'JAVA_HOME' not in os.environ:
    candidates = [
        '/opt/homebrew/opt/openjdk@17',
        '/opt/homebrew/opt/openjdk',
        '/usr/local/opt/openjdk@17',
    ]
    for c in candidates:
        if os.path.isdir(c):
            os.environ['JAVA_HOME'] = c
            break

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

# allow relative imports when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from preprocess import fetch_temperature

RAW_ENERGY = 'data/raw/opsd_germany_daily.csv'
RAW_WEATHER = 'data/raw/weather_berlin.csv'
OUTPUT_DIR = 'data/processed/spark_features.parquet'


def get_spark():
    return (
        SparkSession.builder
        .appName("energy_feature_engineering")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_energy(spark):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_ENERGY)
    )
    return (
        df.select(
            F.to_date(F.col("Date")).alias("date"),
            F.col("Consumption").cast(DoubleType()).alias("consumption")
        )
        .filter(F.col("consumption").isNotNull())
        .orderBy("date")
    )


def load_weather(spark):
    """Load pre-saved weather CSV; fetch+save it first if missing."""
    if not os.path.exists(RAW_WEATHER):
        print("Fetching weather data (requires internet)...")
        temp_df = fetch_temperature('2006-01-01', '2017-12-31')
        temp_df.index.name = 'date'
        temp_df.to_csv(RAW_WEATHER)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(RAW_WEATHER)
    )
    return (
        df.select(
            F.to_date(F.col("date")).alias("date"),
            F.col("temperature").cast(DoubleType()).alias("temperature")
        )
        .filter(F.col("temperature").isNotNull())
    )


def add_features(energy_df, weather_df):
    """
    Replicates preprocess.add_features():
      - join temperature
      - dayofweek (0=Mon … 6=Sun, matching pandas convention)
      - month, is_weekend, is_holiday (approximated: no holidays library in Spark)
      - lag_1, lag_7
      - rolling_7 (shift-1 7-day mean, same window as pandas version)
    """
    # --- join temperature ---
    df = energy_df.join(weather_df, on="date", how="left")

    # --- time features ---
    # PySpark dayofweek: 1=Sunday … 7=Saturday
    # Pandas  dayofweek: 0=Monday … 6=Sunday
    # Convert: pandas_dow = (spark_dow + 5) % 7  (Sun→6, Mon→0, … Sat→5)
    df = (
        df
        .withColumn("_spark_dow", F.dayofweek(F.col("date")))
        .withColumn("dayofweek",  ((F.col("_spark_dow") + 5) % 7).cast(IntegerType()))
        .withColumn("month",       F.month(F.col("date")).cast(IntegerType()))
        .withColumn("is_weekend",
                    F.when(F.col("_spark_dow").isin(1, 7), 1).otherwise(0)
                     .cast(IntegerType()))
        .drop("_spark_dow")
    )

    # --- lag & rolling features ---
    date_window    = Window.orderBy("date")
    rolling_window = Window.orderBy("date").rowsBetween(-8, -2)  # shift-1 rolling 7

    df = (
        df
        .withColumn("lag_1",     F.lag("consumption", 1).over(date_window))
        .withColumn("lag_7",     F.lag("consumption", 7).over(date_window))
        .withColumn("rolling_7", F.avg("consumption").over(rolling_window))
    )

    # drop rows where any lag/rolling is null (mirrors pandas dropna)
    df = df.filter(
        F.col("lag_1").isNotNull() &
        F.col("lag_7").isNotNull() &
        F.col("rolling_7").isNotNull()
    )

    return df


def main():
    os.makedirs("data/processed", exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading energy data...")
    energy_df = load_energy(spark)

    print("Loading weather data...")
    weather_df = load_weather(spark)

    print("Engineering features...")
    features_df = add_features(energy_df, weather_df)

    n = features_df.count()
    print(f"Feature rows: {n}")
    features_df.printSchema()
    features_df.show(5, truncate=False)

    print(f"Saving to {OUTPUT_DIR} ...")
    features_df.write.mode("overwrite").parquet(OUTPUT_DIR)
    print("Done.")

    spark.stop()


if __name__ == '__main__':
    main()
