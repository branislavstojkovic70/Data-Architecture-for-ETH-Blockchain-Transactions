#!/usr/bin/python
# Bronze to Silver transformation - Cleans and enriches data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
from helper import *

spark = SparkSession \
    .builder \
    .config(conf=get_conf("ETH-01-BronzeToSilver")) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ELASTIC_SEARCH_INDEX = "eth_silver"

print("SILVER LAYER - Bronze to Silver Transformation")
print(f"\n=== Reading from Bronze HDFS: {ETH_RAW_PATH} ===")
df = spark.read.parquet(ETH_RAW_PATH)
print(f"Bronze records: {df.count():,}")

print("\n=== Applying Transformations ===")
print("  • Converting value to ETH (÷ 1e18)")
print("  • Converting gas_price to Gwei (÷ 1e9)")
print("  • Parsing block_timestamp")
print("  • Flagging contract calls")

silver_df = df.select(
    col("transaction_hash").alias("hash"),
    col("from_address"),
    col("to_address"),
    (col("value").cast("double") / 1e18).alias("value_eth"),
    (col("gas_price").cast("double") / 1e9).alias("gas_price_gwei"),
    col("gas").alias("gas_used"),
    to_timestamp(col("block_timestamp"), "yyyy-MM-dd HH:mm:ss z").alias("block_timestamp"),
    col("block_number"),
    when(col("input") != "0x", True).otherwise(False).alias("is_contract_call")
)

silver_count = silver_df.count()
print(f"\nSilver records: {silver_count:,}")

print(f"\nWriting to Silver HDFS: {ETH_SILVER_PATH}")
silver_df.write.mode("overwrite").parquet(ETH_SILVER_PATH)
print("Silver Parquet saved!")

print(f"\nWriting to JSON + Elasticsearch...")
save_data(silver_df, ELASTIC_SEARCH_INDEX)

print("SILVER LAYER COMPLETE!")
print(f"HDFS Location: {ETH_SILVER_PATH}")
print(f"JSON Location: {ETH_TRANSFORM_PATH}{ELASTIC_SEARCH_INDEX}")
print(f"Records: {silver_count:,}")
