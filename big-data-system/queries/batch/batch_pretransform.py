#!/usr/bin/python
# Reads CSV from /batch-data/ethereum/ and writes to HDFS Bronze layer as Parquet
# /spark/bin/spark-submit /queries/batch_pretransform.py

from os import environ
from pyspark.sql import SparkSession

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
ETH_RAW_PATH = HDFS_NAMENODE + "/project/raw/batch/ethereum/"

spark = SparkSession.builder \
    .appName("EthereumPretransform") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("BRONZE LAYER - ETH Transactions Pretransform")

print("\n=== Reading Ethereum CSV from /batch-data/ethereum/ ===")
df = spark.read.csv(
    path="/batch-data/ethereum/",
    header=True,
    inferSchema=True
)

print(f"\nTotal transactions loaded: {df.count():,}")
print("\nSchema:")
df.printSchema()

initial_count = df.count()
df = df.dropDuplicates(['transaction_hash'])
duplicates_removed = initial_count - df.count()
print(f"\nDuplicates removed: {duplicates_removed:,}")

print("\nRepartitioning by block_number (50 partitions)...")
df = df.repartition(50, "block_number")

print(f"\nWriting to HDFS Bronze: {ETH_RAW_PATH}")
df.write \
    .partitionBy("block_number") \
    .mode("overwrite") \
    .parquet(ETH_RAW_PATH)

print(" BRONZE LAYER COMPLETE!")
print(f"Location: {ETH_RAW_PATH}")
print(f"Format: Parquet (partitioned by block_number)")
print(f"Records: {df.count():,}")
