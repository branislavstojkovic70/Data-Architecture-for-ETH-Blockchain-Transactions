#!/usr/bin/python
# Top 100 Wallets - READS FROM SILVER LAYER

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, desc, avg
from helper import *

spark = SparkSession \
    .builder \
    .config(conf=get_conf("ETH-02-Top100Wallets")) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(ETH_SILVER_PATH)

print("\n=== Computing Top 100 Wallets ===")
print("  • Using pre-transformed value_eth from Silver")
print("  • Grouping by to_address")

top_wallets = df.groupBy("to_address").agg(
    _sum("value_eth").alias("total_received_eth"),
    count("*").alias("num_transactions"),
    avg("gas_price_gwei").alias("avg_gas_price_gwei"),
    _sum("gas_used").alias("total_gas_used")
).orderBy(desc("total_received_eth")).limit(100).cache()

top_1 = top_wallets.first()
total_wallets = df.select("to_address").distinct().count()

print(f"\nSUMMARY:")
print(f"Total unique wallets: {total_wallets:,}")
print(f"#1 wallet received: {top_1['total_received_eth']:,.2f} ETH")
print(f"#1 wallet transactions: {top_1['num_transactions']:,}")

print("\nTOP 100 WALLETS:\n")
top_wallets.show(100, truncate=False)

save_data_fast(top_wallets, "eth_top_100_wallets")

print("\nCOMPLETE!")