from os import environ
from pyspark import SparkConf

HDFS_NAMENODE = environ.get("CORE_CONF_fs_defaultFS", "hdfs://namenode:9000")
ETH_RAW_PATH = HDFS_NAMENODE + "/project/raw/batch/ethereum/"
ETH_SILVER_PATH = HDFS_NAMENODE + "/project/silver/batch/ethereum/"
ETH_TRANSFORM_PATH = HDFS_NAMENODE + "/project/transform/batch/"
ELASTIC_SEARCH_NODE = environ.get("ELASTIC_SEARCH_NODE", "elasticsearch")
ELASTIC_SEARCH_USERNAME = environ.get("ELASTIC_SEARCH_USERNAME", "elastic")
ELASTIC_SEARCH_PASSWORD = environ.get("ELASTIC_SEARCH_PASSWORD", "password")
ELASTIC_SEARCH_PORT = environ.get("ELASTIC_SEARCH_PORT", "9200")

def get_conf(name):
    return SparkConf() \
            .setAppName(name) \
            .setMaster("spark://spark-master:7077") \
            .set("spark.cores.max", "8") \
            .set("spark.executor.cores", "4") \
            .set("spark.sql.shuffle.partitions", "200")

def save_data(df, ELASTIC_SEARCH_INDEX):
    """Save DataFrame to HDFS JSON and optionally to Elasticsearch"""
    df.show(20, truncate=False)
    
    print(f"\nWriting to HDFS JSON: {ETH_TRANSFORM_PATH}{ELASTIC_SEARCH_INDEX}")
    df.write.json(ETH_TRANSFORM_PATH + ELASTIC_SEARCH_INDEX, mode="overwrite")
    print(f"HDFS JSON saved!")
    
    try:
        print(f"\nAttempting Elasticsearch write: {ELASTIC_SEARCH_INDEX}")
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("overwrite") \
            .option("es.net.http.auth.user", ELASTIC_SEARCH_USERNAME) \
            .option("es.net.http.auth.pass", ELASTIC_SEARCH_PASSWORD) \
            .option("mergeSchema", "true") \
            .option('es.index.auto.create', 'true') \
            .option('es.nodes', ELASTIC_SEARCH_NODE) \
            .option('es.port', ELASTIC_SEARCH_PORT) \
            .option('es.batch.write.retry.wait', '10s') \
            .save(ELASTIC_SEARCH_INDEX)
        print(f"Elasticsearch saved!")
    except Exception as e:
        print(f"Elasticsearch not available (continuing without it): {str(e)[:100]}")