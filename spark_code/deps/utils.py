import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F

#costanti per evitare errori
CARRIER   = "OP_UNIQUE_CARRIER"
MONTH     = "MONTH"
DEP_DELAY = "DEP_DELAY"
ARR_DELAY = "ARR_DELAY"
CANCELLED = "CANCELLED"
DIVERTED  = "DIVERTED"
CARRIER_DELAY = "CARRIER_DELAY"
WEATHER_DELAY = "WEATHER_DELAY"
NAS_DELAY = "NAS_DELAY"
SECURITY_DELAY = "SECURITY_DELAY"
LATE_AIRCRAFT_DELAY = "LATE_AIRCRAFT_DELAY"

DELAY_COLS = [DEP_DELAY, ARR_DELAY, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY]

# ==========================================
# FUNZIONI DI UTILITÀ
# ==========================================

def get_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.sparkContext.setLogLevel("ERROR")
    return spark, spark.sparkContext



def check_hdfs(sc: SparkContext, path_str: str):
    hadoop_conf = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hadoop_conf)
    if not fs.exists(path):
        print(f"Dataset in {path_str} not exist on HDFS")
        sys.exit(1)



def get_df(spark: SparkSession, dataset_path: str, file_format: str):
    df = spark.read.format(file_format).option("header", "true").load(dataset_path)
    df = df.withColumn(CANCELLED, F.col(CANCELLED).cast("int")).withColumn(DIVERTED, F.col(DIVERTED).cast("int")).withColumn(MONTH, F.col(MONTH).cast("int"))
    for col in DELAY_COLS:
        df = df.withColumn(col, F.col(col).cast("float"))  
    return df