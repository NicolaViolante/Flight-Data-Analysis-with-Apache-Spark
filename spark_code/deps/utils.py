import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from deps.export_utils import export_results

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



def get_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.sparkContext.setLogLevel("ERROR")
    return spark



def check_hdfs(spark: SparkSession, path_str: str):
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
    if not fs.exists(path):
        print(f"Dataset in {path_str} not exist on HDFS")
        sys.exit(1)



def get_df(spark: SparkSession, dataset_path: str, file_format: str):
    df = spark.read.format(file_format).option("header", "true").load(dataset_path)
    df = df.withColumn(CANCELLED, F.col(CANCELLED).cast("int")).withColumn(DIVERTED, F.col(DIVERTED).cast("int")).withColumn(MONTH, F.col(MONTH).cast("int"))
    for col in DELAY_COLS:
        df = df.withColumn(col, F.col(col).cast("float"))  
    return df


def run_query(spark: SparkSession, dataset_path: str, file_format: str, timed: bool, query_num: int, api_name: str, core_logic_func) -> float:
    start_time = time.time()
    output_format = "csv"
    result_file = f"hdfs://hdfs-master:54310/results/query{query_num}_{file_format}_{api_name}.{output_format}"

    df = get_df(spark, dataset_path, file_format) # restituisce dataframe normalizzato e con i valori castati

    result_df = core_logic_func(df, spark) # esecuzione della query specifica

    if timed:
        result_df.collect() # per evitare scritture su disco e non inficiare i risultati dei benchmark
    else:
        export_results(result_df, output_format, result_file)
        
        if query_num == 1:
            from deps.influxdb_utils import write_results_on_influxdb
            from deps.config import QUERY1_CONFIG
            write_results_on_influxdb(result_df, f"query1_{file_format}_{api_name}", QUERY1_CONFIG)
        elif query_num == 2:
            from deps.redis_utils import write_results_on_redis
            redis_data = [row.asDict() for row in result_df.collect()] # restituisce lista di dizionari, essenzialmente JSON
            write_results_on_redis(redis_data, f"query2_{file_format}_{api_name}")
    return time.time() - start_time