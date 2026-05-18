import time
from pyspark.sql import functions as F

from deps.export_utils import export_results
from deps.influxdb_utils import write_results_on_influxdb
from deps.config import QUERY1_CONFIG

# Magia: Importiamo tutto da utils!
from deps.utils import *

def run(spark, sc, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    start_time = time.time()
    OUTPUT_FORMAT = "csv"
    result_file = f"hdfs://hdfs-master:54310/dataset_results/query1_df.{OUTPUT_FORMAT}"

    # 1. Caricamento e cast avvengono in una sola riga!
    df = get_df(spark, dataset_path, FILE_FORMAT)

    # 2. Uso delle costanti (CARRIER, CANCELLED, etc.) invece delle stringhe
    df_filtered = df.filter(F.col(CARRIER).isin("AA", "DL"))

    result = df_filtered.groupBy(MONTH, CARRIER).agg(
        F.round(F.mean(F.when(F.col(CANCELLED) == 0, F.col(DEP_DELAY))), 2).alias("dep_delay_mean"),
        F.min(F.when(F.col(CANCELLED) == 0, F.col(DEP_DELAY))).alias("dep_delay_min"),
        F.max(F.when(F.col(CANCELLED) == 0, F.col(DEP_DELAY))).alias("dep_delay_max"),
        F.round((F.sum(CANCELLED) / F.count("*")) * 100, 2).alias("cancellation_rate")
    ).orderBy(MONTH, CARRIER)

    result = result.withColumnRenamed("MONTH", "month") \
                   .withColumnRenamed("OP_UNIQUE_CARRIER", "op_unique_carrier")
    
    if TIMED: 
        result.collect()
    else:
        export_results(result, OUTPUT_FORMAT, result_file)
        write_results_on_influxdb(result, "query1", QUERY1_CONFIG)

    return time.time() - start_time