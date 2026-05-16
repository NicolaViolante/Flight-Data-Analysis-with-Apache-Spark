import time
from pyspark.sql import functions as F

from deps.hdfs_utils import write_results_on_hdfs
from deps.redis_utils import write_results_on_redis
from deps.utils import *

def run(spark, sc, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    start_time = time.time()
    OUTPUT_FORMAT = "csv"
    result_file = f"hdfs://hdfs-master:54310/dataset_results/query2_df.{OUTPUT_FORMAT}"

    # Caricamento e cast centralizzati
    df = get_df(spark, dataset_path, FILE_FORMAT)

    # Filtro usando le costanti
    df_valid = df.filter((F.col(CANCELLED) == 0) & (F.col(DIVERTED) == 0))

    grouped = df_valid.groupBy(CARRIER).agg(
        F.count("*").alias("num_flights"),
        F.round(F.mean(ARR_DELAY), 2).alias("arrdelay_mean"),
        F.round(F.mean(F.coalesce(CARRIER_DELAY, F.lit(0))), 2).alias("carrier_delay_mean"),
        F.round(F.mean(F.coalesce(WEATHER_DELAY, F.lit(0))), 2).alias("weather_delay_mean"),
        F.round(F.mean(F.coalesce(NAS_DELAY, F.lit(0))), 2).alias("nas_delay_mean"),
        F.round(F.mean(F.coalesce(SECURITY_DELAY, F.lit(0))), 2).alias("security_delay_mean"),
        F.round(F.mean(F.coalesce(LATE_AIRCRAFT_DELAY, F.lit(0))), 2).alias("late_aircraft_delay_mean")
    )

    result = grouped.filter(F.col("num_flights") >= 500) \
                    .orderBy(F.col("arrdelay_mean").desc()) \
                    .limit(10)

    if TIMED: 
        result.collect()
    else:
        write_results_on_hdfs(result, OUTPUT_FORMAT, result_file)
        
        # Salvataggio su Redis
        redis_data = [row.asDict() for row in result.collect()]
        write_results_on_redis(redis_data, "query2_df")

    return time.time() - start_time