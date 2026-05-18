import time

from deps.export_utils import export_results
from deps.redis_utils import write_results_on_redis
from deps.utils import *

def run(spark, sc, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    start_time = time.time()
    OUTPUT_FORMAT = "csv"
    result_file = f"hdfs://hdfs-master:54310/results/query2_{FILE_FORMAT}_sql.{OUTPUT_FORMAT}"

    df = get_df(spark, dataset_path, FILE_FORMAT)
    df.createOrReplaceTempView("flights")

    query = f"""
        SELECT {CARRIER} AS carrier,
               COUNT(*) AS num_flights,
               ROUND(AVG({ARR_DELAY}), 2) AS arrdelay_mean,
               ROUND(AVG(COALESCE({CARRIER_DELAY}, 0)), 2) AS carrier_delay_mean,
               ROUND(AVG(COALESCE({WEATHER_DELAY}, 0)), 2) AS weather_delay_mean,
               ROUND(AVG(COALESCE({NAS_DELAY}, 0)), 2) AS nas_delay_mean,
               ROUND(AVG(COALESCE({SECURITY_DELAY}, 0)), 2) AS security_delay_mean,
               ROUND(AVG(COALESCE({LATE_AIRCRAFT_DELAY}, 0)), 2) AS late_aircraft_delay_mean
        FROM flights
        WHERE {CANCELLED} = 0 AND {DIVERTED} = 0
        GROUP BY {CARRIER}
        HAVING COUNT(*) >= 500
        ORDER BY arrdelay_mean DESC
        LIMIT 10
    """
    
    result = spark.sql(query)

    if TIMED: 
        result.collect()
    else:
        export_results(result, OUTPUT_FORMAT, result_file)
        
        redis_data = [row.asDict() for row in result.collect()]
        write_results_on_redis(redis_data, f"query2_{FILE_FORMAT}_df")

    return time.time() - start_time