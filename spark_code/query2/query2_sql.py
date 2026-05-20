from deps.utils import *

def process_query2_sql(df, spark):
    df.createOrReplaceTempView("flights")
    query = f"""
        SELECT {CARRIER} AS carrier,
               COUNT(*) AS num_flights,
               ROUND(AVG({ARR_DELAY}), 5) AS arrdelay_mean,
               ROUND(AVG(COALESCE({CARRIER_DELAY}, 0.0)), 5) AS carrier_delay_mean,
               ROUND(AVG(COALESCE({WEATHER_DELAY}, 0.0)), 5) AS weather_delay_mean,
               ROUND(AVG(COALESCE({NAS_DELAY}, 0.0)), 5) AS nas_delay_mean,
               ROUND(AVG(COALESCE({SECURITY_DELAY}, 0.0)), 5) AS security_delay_mean,
               ROUND(AVG(COALESCE({LATE_AIRCRAFT_DELAY}, 0.0)), 5) AS late_aircraft_delay_mean
        FROM flights
        WHERE {CANCELLED} = 0 AND {DIVERTED} = 0
        GROUP BY {CARRIER}
        HAVING COUNT(*) >= 500
        ORDER BY arrdelay_mean DESC
        LIMIT 10
    """
    return spark.sql(query)


def run(spark, dataset_path: str, file_format: str, timed: bool) -> float:
    return run_query(spark, dataset_path, file_format, timed, query_num=2, api_name="sql", core_logic_func=process_query2_sql)