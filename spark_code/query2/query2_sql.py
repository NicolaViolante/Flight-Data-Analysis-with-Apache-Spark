from deps.utils import *

def process_query2_sql(df, spark):
    """
    Logica core per Query 2 usando Spark SQL.
    Registra la view temporanea e lancia la query testuale.
    """
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
    
    return spark.sql(query)


def run(spark, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    """Entry point universale che richiama il wrapper in utils.py"""
    return run_query(
        spark=spark, 
        dataset_path=dataset_path, 
        file_format=FILE_FORMAT, 
        timed=TIMED, 
        query_num=2, 
        api_name="sql", 
        core_logic_func=process_query2_sql
    )