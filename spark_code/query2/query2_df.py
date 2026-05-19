from pyspark.sql import functions as F
from deps.utils import *

def process_query2_df(df, _spark):
    """
    Logica core per Query 2 usando l'API DataFrame.
    Calcola le medie dei ritardi e la classifica delle top 10 compagnie.
    """
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
    
    return result.withColumnRenamed("OP_UNIQUE_CARRIER", "carrier")


def run(spark, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    """Entry point universale che richiama il wrapper in utils.py"""
    return run_query(
        spark=spark, 
        dataset_path=dataset_path, 
        file_format=FILE_FORMAT, 
        timed=TIMED, 
        query_num=2, 
        api_name="df", 
        core_logic_func=process_query2_df
    )