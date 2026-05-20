from pyspark.sql import functions as F
from deps.utils import *

def process_query2_df(df, _spark):
    df_filtered = df.filter((F.col(CANCELLED) == 0) & (F.col(DIVERTED) == 0))
    grouped = df_filtered.groupBy(CARRIER).agg( #trasformazione wide
        F.count("*").alias("num_flights"),
        F.round(F.mean(ARR_DELAY), 5).alias("arrdelay_mean"),
        F.round(F.mean(F.coalesce(CARRIER_DELAY, F.lit(0.0))), 5).alias("carrier_delay_mean"),
        F.round(F.mean(F.coalesce(WEATHER_DELAY, F.lit(0.0))), 5).alias("weather_delay_mean"),
        F.round(F.mean(F.coalesce(NAS_DELAY, F.lit(0.0))), 5).alias("nas_delay_mean"),
        F.round(F.mean(F.coalesce(SECURITY_DELAY, F.lit(0.0))), 5).alias("security_delay_mean"),
        F.round(F.mean(F.coalesce(LATE_AIRCRAFT_DELAY, F.lit(0.0))), 5).alias("late_aircraft_delay_mean")
    )
    result = grouped.filter(F.col("num_flights") >= 500).orderBy(F.col("arrdelay_mean").desc()).limit(10) # orderby wide, limit agisce prima in locale e poi in globale
    return result.withColumnRenamed("OP_UNIQUE_CARRIER", "carrier")


def run(spark, dataset_path: str, file_format: str, timed: bool) -> float:
    return run_query(spark, dataset_path, file_format, timed, query_num=2, api_name="df", core_logic_func=process_query2_df)