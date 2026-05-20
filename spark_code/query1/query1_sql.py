from deps.utils import *

def process_query1_sql(df, spark):
    df.createOrReplaceTempView("flights")

    query = f"""
        SELECT {MONTH} AS month, {CARRIER} AS op_unique_carrier,
               ROUND(AVG(CASE WHEN {CANCELLED} = 0 THEN {DEP_DELAY} END), 5) AS dep_delay_mean,
               MIN(CASE WHEN {CANCELLED} = 0 THEN {DEP_DELAY} END) AS dep_delay_min,
               MAX(CASE WHEN {CANCELLED} = 0 THEN {DEP_DELAY} END) AS dep_delay_max,
               ROUND((SUM({CANCELLED}) / COUNT(*)) * 100, 5) AS cancellation_rate
        FROM flights
        WHERE {CARRIER} IN ('AA', 'DL')
        GROUP BY {MONTH}, {CARRIER}
        ORDER BY {MONTH}, {CARRIER}
    """
    
    return spark.sql(query)


def run(spark, dataset_path: str, file_format: str, timed: bool) -> float:
    return run_query(spark, dataset_path, file_format, timed, query_num=1, api_name="sql", core_logic_func=process_query1_sql)