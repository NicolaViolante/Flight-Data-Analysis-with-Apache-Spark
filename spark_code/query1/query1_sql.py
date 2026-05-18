import time

from deps.export_utils import export_results
from deps.influxdb_utils import write_results_on_influxdb
from deps.config import QUERY1_CONFIG
from deps.utils import *

def run(spark, sc, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    start_time = time.time()
    OUTPUT_FORMAT = "csv"
    result_file = f"hdfs://hdfs-master:54310/dataset_results/query1_sql.{OUTPUT_FORMAT}"

    df = get_df(spark, dataset_path, FILE_FORMAT)
    df.createOrReplaceTempView("flights")

    # Usiamo le f-string e le costanti per evitare errori di battitura nei nomi delle colonne!
    query = f"""
        SELECT {MONTH} AS month, {CARRIER} AS op_unique_carrier,
               ROUND(AVG(CASE WHEN {CANCELLED} = 0 THEN {DEP_DELAY} END), 2) AS dep_delay_mean,
               MIN(CASE WHEN {CANCELLED} = 0 THEN {DEP_DELAY} END) AS dep_delay_min,
               MAX(CASE WHEN {CANCELLED} = 0 THEN {DEP_DELAY} END) AS dep_delay_max,
               ROUND((SUM({CANCELLED}) / COUNT(*)) * 100, 2) AS cancellation_rate
        FROM flights
        WHERE {CARRIER} IN ('AA', 'DL')
        GROUP BY {MONTH}, {CARRIER}
        ORDER BY CAST({MONTH} AS INT), {CARRIER}
    """
    
    result = spark.sql(query)

    if TIMED: 
        result.collect()
    else:
        export_results(result, OUTPUT_FORMAT, result_file)
        write_results_on_influxdb(result, "query1_sql", QUERY1_CONFIG)

    return time.time() - start_time