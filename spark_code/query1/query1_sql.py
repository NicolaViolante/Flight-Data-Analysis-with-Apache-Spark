from deps.utils import *

def process_query1_sql(df, spark):
    """
    Logica core per Query 1 usando Spark SQL.
    Registra la view temporanea e lancia la query testuale.
    """
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
        ORDER BY {MONTH}, {CARRIER}
    """
    
    return spark.sql(query)


def run(spark, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    """Entry point universale che richiama il wrapper in utils.py"""
    return run_query(
        spark=spark, 
        dataset_path=dataset_path, 
        file_format=FILE_FORMAT, 
        timed=TIMED, 
        query_num=1, 
        api_name="sql", 
        core_logic_func=process_query1_sql
    )