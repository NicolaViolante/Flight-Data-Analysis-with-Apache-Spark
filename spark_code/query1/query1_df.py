from pyspark.sql import functions as F
from deps.utils import *

def process_query1_df(df, _spark):
    df_filtered = df.filter(F.col(CARRIER).isin("AA", "DL")) #filtra le righe, mantenendo solamente valori di AA e DL 

    result = df_filtered.groupBy(MONTH, CARRIER).agg( #innesca lo shuffle dei dati aggregando per mese e compagnia, successivamente aggrega i valori (wide)
        F.round(F.mean(F.when(F.col(CANCELLED) == 0, F.col(DEP_DELAY))), 5).alias("dep_delay_mean"),
        F.min(F.when(F.col(CANCELLED) == 0, F.col(DEP_DELAY))).alias("dep_delay_min"),
        F.max(F.when(F.col(CANCELLED) == 0, F.col(DEP_DELAY))).alias("dep_delay_max"),
        F.round((F.sum(CANCELLED) / F.count("*")) * 100, 5).alias("cancellation_rate")
    ).orderBy(MONTH, CARRIER) # ordina dataframe risultante per mese e per compagnia (a parità di mese) (wide)

    return result.withColumnRenamed("MONTH", "month").withColumnRenamed("OP_UNIQUE_CARRIER", "op_unique_carrier")


def run(spark, dataset_path: str, file_format: str, timed: bool) -> float:
    return run_query(spark, dataset_path, file_format, timed, query_num=1, api_name="df", core_logic_func=process_query1_df)