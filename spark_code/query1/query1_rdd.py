import time

from deps.hdfs_utils import write_results_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.config import QUERY1_CONFIG, QUERY1_HEADER

# Importiamo get_df e tutte le costanti
from deps.utils import *

def run(spark, sc, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    start_time = time.time()
    OUTPUT_FORMAT = "csv"
    result_file = f"hdfs://hdfs-master:54310/dataset_results/query1_rdd.{OUTPUT_FORMAT}"

    # get_df si occupa della lettura e di tutti i cast ai tipi corretti
    df = get_df(spark, dataset_path, FILE_FORMAT)
    rdd = df.rdd

    def parse_row(row):
        try:
            # Usiamo le costanti definite in utils.py
            carrier = row[CARRIER]
            if carrier not in ["AA", "DL"]: return []
            
            month = int(row[MONTH])
            cancelled = row[CANCELLED] if row[CANCELLED] is not None else 0
            
            if cancelled == 0 and row[DEP_DELAY] is not None:
                delay = float(row[DEP_DELAY])
                return [((month, carrier), (delay, 1, delay, delay, 0, 1))]
            else:
                return [((month, carrier), (0.0, 0, float('inf'), float('-inf'), 1, 1))]
        except:
            return []

    mapped_rdd = rdd.flatMap(parse_row)
    reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: (
        x[0] + y[0], x[1] + y[1], min(x[2], y[2]), max(x[3], y[3]), x[4] + y[4], x[5] + y[5]
    ))

    def format_results(item):
        (month, carrier), stats = item
        sum_delay, count_non_canc, min_d, max_d, sum_canc, count_tot = stats
        avg_delay = round(sum_delay / count_non_canc, 2) if count_non_canc > 0 else 0.0
        min_d = min_d if count_non_canc > 0 else 0.0
        max_d = max_d if count_non_canc > 0 else 0.0
        canc_rate = round((sum_canc / count_tot) * 100, 2) if count_tot > 0 else 0.0
        return (str(month), carrier, float(avg_delay), float(min_d), float(max_d), float(canc_rate))

    final_rdd = reduced_rdd.map(format_results).sortBy(lambda x: (int(x[0]), x[1]))

    if TIMED: 
        final_rdd.collect()
    else:
        res_df = spark.createDataFrame(final_rdd, schema=QUERY1_HEADER)
        write_results_on_hdfs(res_df, OUTPUT_FORMAT, result_file)
        write_results_on_influxdb(res_df, "query1_rdd", QUERY1_CONFIG)

    return time.time() - start_time