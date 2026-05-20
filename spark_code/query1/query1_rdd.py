from deps.utils import *
from deps.config import QUERY1_HEADER

def process_query1_rdd(df, spark):
    rdd = df.rdd # trasforma DataFrame in RDD, organizzati per riga come, le righe si comportano come tuple/dizionari

    def parse_row(row):
        try:
            carrier = row[CARRIER]
            month = row[MONTH]
            if carrier not in ["AA", "DL"] or month is None: return [] # se la compagnia non fa parte di quelle che analiziamo la funzione ritorna una lista vuota
            cancelled = row[CANCELLED] if row[CANCELLED] is not None else 0 # per sicurezza i Null interpretati come voli non cancellati
            if cancelled == 0 and row[DEP_DELAY] is not None:
                delay_sum = row[DEP_DELAY]
                delay_min = row[DEP_DELAY]
                delay_max = row[DEP_DELAY]
                return [((month, carrier), (delay_sum, 1, delay_min, delay_max, 0))]
            else:
                return [((month, carrier), (0.0, 0, float('inf'), float('-inf'), 1))]
        except:
            return []

    mapped_rdd = rdd.flatMap(parse_row)
    reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: ( # wide transformation
            x[0] + y[0], x[1] + y[1], min(x[2], y[2]), max(x[3], y[3]), x[4] + y[4]
        ))

    def format_results(item): #item è un tupla di tuple che ha questa forma ((month, carrier), (delay_sum, 1, delay_min, delay_max, 0))
        (month, carrier), stats = item 
        delay_sum, count_non_canc, delay_min, delay_max, count_canc = stats
        count_tot = count_non_canc + count_canc
        avg_delay = round(delay_sum / count_non_canc, 5) if count_non_canc > 0 else 0.0
        delay_min = delay_min if count_non_canc > 0 else 0.0
        delay_max = delay_max if count_non_canc > 0 else 0.0
        canc_rate = round((count_canc / count_tot) * 100, 5) if count_tot > 0 else 0.0
        return (month, carrier, avg_delay, delay_min, delay_max, canc_rate)
    final_rdd = reduced_rdd.map(format_results).sortBy(lambda x: (x[0], x[1]))

    return spark.createDataFrame(final_rdd, schema=QUERY1_HEADER)


def run(spark, dataset_path: str, file_format: str, timed: bool) -> float:
    return run_query(spark, dataset_path, file_format, timed, query_num=1, api_name="rdd", core_logic_func=process_query1_rdd)