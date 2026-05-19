from deps.utils import *
from deps.config import QUERY2_HEADER

def process_query2_rdd(df, spark):
    """
    Logica core per Query 2 usando l'API RDD.
    Esegue il MapReduce e restituisce il DataFrame finale con le Top 10 compagnie.
    """
    rdd = df.rdd

    def parse_row(row):
        try:
            canc = row[CANCELLED] if row[CANCELLED] is not None else 0
            div = row[DIVERTED] if row[DIVERTED] is not None else 0
            if canc == 1 or div == 1: return []
            
            carrier = row[CARRIER]
            
            def get_val(col_name):
                val = row[col_name]
                return val if val is not None else 0.0
                
            arr = get_val(ARR_DELAY)
            carr = get_val(CARRIER_DELAY)
            wea = get_val(WEATHER_DELAY)
            nas = get_val(NAS_DELAY)
            sec = get_val(SECURITY_DELAY)
            late = get_val(LATE_AIRCRAFT_DELAY)
            
            return [(carrier, (1, arr, carr, wea, nas, sec, late))]
        except:
            return []

    mapped_rdd = rdd.flatMap(parse_row)
    reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: (
        x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6]
    ))

    def calc_averages(item):
        carrier, stats = item
        count = stats[0]
        return (carrier, count, 
                round(stats[1]/count, 2), round(stats[2]/count, 2),
                round(stats[3]/count, 2), round(stats[4]/count, 2),
                round(stats[5]/count, 2), round(stats[6]/count, 2))

    top10_list = reduced_rdd.filter(lambda x: x[1][0] >= 500) \
                            .map(calc_averages) \
                            .sortBy(lambda x: x[2], ascending=False) \
                            .take(10)

    return spark.createDataFrame(top10_list, schema=QUERY2_HEADER)


def run(spark, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    """Entry point universale che richiama il wrapper in utils.py"""
    return run_query(
        spark=spark, 
        dataset_path=dataset_path, 
        file_format=FILE_FORMAT, 
        timed=TIMED, 
        query_num=2, 
        api_name="rdd", 
        core_logic_func=process_query2_rdd
    )