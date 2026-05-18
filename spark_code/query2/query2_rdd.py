import time

from deps.export_utils import export_results
from deps.redis_utils import write_results_on_redis
from deps.utils import *

def run(spark, sc, dataset_path: str, FILE_FORMAT: str, TIMED: bool) -> float:
    start_time = time.time()
    OUTPUT_FORMAT = "csv"
    result_file = f"hdfs://hdfs-master:54310/dataset_results/query2_rdd.{OUTPUT_FORMAT}"

    df = get_df(spark, dataset_path, FILE_FORMAT)
    rdd = df.rdd

    def parse_row(row):
        try:
            canc = row[CANCELLED] if row[CANCELLED] is not None else 0
            div = row[DIVERTED] if row[DIVERTED] is not None else 0
            if canc == 1 or div == 1: return []
            
            carrier = row[CARRIER]
            
            def get_val(col_name):
                val = row[col_name]
                return float(val) if val is not None else 0.0
                
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

    if not TIMED: 
        schema = ["carrier", "num_flights", "arrdelay_mean", "carrier_delay_mean", 
                  "weather_delay_mean", "nas_delay_mean", "security_delay_mean", "late_aircraft_delay_mean"]
        
        res_df = spark.createDataFrame(top10_list, schema=schema)
        export_results(res_df, OUTPUT_FORMAT, result_file)
        
        redis_data = [dict(zip(schema, row)) for row in top10_list]
        write_results_on_redis(redis_data, "query2_rdd")

    return time.time() - start_time