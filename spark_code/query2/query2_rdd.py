from deps.utils import *
from deps.config import QUERY2_HEADER

def process_query2_rdd(df, spark):
    rdd = df.rdd
    def parse_row(row):
        try:
            canc = row[CANCELLED] if row[CANCELLED] is not None else 0
            div = row[DIVERTED] if row[DIVERTED] is not None else 0
            if canc == 1 or div == 1: return []
            carrier = row[CARRIER]
            arr = row[ARR_DELAY]
            carr = row[CARRIER_DELAY]
            wea = row[WEATHER_DELAY]
            nas = row[NAS_DELAY]
            sec = row[SECURITY_DELAY]
            late = row[LATE_AIRCRAFT_DELAY]
            return [(carrier, (1, arr, carr, wea, nas, sec, late))]
        except:
            return []

    mapped_rdd = rdd.flatMap(parse_row)
    reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: ( # trasformazione wide
        x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6]
    ))

    def calc_averages(item):
        carrier, stats = item
        count, arr_sum, carr_sum, wea_sum, nas_sum, sec_sum, late_sum = stats
        return (carrier, count, round(arr_sum / count, 5), round(carr_sum / count, 5),round(wea_sum / count, 5), round(nas_sum / count, 5),round(sec_sum / count, 5), round(late_sum / count, 5))

    top10_list = reduced_rdd.filter(lambda x: x[1][0] >= 500).map(calc_averages).top(10, key=lambda x: x[2])
    return spark.createDataFrame(top10_list, schema=QUERY2_HEADER)


def run(spark, dataset_path: str, file_format: str, timed: bool) -> float:
    return run_query(spark, dataset_path, file_format, timed, query_num=2, api_name="rdd", core_logic_func=process_query2_rdd)