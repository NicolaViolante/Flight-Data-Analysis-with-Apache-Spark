import argparse
import importlib
from deps.influxdb_utils import write_job_time_on_influxdb, get_influx_client
from deps.utils import get_spark, check_hdfs

def print_logs(run_time: float, num_run: int, times: int):
    if not hasattr(print_logs, "time_sum"):
        print_logs.time_sum = 0
    print_logs.time_sum += run_time
    run_width = len(str(times))
    remaining_seconds = (print_logs.time_sum / num_run) * (times - num_run)
    secs  = remaining_seconds % 60
    mins  = int((remaining_seconds % 3600) // 60)
    hours = int(remaining_seconds // 3600)
    print(f"Run {num_run:>{run_width}d}/{times} - Time elapsed: {run_time:7.4f} s - Expected remaining time: {hours:2d}h {mins:2d}m {secs:5.2f}s")

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q", type=int, choices=[1, 2], required=True)
    arg_parser.add_argument("--api", type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"], required=True)
    arg_parser.add_argument("--times", type=int, default=30, required=False)
    arg_parser.add_argument("--workers", type=int, default=1, required=False)
    args = arg_parser.parse_args()
    shuffle_partitions = args.workers * 2 * 2 # la documentazione di spark suggerisce 2/3 task per core, ogni worker nel mio caso ha 2 core assegnati
    try: 
        query = importlib.import_module(f'query{args.q}.query{args.q}_{args.api}')
    except KeyError: 
        raise Exception("Not legit combination")
    dataset_path = f"hdfs://hdfs-master:54310/dataset_{args.format}/"
    print(f"Benchmark start: Query {args.q}, api: {args.api}, format: {args.format}, workers: {args.workers}, partitions: {shuffle_partitions}")
    client, write_api = get_influx_client()
    for num_run in range(1, args.times + 1):
        spark = get_spark(f"benchmark_query{args.q}_{args.format}_{args.api}_run{num_run}", partitions=shuffle_partitions) #start della SparkSession (per non far usare dati in RAM precedenti)
        if num_run == 1:
            check_hdfs(spark, dataset_path)
        run_time = query.run(spark, dataset_path, args.format, timed=True) #evita salvataggio dei dati
        spark.stop() #per svuotare la RAM e non inficiare i risultati dei successivi giri
        print_logs(run_time, num_run, args.times)
        if write_api:
            write_job_time_on_influxdb(write_api, f"perf_query{args.q}_{args.api}_{args.format}", run_time, num_run, args.workers) #salvataggio dei risultati su Influx
    if client:
        client.close()    
    print("\nBenchmark completed")