import argparse
import importlib

from deps.influxdb_utils import write_job_time_on_influxdb, get_influx_client
from deps.utils import get_spark

def print_logs(run_time: float, num_run: int, times: int):
    """Calcola la stima del tempo rimanente e la stampa a schermo."""
    if not hasattr(print_logs, "time_sum"):
        print_logs.time_sum = 0
    print_logs.time_sum += run_time

    run_width = len(str(times))
    # Calcolo tempo rimanente basato sulla media mobile
    remaining_seconds = (print_logs.time_sum / num_run) * (times - num_run)
    secs  = remaining_seconds % 60
    mins  = int((remaining_seconds % 3600) // 60)
    hours = int(remaining_seconds // 3600)

    print(f"Run {num_run:>{run_width}d}/{times} - Tempo: {run_time:7.4f} s - Fine stimata in: {hours:02d}h {mins:02d}m {secs:05.2f}s")

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    arg_parser.add_argument("--times",  type=int, default=30,                   required=False)
    args = arg_parser.parse_args()

    QUERY = args.q
    API = args.api
    TIMES = args.times
    FILE_FORMAT = args.format

    try: 
        query_module = importlib.import_module(f'query{QUERY}.query{QUERY}_{API}')
    except KeyError: 
        raise Exception("Combinazione di query e api non valida.")

    dataset_path = f"hdfs://hdfs-master:54310/dataset_{FILE_FORMAT}/*"
    
    print(f"\n==========================================================")
    print(f" INIZIO BENCHMARK: Query {QUERY} | API: {API.upper()} | Formato: {FILE_FORMAT.upper()} ")
    print(f"==========================================================\n")

    client, write_api = get_influx_client()

    for num_run in range(1, TIMES + 1):
        # 1. Start Spark Session
        spark, sc = get_spark(f"Benchmark Q{QUERY}-{API} Run {num_run}")
        
        # 2. Esegui la query misurando i tempi (TIMED=True evita il salvataggio dei risultati CSV)
        run_time = query_module.run(spark, sc, dataset_path, FILE_FORMAT, TIMED=True)
        
        # 3. Spegni Spark per pulire la Cache per il prossimo round
        spark.stop()

        # 4. Log a schermo
        print_logs(run_time, num_run, TIMES)

        # 5. Salva il tempo su InfluxDB
        if write_api:
            write_job_time_on_influxdb(write_api, f"perf_query{QUERY}_{API}_{FILE_FORMAT}", run_time, num_run)

    if client:
        client.close()
        
    print("\nBenchmark completato e salvato su InfluxDB! 🎉\n")