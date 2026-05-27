import argparse
import importlib
from deps.utils import get_spark, check_hdfs

if __name__ =="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q", type=int, choices=[1, 2], required=True)
    arg_parser.add_argument("--api", type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"], required=True)
    args = arg_parser.parse_args()
    shuffle_partitions = args.workers * 2 * 2

    try: 
        query = importlib.import_module(f'query{args.q}.query{args.q}_{args.api}') # importazione a runtime della query da eseguire
    except KeyError: 
        raise Exception("Not legit combination")
    spark = get_spark(f"query{args.q}_{args.format}_{args.api}", partitions=shuffle_partitions) # creazione SparkSession per la query specifica
    dataset_path = f"hdfs://hdfs-master:54310/dataset_{args.format}/"
    check_hdfs(spark, dataset_path) 
    execution_time = query.run(spark, dataset_path, args.format, timed=False)
    print(f"Execution time: {execution_time:.2f} seconds")
    spark.stop()