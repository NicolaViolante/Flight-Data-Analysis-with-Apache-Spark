import argparse
import importlib

# Importiamo le nostre nuove utility!
from deps.utils import get_spark, check_hdfs

if __name__ =="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    args = arg_parser.parse_args()

    try: 
        query_module = importlib.import_module(f'query{args.q}.query{args.q}_{args.api}')
    except KeyError: 
        raise Exception("Combinazione di query e api non valida.")

    # Creazione centralizzata di Spark
    spark, sc = get_spark(f"Query {args.q} - {args.api.upper()}")
    
    dataset_path = f"hdfs://hdfs-master:54310/dataset_{args.format}/"
    
    # Controllo di sicurezza HDFS (opzionale ma consigliato)
    # check_hdfs(sc, dataset_path) 

    tempo_esecuzione = query_module.run(spark, sc, dataset_path, args.format, TIMED=False)
    
    print(f"Tempo di esecuzione: {tempo_esecuzione:.2f} secondi")
    spark.stop()