from pyspark.sql.dataframe import DataFrame

def write_results_on_hdfs(df_res: DataFrame, file_format: str, result_file: str):
    """
    Salva il DataFrame su HDFS. 
    Usa coalesce(1) per avere un singolo file CSV/Parquet invece di molti frammenti.
    """
    print(f"[*] Salvataggio su HDFS in corso: {result_file}")
    
    # Aggiungiamo .option("header", "true") per forzare la scrittura dell'intestazione nei CSV
    df_res.coalesce(1).write.mode("overwrite") \
          .option("header", "true") \
          .format(file_format).save(result_file)