import csv
from pyspark.sql.dataframe import DataFrame

def write_results_on_hdfs(df_res: DataFrame, file_format: str, result_file: str):
    """
    Salva il DataFrame su HDFS e in contemporanea salva una copia 
    locale come singolo file CSV pulito sul Mac.
    """
    print(f"[*] Salvataggio su HDFS in corso: {result_file}")
    
    # 1. Salvataggio su Hadoop (HDFS) per rispettare le specifiche
    df_res.coalesce(1).write.mode("overwrite") \
          .option("header", "true") \
          .format(file_format).save(result_file)
          
    # 2. Salvataggio IN LOCALE per tua comodità
    try:
        # Capisce automaticamente se stiamo eseguendo la Query 1 o 2 dal nome del file
        query_folder = "query1" if "query1" in result_file else "query2"
        
        # /opt/spark/work è la cartella spark_code sul tuo Mac!
        local_path = f"/opt/spark/work/{query_folder}/results.csv"
        
        # Creiamo un file CSV singolo e pulito
        with open(local_path, "w", newline="") as f:
            writer = csv.writer(f)
            # Scrive l'intestazione
            writer.writerow(df_res.columns)
            # Scrive i dati
            for row in df_res.collect():
                writer.writerow(row)
                
        print(f"[*] Salvataggio LOCALE completato: Vallo a vedere nella cartella {query_folder}/results.csv su VSCode!")
    except Exception as e:
        print(f"⚠️ Errore durante il salvataggio locale: {e}")