import csv
from pyspark.sql.dataframe import DataFrame

def export_results(results: DataFrame, file_format: str, result_file: str):

    #Salvataggio risultati query su HDFS
    print(f"Saving on HDFS: {result_file}")
    try:
        results.coalesce(1).write.mode("overwrite").option("header", "true").format(file_format).save(result_file)
        print("Results saved successfully on HDFS")
    except Exception as e:
        print(f"Results NOT saved on HDFS: {e}")
        return 
    
    #Salvataggio risultati query in locale
    try:
        folder = "query1" if "query1" in result_file else "query2"
        filename = result_file.split("/")[-1]
        path = f"/opt/spark/work/{folder}/{filename}" #utilizzo Bind Mount per scrivere csv 
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(results.columns)
            for row in results.collect():
                writer.writerow(row)       
        print(f"Results saved successfully in local")
    except Exception as e:
        print(f"Results NOT saved in local: {e}")