import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F

# ==========================================
# COSTANTI: Nomi delle Colonne 
# (Evita errori di battitura e rende il codice pulito)
# ==========================================
CARRIER   = "OP_UNIQUE_CARRIER"
MONTH     = "MONTH"
DEP_DELAY = "DEP_DELAY"
ARR_DELAY = "ARR_DELAY"
CANCELLED = "CANCELLED"
DIVERTED  = "DIVERTED"

# Cause di ritardo
CARRIER_DELAY       = "CARRIER_DELAY"
WEATHER_DELAY       = "WEATHER_DELAY"
NAS_DELAY           = "NAS_DELAY"
SECURITY_DELAY      = "SECURITY_DELAY"
LATE_AIRCRAFT_DELAY = "LATE_AIRCRAFT_DELAY"

DELAY_COLS = [ARR_DELAY, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY]

# ==========================================
# FUNZIONI DI UTILITÀ
# ==========================================

def get_spark(app_name: str):
    """Crea e restituisce la SparkSession e lo SparkContext con configurazioni ottimali."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    # Esempio di configurazione utile per l'efficienza degli shuffle
    spark.conf.set("spark.sql.shuffle.partitions", "200") 
    return spark, spark.sparkContext

def check_hdfs(sc: SparkContext, path: str):
    """Controlla se un file esiste su HDFS per evitare crash tardivi."""
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    
    fs = FileSystem.get(URI(path), Configuration())
    if not fs.exists(Path(path)):
        print(f"ERRORE: Il dataset in {path} non esiste su HDFS!")
        sys.exit(1)

def get_df(spark: SparkSession, dataset_path: str, file_format: str):
    """
    Legge il dataset e applica automaticamente tutti i cast necessari 
    per le analisi, così non devi farlo in ogni singola query.
    """
    df = spark.read.format(file_format).option("header", "true").load(dataset_path)
    
    # Cast per la cancellazione e deviazione
    df = df.withColumn(CANCELLED, F.col(CANCELLED).cast("int")) \
           .withColumn(DIVERTED, F.col(DIVERTED).cast("int")) \
           .withColumn(DEP_DELAY, F.col(DEP_DELAY).cast("float"))
           
    # Cast per tutte le cause di ritardo in modo iterativo
    for col in DELAY_COLS:
        df = df.withColumn(col, F.col(col).cast("float"))
        
    return df