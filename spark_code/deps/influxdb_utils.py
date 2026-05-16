from datetime import datetime, timezone
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql.dataframe import DataFrame

from deps.config import INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET, InfluxWriterConfig

def get_influx_client():
    """
    Crea e restituisce il client InfluxDB.
    Centralizza la logica di connessione per evitare duplicazioni di codice.
    """
    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        return client, write_api
    except Exception as e:
        print(f"Errore connessione InfluxDB: {e}")
        return None, None

def write_results_on_influxdb(df_res: DataFrame, measurement: str, config: InfluxWriterConfig):
    """
    Salva i risultati analitici delle query (es. Query 1) su InfluxDB.
    """
    client, write_api = get_influx_client()
    if not client or not write_api:
        print("❌ Impossibile connettersi a InfluxDB. Salvataggio saltato.")
        return

    print("--- INVIO A INFLUXDB IN CORSO ---")
    punti_inviati = 0

    for row in df_res.collect():
        try:
            # Cast ipersicuro del mese (il 2025 è l'anno del dataset)
            mese = int(float(row[config.timestamp_col]))
            timestamp = datetime(year=2025, month=mese, day=1, tzinfo=timezone.utc)
            
            point = Point(measurement).time(timestamp, WritePrecision.S)
            
            # Aggiunta dei Tags (es. OP_UNIQUE_CARRIER)
            for i in config.tag_cols:   
                point.tag(config.header[i], str(row[i]).strip())
                
            # Aggiunta dei Fields (I valori numerici aggregati)
            ha_valori = False
            for i in config.field_cols: 
                valore = row[i]
                if valore is not None:
                    point.field(config.header[i], float(valore))
                    ha_valori = True
            
            if ha_valori:
                print(f"✅ Inviando: {point.to_line_protocol()}")
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
                punti_inviati += 1
            else:
                print(f"❌ Riga scartata (valori tutti nulli): {row}")
        except Exception as e:
            print(f"⚠️ Errore sulla riga {row}: {e}")

    client.close()
    print(f"[*] {punti_inviati} punti inviati a InfluxDB con successo!\n")


def write_job_time_on_influxdb(write_api, measurement: str, job_time: float, run_num: int):
    """
    Salva i tempi di esecuzione del benchmark su InfluxDB.
    Non apre/chiude il client da sola, ma riceve la write_api dal benchmark_runner
    per massimizzare la velocità nei cicli di test.
    """
    try:
        # Crea un punto di misurazione per il tempo
        p = Point(measurement) \
            .tag("run_type", "benchmark") \
            .field("execution_time", float(job_time)) \
            .field("run_number", int(run_num))
        
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=p)
    except Exception as e:
        print(f"⚠️ Errore scrittura benchmark su InfluxDB: {e}")