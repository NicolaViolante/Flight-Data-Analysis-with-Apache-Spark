from datetime import datetime, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql.dataframe import DataFrame
from deps.config import INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET, InfluxWriterConfig

def get_influx_client():

    try:
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        return client, write_api
    except Exception as e:
        print(f"InfluxDB connection error: {e}")
        return None, None



def write_results_on_influxdb(results: DataFrame, measurement: str, config: InfluxWriterConfig):
    
    client, write_api = get_influx_client()
    if not client or not write_api:
        print("Unable to connect to InfluxDB")
        return
    for row in results.collect():
        try:
            month = int(float(row[config.timestamp_col]))
            timestamp = datetime(year=2025, month=month, day=1, tzinfo=timezone.utc) #costruzione artificiale data
            point = Point(measurement).time(timestamp, WritePrecision.S) #costruzione punto da inviare a Influx
            for i in config.tag_cols:   
                point.tag(config.header[i], str(row[i]).strip()) # .strip() rimuove spazi vuoti
            has_values = False
            for i in config.field_cols: 
                value = row[i]
                if value is not None:
                    point.field(config.header[i], float(value))
                    has_values = True
            if has_values:
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        except Exception as e:
            print(f"Error processing row {row}: {e}")
    client.close()
    print(f"Data sent successfully to InflxDB")



def write_job_time_on_influxdb(write_api, measurement: str, job_time: float, run_num: int, workers: int):

    try:
        # Crea un punto di misurazione per il tempo
        point = Point(measurement).tag("run_type", "benchmark").tag("workers", str(workers)).field("execution_time", float(job_time)).field("run_number", int(run_num))
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    except Exception as e:
        print(f"Error saving benchmark to InfluxDB: {e}")