import os

#Configurazione per connessione a InfluxDB
INFLUXDB_URL    = "http://influxdb-server:8086"
INFLUXDB_ORG    = "it.uniroma2"
INFLUXDB_BUCKET = "query_results"
INFLUXDB_TOKEN  = os.environ["INFLUX_TOKEN"]

class InfluxWriterConfig:
    def __init__(self, header, time_format, timestamp_col, tag_cols, field_cols):
        self.header = header
        self.time_format = time_format
        self.timestamp_col = timestamp_col
        self.tag_cols = tag_cols
        self.field_cols = field_cols

QUERY1_HEADER = ["month", "op_unique_carrier", "dep_delay_mean", "dep_delay_min", "dep_delay_max", "cancellation_rate"]
QUERY2_HEADER = ["carrier", "num_flights", "arrdelay_mean", "carrier_delay_mean", "weather_delay_mean", "nas_delay_mean", "security_delay_mean", "late_aircraft_delay_mean"]

QUERY1_CONFIG = InfluxWriterConfig(
    header=QUERY1_HEADER,
    time_format="%m",
    timestamp_col=0,
    tag_cols=[1],
    field_cols=[2, 3, 4, 5]
)