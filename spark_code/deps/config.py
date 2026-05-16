import os

INFLUXDB_URL    = "http://influxdb-server:8086"
INFLUXDB_ORG    = "sabd_org"
INFLUXDB_BUCKET = "query_results"

# NESSUN VALORE DI RISERVA! Legge rigorosamente dalle variabili d'ambiente (dal tuo .env)
INFLUXDB_TOKEN  = os.environ["INFLUX_TOKEN"]

class InfluxWriterConfig:
    def __init__(self, header, time_format, timestamp_col, tag_cols, field_cols):
        self.header = header
        self.time_format = time_format
        self.timestamp_col = timestamp_col
        self.tag_cols = tag_cols
        self.field_cols = field_cols

QUERY1_HEADER = ["MONTH", "OP_UNIQUE_CARRIER", "dep_delay_mean", "dep_delay_min", "dep_delay_max", "cancellation_rate"]

QUERY1_CONFIG = InfluxWriterConfig(
    header=QUERY1_HEADER,
    time_format="%m",
    timestamp_col=0,
    tag_cols=[1],
    field_cols=[2, 3, 4, 5]
)