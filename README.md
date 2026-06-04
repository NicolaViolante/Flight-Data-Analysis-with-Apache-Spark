# Flight Data Analysis with Apache Spark
This project implements an end-to-end containerized Big Data pipeline for the batch analysis of historical flight data

The architecture manages the entire data lifecycle using:
* **Ingestion:** Apache NiFi
* **Storage:** Hadoop HDFS (CSV and Parquet)
* **Processing:** Apache Spark (RDD, DataFrame, SQL)
* **Serving:** InfluxDB (Time-Series) and Redis (Key-Value)
* **Visualization:** Grafana

---

## Quick start
To spin up the infrastructure and prepare the data, run the following commands in sequence in your terminal.

**1. Start the Docker containers:** `make up`

**2. Initialize the distributed file system:** `make init-hdfs`

**3. Restore NiFi flow:** `make restore-flow`

**4. Start data download and processing:** `make nifi-start`

---

## Query execution and benchmarking

Once the pipeline is ready and the data is available on HDFS, you can interact with Spark in standard mode or benchmark mode

### Standard mode

Executes the query once and exports the results on HDFS, locally, and to InfluxDB or Redis (depending on the query)

**Syntax:** `make query q=<1|2> api=<rdd|df|sql> format=<csv|parquet>`

Note: workers is an aptional parameter

**Benchmark mode**

Executes the query N times (default 10) to collect execution time metrics. It supports dynamic scaling of Spark Workers. The timing results are logged into InfluxDB

**Syntax:** `make perf q=<1|2> api=<rdd|df|sql> format=<csv|parquet> workers=<num>`

Note: times is an aptional parameter

---

## Web Interfaces (UI)

Once the project is up and running, you can monitor the various services through the following web interfaces:
* **Grafana:** [http://localhost:3000](http://localhost:3000)
* **Apache Spark Master:** [https://localhost:8080](https://localhost:8080)
* **HDFS NameNode:** [http://localhost:9870](http://localhost:9870)
* **NiFi:** [https://localhost:8443](https://localhost:8443)
* **InfluxDB:** [http://localhost:8086](http://localhost:8086)

---

## Teardown

To shut down the infrastructure and safely remove the associated containers, network and volumes: 

`make reset`
