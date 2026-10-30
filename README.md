# Flight Data Analysis with Apache Spark

<p align="left">
  <img src="https://img.shields.io/badge/Apache%20NiFi-729B1B?style=for-the-badge&logo=Apache%20NiFi&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Hadoop-66CC00?style=for-the-badge&logo=apachehadoop&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=InfluxDB&logoColor=white" />
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" />
  <img src="https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white" />
</p>

## 📝 Overview
This repository contains an end-to-end containerized Big Data pipeline designed for the **batch processing and analysis** of historical flight data. 

The system extracts, transforms, and analyzes a massive dataset of approximately 2.2 million US domestic flight events (January–April 2025). The core computational engine leverages Apache Spark to evaluate complex analytical queries, such as monthly delay/cancellation trends and severe delay carrier rankings. A primary objective of this project is a comprehensive performance evaluation: it rigorously benchmarks execution times to compare the efficiency of different Spark APIs (RDD, DataFrame, Spark SQL) and storage formats (row-based CSV vs. columnar Parquet) under varying degrees of horizontal scalability.

## ⚙️ System Architecture & Data Pipelines
The infrastructure is orchestrated entirely via **Docker Compose** and is logically partitioned into five specialized layers:

- **Ingestion Layer (Apache NiFi):**
  - **Apache NiFi** fully automates the ETL process via programmatic REST API triggers. It fetches the remote compressed archive, filters out unwanted files, and applies schema-on-read SQL queries (`Query Record`) to aggressively reduce the data footprint. 
  - To prevent the Hadoop "small files problem" and avoid NameNode memory overhead, it uses a bin-packing algorithm (`MergeContent`) to aggregate data into a single cohesive dataset before routing it to storage.
- **Storage Layer (Hadoop HDFS):**
  - A distributed file system configured with one Master and three Worker nodes ensures data replication and fault tolerance. It persists the cleansed datasets in both standard **CSV** and highly optimized, dictionary-encoded **Parquet** formats to support downstream comparative benchmarking.
- **Processing Layer (Apache Spark):**
  - A standalone **Apache Spark** cluster (Master + Workers) designed with a strict separation of concerns between I/O operations and business logic.
  - It executes queries across three paradigms: declarative APIs (**DataFrame** and **Spark SQL**, heavily accelerated by the Catalyst Optimizer through predicate pushdown and local limits) and the imperative **RDD** API (utilizing manual map-side combines). The cluster dynamically tunes shuffle partitions at runtime based on the underlying hardware topology to avoid over-partitioning latency.
- **Polyglot Serving Layer (InfluxDB & Redis):**
  - **InfluxDB:** A time-series database configured to store the continuous chronological aggregations of Query 1 (e.g., monthly delays) and the execution time metrics collected during automated benchmark runs.
  - **Redis:** An in-memory key-value store chosen to guarantee extremely low-latency access for Query 2's static, rank-based top-10 carrier leaderboards.
- **Visualization Layer (Grafana):**
  - **Grafana** connects simultaneously to both databases to render interactive observability dashboards, plotting flight disruptions and benchmark metrics. It includes an integrated Grafana Image Renderer container for seamless panel exports.

## 🚀 Quick Start
To spin up the infrastructure and prepare the data, run the following commands in sequence in your terminal.

**1. Start the Docker containers:** `make up`

**2. Initialize the distributed file system:** `make init-hdfs`

**3. Restore NiFi flow:** `make restore-flow`

**4. Start data ingestion:** `make nifi-start`

## 💻 Query Execution and Benchmarking
Once the pipeline is ready and the data is available on HDFS, you can interact with Spark in standard mode or benchmark mode.

### Standard mode
Executes the query once and exports the results on HDFS, locally, and to InfluxDB or Redis (depending on the query).

**Syntax:** `make query q=<1|2> api=<rdd|df|sql> format=<csv|parquet>`

> **Note:** `workers` is an optional parameter.

### Benchmark mode
Executes the query N times (default 10) to collect execution time metrics, automatically handling JVM cold starts. It supports dynamic scaling of Spark Workers. The timing results are logged directly into InfluxDB.

**Syntax:** `make perf q=<1|2> api=<rdd|df|sql> format=<csv|parquet> workers=<num>`

> **Note:** `times` is an optional parameter.

## 🌐 Web Interfaces (UI)
Once the project is up and running, you can monitor the various services through the following web interfaces:
- **Grafana:** [http://localhost:3000](http://localhost:3000)
- **Apache Spark Master:** [http://localhost:8080](http://localhost:8080)
- **HDFS NameNode:** [http://localhost:9870](http://localhost:9870)
- **NiFi:** [https://localhost:8443](https://localhost:8443)
- **InfluxDB:** [http://localhost:8086](http://localhost:8086)

## 🧹 Teardown
To shut down the infrastructure and safely remove the associated containers, network and volumes: 

`make reset`

---
*Developed by Nicola Violante as an academic project at the Department of Computer Engineering, University of Rome Tor Vergata.*
