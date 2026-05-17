# ==========================================
# Variabili di Ambiente 
# ==========================================
DEPS = cd spark_code && rm -f deps.zip && zip -r deps.zip . -x "*.pyc" "__pycache__/*"

SUBMIT_QUERY = $(DEPS) && docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--py-files /opt/spark/work/deps.zip \
		/opt/spark/work/

# ==========================================
# Comandi Rapidi - Progetto SABD
# ==========================================

# 1. Accende l'infrastruttura
up:
	docker-compose up -d --build

# 2. Spegne tutto
down:
	docker-compose down

# 3. Spegne tutto e resetta i dati (elimina i volumi)
reset:
	docker-compose down -v

# 4. INIZIALIZZA HDFS (Da lanciare solo la prima volta o dopo un "make reset")
init-hdfs:
	@echo "Formattazione e primo avvio HDFS..."
	docker exec -it hdfs-master bash -c "hdfs namenode -format && \$$HADOOP_HOME/sbin/start-dfs.sh"
	@echo "Attendo l'avvio (10s)..."
	sleep 10
	docker exec -it hdfs-master hdfs dfsadmin -safemode leave
	docker exec -it hdfs-master hdfs dfs -chmod 777 /

# 5. AVVIA HDFS (Da lanciare se avevi spento il PC ma NON avevi fatto il reset)
start-hdfs:
	@echo "Riavvio HDFS..."
	docker exec -it hdfs-master bash -c "\$$HADOOP_HOME/sbin/start-dfs.sh"
	@echo "Attendo l'avvio (10s)..."
	sleep 10
	@echo "Sblocco SafeMode per consentire a NiFi di scrivere..."
	docker exec -it hdfs-master hdfs dfsadmin -safemode leave

# 6. PULIZIA HDFS
clean-hdfs:
	@echo "Pulizia della cartella / su HDFS in corso..."
	docker exec -it hdfs-master hdfs dfs -rm -r -skipTrash /dataset_parquet || true
	docker exec -it hdfs-master hdfs dfs -rm -r -skipTrash /dataset_csv || true
	@echo "Pulizia completata!"

# ==========================================
# GESTIONE NiFi 
# ==========================================

# Salva la configurazione di NiFi sul Mac
flow:
	docker cp nifi-server:/opt/nifi/nifi-current/conf/flow.json.gz ./nifi/flow.json.gz

# Ripristina la configurazione dal Mac a NiFi (da lanciare se fai make reset)
restore-flow:
	docker cp ./nifi/flow.json.gz nifi-server:/opt/nifi/nifi-current/conf/flow.json.gz
	docker exec -u root nifi-server chown nifi:nifi /opt/nifi/nifi-current/conf/flow.json.gz
	docker restart nifi-server

# ==========================================
# GESTIONE SPARK 
# ==========================================

deps: 
	$(DEPS)

query:
	@if [ -z "$(q)" ] || [ -z "$(api)" ]; then \
		echo "Esempio: make query q=1 api=df format=csv"; \
	else \
		$(SUBMIT_QUERY)main.py --q $(q) --api $(api) --format $${format:-csv}; \
	fi

perf:
	@if [ -z "$(q)" ] || [ -z "$(api)" ]; then \
		echo "Esempio: make perf q=1 api=df times=10 format=csv"; \
	else \
		$(SUBMIT_QUERY)benchmark_runner.py --q $(q) --api $(api) --format $${format:-csv} --times $${times:-10}; \
	fi
	
# ==========================================
# ESECUZIONI BATCH E SCALABILITÀ
# ==========================================

all_query:
	$(SUBMIT_QUERY)main.py --q 1 --api rdd --format csv
	$(SUBMIT_QUERY)main.py --q 1 --api df  --format csv
	$(SUBMIT_QUERY)main.py --q 1 --api sql --format csv
	$(SUBMIT_QUERY)main.py --q 2 --api rdd --format csv
	$(SUBMIT_QUERY)main.py --q 2 --api df  --format csv
	$(SUBMIT_QUERY)main.py --q 2 --api sql --format csv
	$(SUBMIT_QUERY)main.py --q 1 --api rdd --format parquet
	$(SUBMIT_QUERY)main.py --q 1 --api df  --format parquet
	$(SUBMIT_QUERY)main.py --q 1 --api sql --format parquet
	$(SUBMIT_QUERY)main.py --q 2 --api rdd --format parquet
	$(SUBMIT_QUERY)main.py --q 2 --api df  --format parquet
	$(SUBMIT_QUERY)main.py --q 2 --api sql --format parquet

all_perf:
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api rdd --format csv --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api df  --format csv --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api sql --format csv --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api rdd --format csv --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api df  --format csv --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api sql --format csv --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api rdd --format parquet --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api df  --format parquet --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 1 --api sql --format parquet --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api rdd --format parquet --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api df  --format parquet --times 10
	$(SUBMIT_QUERY)benchmark_runner.py --q 2 --api sql --format parquet --times 10

# Test di scalabilità (aggiunge o rimuove container worker a caldo)
scale:
	@if [ -z "$(workers)" ]; then \
		echo "Uso: make scale workers=3"; \
	else \
		echo "Scalando il cluster a $(workers) Spark Worker..."; \
		docker-compose up -d --scale spark-worker=$(workers); \
	fi