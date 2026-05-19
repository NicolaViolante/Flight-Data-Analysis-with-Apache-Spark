DEPS = cd spark_code && rm -f deps.zip && zip -r deps.zip . -x "*.pyc" "__pycache__/*" 

SUBMIT_QUERY = $(DEPS) && docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--py-files /opt/spark/work/deps.zip \
		/opt/spark/work/

up:
	docker-compose up -d --build

down:
	docker-compose down

reset: #elimina anche i volumi
	docker-compose down -v 

init-hdfs:
	@echo "Initializing HDFS..."
	docker exec -it hdfs-master bash -c "hdfs namenode -format && \$$HADOOP_HOME/sbin/start-dfs.sh"
	sleep 10
	docker exec -it hdfs-master hdfs dfsadmin -safemode leave
	docker exec -it hdfs-master hdfs dfs -chmod 777 /

start-hdfs:
	@echo "Restarting HDFS..."
	docker exec -it hdfs-master bash -c "\$$HADOOP_HOME/sbin/start-dfs.sh"
	sleep 10
	docker exec -it hdfs-master hdfs dfsadmin -safemode leave

clean-hdfs:
	@echo "Cleaning HDFS"
	docker exec -it hdfs-master hdfs dfs -rm -r -skipTrash /dataset_parquet || true
	docker exec -it hdfs-master hdfs dfs -rm -r -skipTrash /dataset_csv || true
	docker exec -it hdfs-master hdfs dfs -rm -r -skipTrash /results || true
	@echo "HDFS cleaned"

flow: #salva backup flow di nifi in locale
	docker cp nifi-server:/opt/nifi/nifi-current/conf/flow.json.gz ./nifi/flow.json.gz

restore-flow: #restore del flow nel container
	docker cp ./nifi/flow.json.gz nifi-server:/opt/nifi/nifi-current/conf/flow.json.gz
	docker exec -u root nifi-server chown nifi:nifi /opt/nifi/nifi-current/conf/flow.json.gz
	docker restart nifi-server

deps: 
	$(DEPS)

query:
	@if [ -z "$(q)" ] || [ -z "$(api)" ] || [ -z "$(format)" ]; then \
    	echo "ERROR: missing parameters"; \
        echo "Example of correct invocation: make query q=1 api=df format=csv"; \
    else \
        $(SUBMIT_QUERY)main.py --q $(q) --api $(api) --format $(format); \
    fi

perf:
	@if [ -z "$(q)" ] || [ -z "$(api)" ] || [ -z "$(format)" ]; then \
		echo "ERROR: missing parameters"; \
        echo "Example of correct invocation: make perf q=1 api=df format=csv"; \
	else \
		$(SUBMIT_QUERY)benchmark_runner.py --q $(q) --api $(api) --format $(format) --times $${times:-10}; \
	fi
	
all-query:
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

all-perf:
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

scale:
	@if [ -z "$(workers)" ]; then \
		echo "ERROR: missing parameters"; \
        echo "Example of correct invocation: make scale workers=3"; \
	else \
		echo "Scaling cluster with $(workers) Spark Workers..."; \
		docker-compose up -d --scale spark-worker=$(workers); \
	fi