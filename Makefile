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

# 5. PULIZIA HDFS
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

# 1. Zippa tutti i file "extra" (es. moduli di utilità) per mandarli ai Worker
deps: #crea zip da mandare ai nodi worker
	cd spark_code && rm -f deps.zip && zip -r deps.zip . -x "*.pyc" "__pycache__/*" 

# 2. Comando base di submit con parametri e dipendenze
submit: deps
	@if [ -z "$(script)" ]; then \
		echo "Uso: make submit script=main.py args='--q 1 --api df'"; \
	else \ 
		docker exec -it spark-master /opt/spark/bin/spark-submit \
			--master spark://spark-master:7077 \
			--py-files /opt/spark/work/deps.zip \
			/opt/spark/work/$(script) $(args); \
	fi

# 3. La VERA scorciatoia per testare tutte le API (RDD, DF, SQL)
# Uso: make query q=1 api=df
query:
	@if [ -z "$(q)" ] || [ -z "$(api)" ]; then \
		echo "Devi specificare query e API! Esempio: make query q=1 api=df"; \
	else \
		echo "Eseguendo Query $(q) con API $(api)..."; \
		make submit script=main.py args="--q $(q) --api $(api) --format csv"; \
	fi