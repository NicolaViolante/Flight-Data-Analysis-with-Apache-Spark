# Usa la stessa versione del tuo docker-compose
FROM apache/spark:3.5.1

USER root

# Installazione delle librerie richieste
RUN pip install influxdb-client redis

# Torna all'utente spark per sicurezza
USER 185