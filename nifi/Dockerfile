# 1. Inchiodiamo NiFi alla versione 2.4.0
FROM apache/nifi:2.4.0

USER root

# 2. Scarichiamo chirurgicamente le versioni 2.4.0 per evitare qualsiasi bug
RUN apt-get update && apt-get install -y wget && \
    echo "Scaricando i plugin versione 2.4.0..." && \
    wget -q --show-progress https://repo1.maven.org/maven2/org/apache/nifi/nifi-parquet-nar/2.4.0/nifi-parquet-nar-2.4.0.nar -P /opt/nifi/nifi-current/lib/ && \
    wget -q --show-progress https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-nar/2.4.0/nifi-hadoop-nar-2.4.0.nar -P /opt/nifi/nifi-current/lib/ && \
    wget -q --show-progress https://repo1.maven.org/maven2/org/apache/nifi/nifi-hadoop-libraries-nar/2.4.0/nifi-hadoop-libraries-nar-2.4.0.nar -P /opt/nifi/nifi-current/lib/ && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    chown nifi:nifi /opt/nifi/nifi-current/lib/nifi-*.nar

USER nifi