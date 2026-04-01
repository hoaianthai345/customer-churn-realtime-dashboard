FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir kafka-python clickhouse-connect python-dotenv

COPY infra/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/project
