FROM bitnami/spark:3.5.1

USER root

RUN install_packages python3-pip curl && \
    pip3 install --no-cache-dir kafka-python clickhouse-connect python-dotenv

COPY infra/spark/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

WORKDIR /opt/project

USER 1001
