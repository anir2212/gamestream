FROM apache/airflow:2.10.5-python3.12
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir 'dbt-trino==1.9.3' 'pyspark==3.5.7' 'fastavro==1.13.1' 'confluent-kafka==2.16.0' 'trino==0.334.0'
ENV PYTHONPATH=/opt/gamestream
WORKDIR /opt/gamestream
