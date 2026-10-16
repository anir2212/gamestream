FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir 'sqlalchemy-trino==0.5.0' 'clickhouse-connect==0.8.18'
USER superset
