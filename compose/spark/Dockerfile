FROM apache/spark:3.5.7-python3
USER root
RUN python3 -m pip install --no-cache-dir --upgrade 'pip<26' \
    && python3 -m pip install --no-cache-dir fastavro==1.9.7 'confluent-kafka[schema-registry]==2.16.0'
WORKDIR /opt/gamestream
COPY spark ./spark
ENV PYTHONPATH=/opt/gamestream
USER 185
