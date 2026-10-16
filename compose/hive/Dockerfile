FROM apache/hive:4.0.1
USER root
ADD https://jdbc.postgresql.org/download/postgresql-42.7.7.jar /opt/hive/lib/postgres.jar
RUN chmod 644 /opt/hive/lib/postgres.jar
RUN ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar \
    && ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
USER hive
