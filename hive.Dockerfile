FROM apache/hive:4.0.0

USER root

RUN curl -fSL \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -o /opt/hive/lib/postgresql-42.7.3.jar \
  && chmod 644 /opt/hive/lib/postgresql-42.7.3.jar

USER hive