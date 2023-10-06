FROM bitnami/spark
USER 1001
RUN curl https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar --output /opt/bitnami/spark/jars/delta-core_2.12-1.0.0.jar