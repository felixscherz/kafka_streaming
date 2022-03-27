FROM docker.io/bitnami/spark:3
COPY ./postgresql-42.3.1.jar /opt/bitnami/spark/jars
USER root
COPY ./submit.sh /opt/bitnami/spark
RUN chmod +x /opt/bitnami/spark/submit.sh

