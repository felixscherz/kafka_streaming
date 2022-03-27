## Steps

1. setup kafka environment
2. setup python environment that publishes example data
3. setup pyspark environment to stream process the events
4. setup a database to store the results for future analysis

### 1. setup kafka environment

get kafka compose file from bitnami:
`curl -sSL https://raw.githubusercontent.com/bitnami/bitnami-docker-kafka/master/docker-compose.yml > docker-compose.yml`
set `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`

### 2. setup python environment

basic Dockerfile using `kafka-python` to connect to the running kafka cluster
reads the sample data using pandas, then randomly selects a record
to publish to kafka

### 3. setup pyspark environment for stream processing

imporant to add packages to spark-submit script to enable connection
to kafka cluster
query.awaitTermination required
