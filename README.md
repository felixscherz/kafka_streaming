# Kafka Data Engineering Challenge

## Components:
* a kafka cluster to act as a message broker
* a python container to publish recentchange events to the kafka broker in 0 to 1 second intervals
* a spark cluster for stream processing of the events
* a postgres instance to save the aggregated counts

## Instructions:

## Steps
To start the environment run ```docker-compose up```.
Then connect to the spark driver by running ```docker-compose exec spark sh```.
Inside the spark driver we can start the stream processing by running
```./submit.sh wikipedia_streaming.py```. 

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
setup watermark to limit state

### 4. setup database instance to write results to

what should the data model be:
events with the columns:
* timestamp
* counts
* FK_region

