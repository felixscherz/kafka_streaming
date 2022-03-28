# Kafka Data Engineering Challenge

## Components:
* a kafka cluster to act as a message broker
* a python container to publish recentchange events to the kafka broker in 0 to 1 second intervals
* a spark cluster for stream processing of the events
* a postgres instance to save the aggregated counts

## Instructions:

## Steps
To start the environment run
```
docker-compose up
```
Then connect to the spark driver by running
```
docker-compose exec spark sh
```
Inside the spark driver we can start the stream processing by running
```
./submit.sh wikipedia_streaming.py
``` 
This will start the stream processing. The spark cluster will continuously
listen for new messages on the kafka topic, group them by region, count
the events and save them to the postgres database.
