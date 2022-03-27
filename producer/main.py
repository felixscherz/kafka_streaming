from kafka import KafkaProducer
import time
import json
import pandas as pd
import random

KAFKA_TOPIC = 'recentchanges'
KAFKA_CONNECTION = 'kafka:9092'
EXAMPLE_DATA_FPATH = './data/de_challenge_sample_data.csv'


def init_producer(connection: str) -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=[connection],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    return producer


def read_example_data(fpath: str) -> list:
    events_raw = pd.read_csv(fpath)
    event_records = events_raw.drop('Unnamed: 0',
                                    axis=1).to_dict(orient='records')
    return event_records


def get_sample_event(event_records: list) -> dict:
    event: dict = random.choice(event_records)
    return event


def main() -> None:
    producer = init_producer(KAFKA_CONNECTION)
    event_records = read_example_data(EXAMPLE_DATA_FPATH)
    while True:
        event = get_sample_event(event_records)
        producer.send(KAFKA_TOPIC, value=event)
        time.sleep(random.random())


if __name__ == '__main__':
    main()
