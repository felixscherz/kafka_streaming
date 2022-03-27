from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
import pandas as pd
import random
import os

print('init publisher')

KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'recentchanges')
KAFKA_CONNECTION = os.environ.get('KAFKA_CONNECTION', 'localhost:9092')
EXAMPLE_DATA_FPATH = os.environ.get('EXAMPLE_DATA_FPATH',
                                    './data/de_challenge_sample_data.csv')


def init_producer(connection: str) -> KafkaProducer:
    connected = False
    while not connected:
        try: 
            print('trying to connect to kafka cluster...')
            producer = KafkaProducer(
                bootstrap_servers=[connection],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            connected = True
        except NoBrokersAvailable:
            connected = False
            print("no brokers available yet, retrying in 3s ...")
            time.sleep(3)
    print('connection succesfull')
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
    print('entering publishing loop')
    while True:
        # print('sending event')
        event = get_sample_event(event_records)
        producer.send(KAFKA_TOPIC, value=event)
        time.sleep(random.random())


if __name__ == '__main__':
    main()
