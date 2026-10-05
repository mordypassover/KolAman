import json
import redis

from confluent_kafka import Consumer

def consumer():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'kafka-python',
        'auto.offset.reset': 'earliest'
    }

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "raw-data"
    consumer.subscribe([topic])
    try:
        # while True:m
        mesege = consumer.consume()
        mesege_content = mesege[0].value().decode('utf-8')
        print(mesege_content)
        return mesege_content
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

def caching_check(data_as_string):

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def validator(dict_data):
    pass


def main():
    data_as_string=consumer()

    dict_data = json.loads(data_as_string)


if __name__ == '__main__':
    main()