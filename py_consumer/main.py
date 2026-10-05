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

def is_not_cached(dict_data):
    redis_con = redis.Redis(host='localhost', port=6379, decode_responses=True)
    try:
        cached =redis_con.hgetall(dict_data["alert_id"])
        data_without_id = {
                "source":dict_data["source"],
                "title":dict_data["title"],
                "content":dict_data["content"],
                "priority":dict_data["priority"],
                "classification":dict_data["classification"],
                "lat":dict_data["lat"],
                "lon":dict_data["lon"],
                "timestamp":dict_data["timestamp"],
                "status":dict_data["status"]
                }
        if cached == None or cached != data_without_id:
            redis_con.hset(dict_data["alert_id"],
                mapping=data_without_id)

            return True

        else:

            return False
    except Exception as e:
        print(e)
        return False
    finally:
        redis_con.close()

def is_valid(dict_data):
    if dict_data["priority"] not in ["CRITICAL",  "HIGH", "MEDIUM", "LOW"]:
        return False
    if dict_data["classification"] not in ["UNCLASSIFIED", "RESTRICTED", "SECRET", "TOP_SECRET"]:
        return False
    if float(dict_data["lat"])> 90 or float(dict_data["lat"]) < -90 :
        return False
    if float(dict_data["lon"])> 180 or float(dict_data["lon"]) < -180 :
        return False
    if dict_data["status"] != "WAITING":
        return False
    else:
        return True


def main():
    data_as_string=consumer()
    dict_data = json.loads(data_as_string)
    if not is_not_cached(dict_data) and is_valid(dict_data):
        pass


if __name__ == '__main__':
    main()
