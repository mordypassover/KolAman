import json
import redis
import geopandas as gpd
from shapely.geometry import Point
import asyncio
from rstream import Producer
from confluent_kafka import Consumer

# 5GB
STREAM_RETENTION = 5000000000


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
            redis_con.expire(dict_data["alert_id"], 1800)

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


def get_region_with_geopandas(file_path: str, lon: float, lat: float) -> str:
    # 1. טעינת קובץ ה-GeoJSON ל-GeoDataFrame
    gdf = gpd.read_file(file_path)

    # 2. יצירת נקודה מתאימה
    pt = Point(lon, lat)

    # 3. סינון השורות שהפוליגון שלהן מכיל את הנקודה
    matched = gdf[gdf.geometry.contains(pt)]

    # 4. החזרת שם האזור אם נמצאה התאמה, אחרת OVERSEAS
    if not matched.empty:
        return matched.iloc[0]["region"]
    return "OVERSEAS"


async def publish(dict_data, stream_name):
    async with Producer(
            host="localhost",
            username="guest",
            password="guest",
    ) as producer:
        await producer.create_stream(
            stream_name, exists_ok=True, arguments={"MaxLengthBytes": STREAM_RETENTION})
        await producer.send(stream=stream_name, message=(json.dumps(dict_data)).encode('utf-8'))

def main():

    data_as_string=consumer()
    dict_data = json.loads(data_as_string)

    if is_not_cached(dict_data) and is_valid(dict_data):

        region = get_region_with_geopandas(
            r".\..\alert-simulator\regions.geojson",
                    lon = dict_data["lon"],
                    lat = dict_data["lat"])
        asyncio.run(publish(dict_data, stream_name=region))
        print(f"sent mesege to streem:{region.lower()}")


if __name__ == '__main__':
    main()
