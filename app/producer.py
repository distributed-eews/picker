import json
import os
from confluent_kafka import Producer
from dotenv import load_dotenv
import copy
load_dotenv()

TOPIC_PRODUCER = os.getenv('TOPIC_PRODUCER')


class KafkaProducer:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def produce(self, station, channel, data, start_time, end_time):
        data = {
            'station': station,
            'channel': channel,
            'starttime': start_time.isoformat(),
            'endtime': end_time.isoformat(),
            'data': data,
            'len': len(data)
        }
        print(("=" * 20) + f"{station}____{channel}" + ("="*20))
        logvalue = copy.copy(data)
        logvalue["data"] = None
        print(logvalue)
        self.producer.produce(TOPIC_PRODUCER, key=station,
                              value=json.dumps(data))
