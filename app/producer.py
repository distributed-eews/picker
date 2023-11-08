import json
import os
from confluent_kafka import Producer
from dotenv import load_dotenv
import copy
load_dotenv()
from datetime import datetime

TOPIC_PRODUCER = os.getenv('TOPIC_PRODUCER')


class KafkaProducer:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def produce(self, station, channel, data, start_time, end_time, eews_producer_time=[], arrive_time=datetime.utcnow()):
        data = {
            'station': station,
            'channel': channel,
            'starttime': start_time.isoformat(),
            'endtime': end_time.isoformat(),
            'data': data,
            'len': len(data),
            'eews_producer_time': eews_producer_time,
            'eews_queue_time':[arrive_time.isoformat(), datetime.utcnow().isoformat()]
        }
        if station == "BKB" and channel == "BHE":
            print(("=" * 20) + f"{station}____{channel}" + ("="*20))
            print('packet time: ',[data['starttime'], data['endtime']])
            print('eews_producer_time: ', data['eews_producer_time'])
            print('eews_queue_time: ', data['eews_queue_time'])
        self.producer.produce(TOPIC_PRODUCER, key=station,
                              value=json.dumps(data))
        
    def startTrace(self):
        self.producer.produce(
            topic=TOPIC_PRODUCER,
            key="start",
            value=json.dumps({"type":"start"}),
        )
        print("="*40, "Start Trace", "="*40)

    def stopTrace(self):
        self.producer.produce(
            topic=TOPIC_PRODUCER,
            key="stop",
            value=json.dumps({"type":"stop"}),
        )
        print("="*40, "Stop Trace", "="*40)
