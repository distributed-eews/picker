from confluent_kafka import Consumer, Producer, KafkaError
import json
from dotenv import load_dotenv
import os
import pickle


load_dotenv()

TOPIC_PRODUCER = os.getenv('TOPIC_PRODUCER')

class KafkaDataProcessor:
    def __init__(self, kafka_config):
        self.consumer = Consumer(kafka_config)
        self.producer = Producer(
            {'bootstrap.servers': kafka_config['bootstrap.servers']})
        self.data_pool = {}

    def consume(self, topic):
        self.consumer.subscribe([topic])

        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                print("No message received")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Error: {msg.error()}")
                continue

            data = pickle.loads(msg.value())
            data = json.loads(data)
            print(f"Data received: {data}")

            station = data['station']
            channel = data['channel']
            traces = data['data']

            if station not in self.data_pool:
                self.data_pool[station] = {}
            if channel not in self.data_pool[station]:
                self.data_pool[station][channel] = []

            self.data_pool[station][channel].extend(traces)

            while len(self.data_pool[station][channel]) >= 128:
                traces_to_send = self.data_pool[station][channel][:128]
                self.data_pool[station][channel] = self.data_pool[station][channel][128:]
                self.send_to_queue(station, channel, traces_to_send)

    def send_to_queue(self, station, channel, traces):
        data = {
            'station': station,
            'channel': channel,
            'data': traces
        }
        self.producer.produce(TOPIC_PRODUCER, key=station, value=json.dumps(data))


if __name__ == "__main__":
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')
    TOPIC_CONSUMER = os.getenv('TOPIC_CONSUMER')
    kafka_config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'my-group',
        'auto.offset.reset': 'earliest'
    }

    processor = KafkaDataProcessor(kafka_config)
    processor.consume(TOPIC_CONSUMER)
