import json
import pickle
from datetime import datetime, timedelta
from typing import List, Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError
from .missing_data_handler import MissingDataHandler


class KafkaDataProcessor:
    def __init__(self, consumer: Consumer, producer: Producer, data_handler: MissingDataHandler):
        self.consumer = consumer
        self.data_handler = data_handler
        self.producer = producer

    def consume(self, topic: str):
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

            value = pickle.loads(msg.value())
            value = json.loads(value)
            print(f"Data received: {value}")

            self.__process_received_data(value)

    def __process_received_data(self, value: Dict[str, Any]):
        station = value['station']
        channel = value['channel']
        data = value['data']
        start_time = datetime.fromisoformat(value['starttime'])
        sampling_rate = value['sampling_rate']

        self.data_handler.handle_missing_data(
            station, channel, start_time, sampling_rate)
        self.__store_data(station, channel, data, start_time, sampling_rate)

    def __store_data(self, station: str, channel: str, data: List[int], start_time: datetime, sampling_rate: float):
        if station not in self.data_handler.data_pool:
            self.data_handler.data_pool[station] = {}
        if channel not in self.data_handler.data_pool[station]:
            self.data_handler.data_pool[station][channel] = []

        current_time = start_time
        if station in self.data_handler.last_processed_time and channel in self.data_handler.last_processed_time[station]:
            current_time = self.data_handler.last_processed_time[station][channel]

        self.data_handler.data_pool[station][channel].extend(data)

        while len(self.data_handler.data_pool[station][channel]) >= 128:
            data_to_send = self.data_handler.data_pool[station][channel][:128]
            self.data_handler.data_pool[station][channel] = self.data_handler.data_pool[station][channel][128:]
            time_to_add = timedelta(seconds=128/sampling_rate)
            self.__send_data_to_queue(
                station, channel, data_to_send, current_time, current_time + time_to_add)
            current_time = current_time + time_to_add

        remaining_data_len = len(self.data_handler.data_pool[station][channel])
        print(
            f"REMAINING DATA: {self.data_handler.data_pool[station][channel]}\nLEN: {remaining_data_len}")
        print(
            f"LAST PROCESSED TIME: {self.data_handler.last_processed_time[station][channel]}")

    def __send_data_to_queue(self, station: str, channel: str, data: List[int], start_time: datetime, end_time: datetime):
        self.data_handler.update_last_processed_time(
            station, channel, end_time)
        self.producer.produce(station, channel, data, start_time, end_time)