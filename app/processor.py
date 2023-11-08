import json
import pickle
from datetime import datetime, timedelta
from typing import List, Dict, Any
from confluent_kafka import Consumer, Producer, KafkaError
from .missing_data_handler import MissingDataHandler
import copy
from datetime import datetime

class KafkaDataProcessor:
    def __init__(self, consumer: Consumer, producer: Producer, data_handler: MissingDataHandler):
        self.consumer = consumer
        self.data_handler = data_handler
        self.producer = producer

    def consume(self, topic: str):
        self.consumer.subscribe([topic])
        i = 1
        while True:
            msg = self.consumer.poll(10.0)
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
            logvalue = copy.copy(value)
            logvalue["data"] = None
            if value["type"] == "start":
                print(i)
                self._start()
            if value["type"] == "stop":
                print(i)
                i = 1
                self._flush(sampling_rate=20)
            if value["type"] == "trace":
                i+=1
                self.__process_received_data(value, arrive_time=datetime.utcnow())

    def __process_received_data(self, value: Dict[str, Any], arrive_time: datetime):
        station = value['station']
        channel = value['channel']
        eews_producer_time = value['eews_producer_time']
        data = value['data']
        start_time = datetime.fromisoformat(value['starttime'])
        sampling_rate = value['sampling_rate']
        if station == "BKB" and channel == "BHE":
            print("Received ", station, channel)
            print("from message: ",value['starttime'])
        self.data_handler.handle_missing_data(
            station, channel, start_time, sampling_rate)
        self.__store_data(station, channel, data, start_time, sampling_rate, 
                          eews_producer_time=eews_producer_time, 
                          arrive_time=arrive_time)

    def __store_data(self, station: str, channel: str, data: List[int], start_time: datetime, sampling_rate: float, eews_producer_time, arrive_time: datetime):
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
                station, channel, data_to_send, current_time, current_time + time_to_add, 
                eews_producer_time=eews_producer_time,
                arrive_time=arrive_time)
            current_time = current_time + time_to_add

        remaining_data_len = len(self.data_handler.data_pool[station][channel])
        # print(
        #     f"REMAINING DATA: {self.data_handler.data_pool[station][channel]}\nLEN: {remaining_data_len}")
        # print(
        #     f"LAST PROCESSED TIME: {self.data_handler.last_processed_time[station][channel]}")

    def __send_data_to_queue(self, station: str, channel: str, data: List[int], start_time: datetime, end_time: datetime, eews_producer_time, arrive_time: datetime):
        self.data_handler.update_last_processed_time(
            station, channel, end_time)
        self.producer.produce(station, channel, data, start_time, end_time, 
                              eews_producer_time=eews_producer_time, 
                              arrive_time=arrive_time)
    

    def _start(self):
        print("="*20, "START", "="*20)
        self.data_handler.data_pool = {}
        self.data_handler.last_processed_time = {}
        self.producer.startTrace()

    def _flush(self, sampling_rate):
        for station, stationDict in self.data_handler.data_pool.items():
            for channel, data_to_send in stationDict.items():
                end_time = self.data_handler.last_processed_time[station][channel]
                time_to_decrease = timedelta(seconds=len(data_to_send)/sampling_rate)
                start_time = end_time - time_to_decrease
                if station == "BKB" and channel == "BHE":
                    print("Flused ", station, channel, start_time, end_time)
                self.producer.produce(station, channel, data_to_send, start_time, end_time)
        print("="*20, "END", "="*20)
        self.producer.stopTrace()

