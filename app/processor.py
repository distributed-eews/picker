import json
from datetime import datetime
from typing import Dict, Any
import copy
import time as t
import os
from confluent_kafka import Consumer, Producer
import requests
from dotenv import load_dotenv
from .pooler import Pooler

load_dotenv()

P_URL = os.getenv('P_URL', 'http://localhost:3000/predict_p')
S_URL = os.getenv('S_URL', 'http://localhost:3000/predict_s')
TOPIC_PRODUCER = os.getenv('TOPIC_PRODUCER', 'pick')


class KafkaDataProcessor:
    def __init__(self, consumer: Consumer, producer: Producer, pooler: Pooler):
        self.consumer = consumer
        self.producer = producer
        self.pooler = pooler

    def consume(self, topic: str):
        self.consumer.subscribe([topic])
        show_nf = True
        while True:
            msg = self.consumer.poll(0.1)
            if msg is None:
                if show_nf:
                    print("No message received")
                show_nf = False
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            show_nf = True
            value = json.loads(msg.value())
            logvalue = copy.copy(value)
            logvalue["data"] = None
            if "type" in value and value["type"] == "start":
                self.pooler.reset()
                continue
            if "type" in value and value["type"] != "trace":
                continue

            print(("="*30) + "START" + ("="*30), end="\n")
            print(f"RECEIVED MESSAGE: {logvalue}", end="\n")
            self.__process_received_data(value)
            print(("="*30) + "END" + ("="*30), end="\n")

    def __process_received_data(self, value: Dict[str, Any]):
        station = value['station']
        channel = value['channel']
        starttime = datetime.fromisoformat(value['starttime'])
        data = value['data']
        self.pooler.set_station_first_start_time(station, starttime)
        self.pooler.extend_data_points(station, channel, data)

        is_ready_to_predict = self.pooler.is_ready_to_predict(station)

        if is_ready_to_predict:
            data_points = self.pooler.get_data_to_predict(station)
            print(f"DATA POINTS: {data_points}")
            time = self.pooler.get_station_time(station)
            payload = {"x": data_points, "metadata": {
                "f": 20.0, "time": str(time), "station": station}}

            result_p = self.__predict_ps(P_URL, payload)
            if not result_p:
                return

            is_p_detected = self.__does_ps_exist(result_p["result"])
            if not is_p_detected:
                return

            value_to_produce = {
                "station": station,
                "channel": channel,
                "time": str(time),
                "type": "p",
                "process_time": result_p["process_time"]
            }
            self.producer.produce(value_to_produce)

            payload.update({"reset": True})
            result_s = self.__predict_ps(S_URL, payload)
            if not result_s:
                return

            is_s_detected = self.__does_ps_exist(result_s["result"])
            if is_s_detected:
                value_to_produce.update(
                    {"type": "s", "process_time": result_s["process_time"]})
                self.producer.produce(value_to_produce)

    def __predict_ps(self, url: str, payload: dict, retry=3):
        res = None
        for i in range(0, retry):
            print(f"RETRING {i}: {url}")
            start_time = datetime.now()
            try:
                response = requests.post(
                    url,
                    data=json.dumps(payload),
                    headers={"content-type": "application/json"},
                    timeout=3
                )
            except Exception as e:
                print(f"ERROR REQUEST: {str(e)}")
                continue
            print(f"RESPONSE TEXT: {response.text}")
            end_time = datetime.now()
            process_time = (end_time - start_time).total_seconds()
            result = json.loads(response.text)
            if isinstance(result, list):
                res = {
                    "process_time": process_time,
                    "result": result
                }
                print(f"RESULT FROM {url}: {res}")
                return res
            res = result
            t.sleep(1)
        print(f"RESULT FROM {url}: {res}")
        return None

    def __does_ps_exist(self, result_ps: list[list[int]]):
        for ls in result_ps:
            for p in ls:
                if float(p) > 0.5:
                    return True
        return False
