import json
from datetime import datetime
from typing import Dict, Any, List
import copy
import time as t
import os
from confluent_kafka import Consumer, Producer
import requests
from dotenv import load_dotenv
from .pooler import Pooler
from .myredis import MyRedis
import numpy as np
from scipy.optimize import minimize


load_dotenv()


PRED_URL = os.getenv('PRED_URL', 'http://localhost:3000/predict')
INIT_URL = os.getenv('INIT_URL', 'http://localhost:3000/restart')
STAT_URL = os.getenv(
    'STAT_URL', 'http://localhost:3000/approx_earthquake_statistics')
REC_URL = os.getenv(
    'REC_URL', 'http://localhost:3000/recalculate')
TOPIC_PRODUCER = os.getenv('TOPIC_PRODUCER', 'pick')


class KafkaDataProcessor:
    def __init__(self, consumer: Consumer, producer: Producer, pooler: Pooler, redis: MyRedis):
        self.consumer = consumer
        self.producer = producer
        self.pooler = pooler
        self.redis = redis

    def consume(self, topic: str):
        self.consumer.subscribe([topic])
        show_nf = True
        while True:
            try:
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
            except Exception as e:
                print(f"ERROR: {str(e)}")
                print(("="*30) + "END" + ("="*30), end="\n")
                continue

    def __process_received_data(self, value: Dict[str, Any]):
        station = value['station']
        channel = value['channel']
        starttime = datetime.fromisoformat(value['starttime'])
        data = value['data']
        self.pooler.set_station_first_start_time(station, starttime)
        self.pooler.extend_data_points(station, channel, data)

        is_ready_to_init = self.pooler.is_ready_to_init(station)
        has_initiated = self.pooler.has_initiated(station)

        if not has_initiated and not is_ready_to_init:
            return

        if not has_initiated and is_ready_to_init:
            self.__init_station(station)
            return

        is_ready_to_predict = self.pooler.is_ready_to_predict(station)

        if is_ready_to_predict:
            self.__predict(station)

    def __init_station(self, station: str) -> None:
        stats_init = self.pooler.initiated_stations
        print(f"INITIATED STATIONS: {stats_init}")
        time = self.pooler.get_station_time(station)
        data = self.pooler.get_data_to_init(station)
        data = [list(x) for x in zip(*data)]

        res1 = self.__req(INIT_URL, {"station_code": station})
        res2 = self.__req(PRED_URL, {"station_code": station, "begin_time": time.strftime(
            "%Y-%m-%d %H:%M:%S.%f"), "x": data})

        if res1 and res2:
            self.pooler.add_initiated_station(station)
        else:
            self.pooler.reset_ps(station)

    def __predict(self, station: str) -> None:
        time = self.pooler.get_station_time(station)
        data = self.pooler.get_data_to_predict(station)
        data_t = [list(x) for x in zip(*data)]
        self.pooler.print_ps_info(station)

        res = self.__req(PRED_URL, {"station_code": station, "begin_time": time.strftime(
            "%Y-%m-%d %H:%M:%S.%f"), "x": data_t})

        if res is None:
            return

        result = res["result"]

        if not result["init_end"]:
            self.pooler.set_caches(station, data)
            return

        if not result["p_arr"]:
            self.pooler.set_caches(station, data)
            return

        p_arr = result["p_arr"]
        s_arr = result["s_arr"]

        if p_arr or s_arr:
            result = res["result"]
            result["process_time"] = res["process_time"]
            result["type"] = "ps"
            self.producer.produce(result)
        prev_p_time_exists = station in self.pooler.station_p_time
        prev_s_time_exists = station in self.pooler.station_s_time

        p_time = datetime.strptime(
            result["p_arr_time"], "%Y-%m-%d %H:%M:%S.%f")
        s_time = datetime.strptime(
            result["s_arr_time"], "%Y-%m-%d %H:%M:%S.%f")

        if not prev_p_time_exists and not prev_s_time_exists and p_arr and s_arr:
            self.pooler.set_caches(station, data, True)
            self.__pred_stats(station, time)
            return

        if prev_p_time_exists and not prev_s_time_exists:
            diff_secs = (
                time - self.pooler.station_p_time[station]).total_seconds()
            if (diff_secs >= 60 and not s_arr) or s_arr:
                self.pooler.set_caches(station, data, True)
                self.__pred_stats(station, time)
                return

        if not prev_p_time_exists and p_arr:
            self.pooler.station_p_time[station] = p_time

        if not prev_s_time_exists and s_arr:
            self.pooler.station_s_time[station] = s_time

        self.pooler.set_caches(station, data)

    def __pred_stats(self, station: str, time: datetime):
        data_cache = self.pooler.get_cache(station)
        data_cache_t = self.__transpose(data_cache)
        res = self.__req(
            STAT_URL, {"x": data_cache_t, "station_code": station})
        if res:
            result = res["result"]
            result["process_time"] = res["process_time"]
            self.redis.save_waveform(station, result)
            wf3 = self.redis.get_3_waveform(station)
            if wf3 is not None and len(wf3) == 3:
                epic = self.__get_epic_ml(wf3)
                payload = {
                    "time": time.isoformat(),
                    **epic,
                    # **wf3[0],
                    # "location": self.__get_epic(wf3),
                    # "magnitude": self.__get_mag(wf3),
                    "type": "params"
                }
                self.producer.produce(payload)

        self.pooler.reset_ps(station)

    def __req(self, url: str, data: Dict[str, Any], retry=3, timeout=3):
        print(f"REQUEST TO {url} with PAYLOAD {data}")
        for i in range(retry):
            start_time = datetime.now()
            try:
                print(f"RETRY {i + 1}")
                response = requests.post(
                    url, data=json.dumps(data), timeout=timeout)
                print(f"RESPONSE TEXT: {response.text}")
                end_time = datetime.now()
                process_time = (end_time - start_time).total_seconds()
                if response.text == "OK":
                    res = {
                        "process_time": process_time,
                        "result": {}
                    }
                    print(f"RESULT FROM 1 {url}: {res}")
                    return res
                result = json.loads(response.text)
                result = json.loads(result)
                if isinstance(result, dict):
                    res = {
                        "process_time": process_time,
                        "result": result
                    }
                    print(f"RESULT FROM 2 {url}: {res}")
                    return res
            except Exception as e:
                print(f"ERROR REQUEST: {str(e)}")
                t.sleep(1)
                continue
        return None

    def __transpose(self, data: List[List[Any]]):
        return [list(x) for x in zip(*data)]

    def to_cartesian(self, lat, lon):
        R = 6371  # Radius bumi dalam kilometer
        phi = np.deg2rad(90 - lat)
        theta = np.deg2rad(lon)

        x = R * np.sin(phi) * np.cos(theta)
        y = R * np.sin(phi) * np.sin(theta)
        z = R * np.cos(phi)

        return np.array([x, y, z])

    def trilaterate(self, p1, p2, p3, r1, r2, r3):
        def error(x, p, r):
            return np.linalg.norm(x - p) - r

        def total_error(x):
            return abs(error(x, p1, r1)) + abs(error(x, p2, r2)) + abs(error(x, p3, r3))

        initial_guess = (p1 + p2 + p3) / 3
        result = minimize(total_error, initial_guess, method='Nelder-Mead')
        print(result)

        if not result.success:
            raise ValueError("Optimization failed")

        x, y, z = result.x
        lat = 90 - np.rad2deg(np.arccos(z / 6371))
        lon = np.rad2deg(np.arctan2(y, x))

        return lat, lon

    def __get_epic(self, wf: list[dict]):
        stat1 = wf[0]
        stat2 = wf[1]
        stat3 = wf[2]

        loc1 = (stat1["location"][0], stat1["location"][1])
        loc2 = (stat2["location"][0], stat2["location"][1])
        loc3 = (stat3["location"][0], stat3["location"][1])

        r1 = stat1["distance"]
        r2 = stat2["distance"]
        r3 = stat3["distance"]

        p1 = self.to_cartesian(*loc1)
        p2 = self.to_cartesian(*loc2)
        p3 = self.to_cartesian(*loc3)

        epicenter = self.trilaterate(p1, p2, p3, r1, r2, r3)

        return [epicenter[0], epicenter[1]]

    def __get_mag(self, wf: list[dict]):
        mag = 0
        for w in wf:
            mag += w["magnitude"]

        return mag / 3

    def __get_epic_ml(self, wf: list[dict]):
        station_codes = []
        station_latitudes = []
        station_longitudes = []
        magnitudes = []
        distances = []
        depths = []

        for w in wf:
            station_codes.append(w["station_code"])
            station_latitudes.append(wf["location"][0])
            station_longitudes.append(wf["location"][1])
            magnitudes.append(float(wf["magnitude"]))
            distances.append(float(wf["distance"]))
            depths.append(float(wf["depth"]))

        res = self.__req(REC_URL, {
            "station_codes": station_codes,
            "station_latitudes": station_latitudes,
            "station_longitudes": station_longitudes,
            "magnitudes": magnitudes,
            "distances": distances,
            "depths": depths
        })

        if res is not None:
            return res
        return {}
