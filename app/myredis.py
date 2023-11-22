from typing import Any
import redis
import json


class MyRedis:
    def __init__(self, config: dict[str, Any]):
        self.c = redis.Redis(**config)

    def get_nearest_stations(self, station: str) -> list[str]:
        n = self.c.get(f'NEAREST_STATION_{station}')
        if n is None or n == "":
            return []
        return n.split(",")

    def has_2_or_more_nearest_stations(self, station: str) -> bool:
        n = self.get_nearest_stations(station)
        return len(n) >= 2

    def save_waveform(self, station: str, waveform: dict):
        self.c.set(f"WAVEFORM_{station}", json.dumps(waveform), 10)

    def get_waveform(self, station: str) -> dict | None:
        r = self.c.get(f"WAVEFORM_{station}")
        if r is None:
            return None
        return json.loads(r)

    def has_3_waveform(self, station: str) -> bool:
        nearest_stats = self.get_nearest_stations(station)
        has_2_nearest = len(nearest_stats) >= 2
        if not has_2_nearest:
            return False

        stats = nearest_stats.extend(station)
        for stat in stats:
            r = self.get_waveform(stat)
            if r is None:
                return False
        return True

    def get_loc(self, station: str):
        loc = self.c.get(f"LOCATION_{station}")
        if loc is None:
            return None
        loc = loc.split(";")
        loc = [float(l) for l in loc]
        return loc

    def get_3_waveform(self, station: str):
        if not self.has_3_waveform(station):
            return None

        nearest_stats = self.get_nearest_stations(station)
        stats = nearest_stats.extend(station)

        data = []

        for stat in stats:
            wf = self.get_waveform(stat)
            loc = self.get_loc(stat)
            if wf is None or loc is None:
                return None
            wf["location"] = loc
            wf["distance"] = float(wf["distance"])
            data.append(wf)
        return data