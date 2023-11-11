from typing import Dict, List
from datetime import datetime, timedelta

class Pooler:
    def __init__(self):
        self.data_points: Dict[str, Dict[str, List[int]]] = {}
        self.station_first_start_time: Dict[str, datetime] = {}
        self.station_data_count: Dict[str, int] = {}

    def set_station_first_start_time(self, station: str, start_time: datetime):
        if not station in self.station_first_start_time:
            self.station_first_start_time[station] = start_time

    def extend_data_points(self, station: str, channel: str, data_points: List[int]):
        if not station in self.data_points:
            self.data_points[station] = {}

        if not channel in self.data_points[station]:
            self.data_points[station][channel] = []

        self.data_points[station][channel].extend(data_points)

    def is_ready_to_predict(self, station: str) -> bool:
        if not station in self.data_points:
            return False
         
        if len(self.data_points[station]) != 3:
            return False

        for channel in self.data_points[station]:
            if len(self.data_points[station][channel]) < 128:
                return False
        return True
    
    def inc_station_data_count(self, station: str, count: int):
        if station not in self.station_data_count:
            self.station_data_count[station] = 0

        self.station_data_count[station] += count

    def get_station_time(self, station: str) -> datetime:
        now = datetime.utcnow()

        if not station in self.station_first_start_time:
            return now
        
        if not station in self.station_data_count:
            return now
        
        first_time = self.station_first_start_time[station]
        count = self.station_data_count[station]
        time_to_add = timedelta(seconds=count/128)

        return time_to_add + first_time


    def get_data_to_predict(self, station: str) -> List[List[int]]:
        if not self.is_ready_to_predict(station):
            return []
        
        data = []

        for channel in self.data_points[station]:
            data_points = self.data_points[station][channel]
            data.append(data_points[:128])
            self.data_points[station][channel] = data_points[128:]

        self.inc_station_data_count(station, 128)

        return data 
    

