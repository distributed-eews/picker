from datetime import datetime
from typing import Dict


class MissingDataHandler:
    def __init__(self):
        self.data_pool: Dict[str, Dict[str, int]] = {}
        self.last_processed_time: Dict[str, Dict[str, datetime]] = {}

    def handle_missing_data(self, station: str, channel: str, start_time: datetime, sampling_rate: float):
        if station in self.last_processed_time and channel in self.last_processed_time[station]:
            time_diff = start_time - self.last_processed_time[station][channel]
            if station == "BKB" and channel == "BHE":
                print(start_time)
                print(self.last_processed_time[station][channel])
                print(time_diff)
                print("-"*20)
            missing_samples = int(time_diff.total_seconds() * sampling_rate)
            if missing_samples > 0:
                missing_data = [0] * missing_samples
                self.data_pool[station][channel].extend(missing_data)

    def update_last_processed_time(self, station: str, channel: str, end_time: datetime):
        if station not in self.last_processed_time:
            self.last_processed_time[station] = {}
        self.last_processed_time[station][channel] = end_time
