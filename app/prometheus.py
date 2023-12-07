from prometheus_client import start_http_server, Counter, Summary

TOTAL_PREDICTED_DATA = Counter(
    "picker_total_predicted_data", "Number of data predicted"
)
TOTAL_PREDICTION_REQUEST_SUCCESS = Counter(
    "picker_total_prediction_request_success", "Number of requests successful"
)
TOTAL_PREDICTION_REQUEST_ERROR = Counter(
    "picker_total_prediction_request_error", "Number of requests error"
)
TOTAL_PREDICTION_TIME = Summary(
    "picker_total_prediction_time", "Total prediction time for requests"
)
TOTAL_RECEIVED_DATA = Counter("picker_total_received_data", "Number of data received")
TOTAL_RECEIVED_TIME = Summary(
    "picker_total_received_time", "Total received time for requests"
)


class Prometheus:
    def __init__(self, port=8012, addr="0.0.0.0"):
        self.addr = addr
        self.port = port

    def start(self):
        start_http_server(self.port, self.addr)

    def inc_pred_data(self, inc=1):
        TOTAL_PREDICTED_DATA.inc(inc)

    def inc_pred_req_suc(self, inc=1):
        TOTAL_PREDICTION_REQUEST_SUCCESS.inc(inc)

    def inc_pred_req_err(self, inc=1):
        TOTAL_PREDICTION_REQUEST_ERROR.inc(inc)

    def inc_rec_data(self, inc=1):
        TOTAL_RECEIVED_DATA.inc(inc)

    def obs_pred_time(self, seconds: float):
        TOTAL_PREDICTION_TIME.observe(seconds)

    def obs_rec_time(self, seconds: float):
        TOTAL_RECEIVED_TIME.observe(seconds)
