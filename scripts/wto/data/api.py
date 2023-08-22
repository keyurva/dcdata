import os
import util
import multiprocessing
import time


def _init_qps_lock(lock):
    '''Initialize each process with a qps lock.
  The WTO API has a rate limit of 1 QPS.
  If making parallel calls to the API, initialize the pool with a lock.
  This lock will be used by the ApiClient to stay within the rate limits.

  More details: https://stackoverflow.com/a/69913167
  '''
    global qps_lock
    qps_lock = lock


def _acquire_qps_lock():
    if 'qps_lock' in globals():
        with qps_lock:
            time.sleep(1.001)


class ApiClient:
    _INDICATORS_API_URL = 'https://api.wto.org/timeseries/v1/indicators'
    _INDICATOR_DATA_API_URL = 'https://api.wto.org/timeseries/v1/data'

    def __init__(self, api_key: str, output_dir: str = 'output'):
        self.request_headers = {'Ocp-Apim-Subscription-Key': api_key}
        self.output_dir = output_dir
        responses_dir = os.path.join(output_dir, 'responses')
        indicator_responses_dir = os.path.join(responses_dir, 'indicators')
        self.indicator_data_responses_dir = os.path.join(responses_dir, 'data')
        self.indicators_response_file = os.path.join(indicator_responses_dir,
                                                     'indicators.json')

        os.makedirs(indicator_responses_dir, exist_ok=True)
        os.makedirs(self.indicator_data_responses_dir, exist_ok=True)

    def fetch_indicator_data(self, indicator_code: str):
        params = f"i={indicator_code}&r=all&p=default&ps=all&pc=default&spc=false&fmt=csv&mode=full&dec=default&off=0&max=1000000&head=H&lang=1&meta=false"
        response_file = os.path.join(self.indicator_data_responses_dir,
                                     f"{indicator_code}.zip")
        util.fetch_and_write(url=self._INDICATOR_DATA_API_URL,
                             params=params,
                             headers=self.request_headers,
                             response_file=response_file,
                             remote_call_lock_func=_acquire_qps_lock)

    def fetch_indicators(self):
        params = 'i=all&t=all&pc=all&tp=all&frq=all&lang=1'
        util.fetch_and_write_json(url=self._INDICATORS_API_URL,
                                  params=params,
                                  headers=self.request_headers,
                                  response_file=self.indicators_response_file)
        return util.read_json(self.indicators_response_file)
