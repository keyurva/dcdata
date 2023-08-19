import os
import multiprocessing
import util


class ApiClient:
  _POOL_SIZE = max(2, multiprocessing.cpu_count() - 1)
  _INDICATORS_API_URL = 'https://api.wto.org/timeseries/v1/indicators'

  def __init__(self, api_key: str, output_dir: str = 'output'):
    self.request_headers = {'Ocp-Apim-Subscription-Key': api_key}
    self.output_dir = output_dir
    responses_dir = os.path.join(output_dir, 'responses')
    indicator_responses_dir = os.path.join(responses_dir, 'indicators')
    self.indicators_response_file = os.path.join(
        indicator_responses_dir, 'indicators.json')

    os.makedirs(indicator_responses_dir, exist_ok=True)

  def fetch_indicators(self):
    params = 'i=all&t=all&pc=all&tp=all&frq=all&lang=1'
    return util.load_json(url=self._INDICATORS_API_URL, params=params,
                          headers=self.request_headers,
                          response_file=self.indicators_response_file)
