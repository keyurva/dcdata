import os

import api
import util


class Indicators:
  def __init__(self, api_client: api.ApiClient):
    self.api_client = api_client
    self.indicators_csv_file = os.path.join(api_client.output_dir,
                                            'indicators.csv')

  def write_indicators(self):
    rows = self.api_client.fetch_indicators()
    util.write_csv(csv_file_path=self.indicators_csv_file, csv_columns=None,
                   csv_rows=rows)
