import csv
import json
import os

import requests
from absl import logging


def load_json(url, params, headers, response_file: str):
  if os.path.exists(response_file):
    logging.info('Reading response from file %s', response_file)
    with open(response_file, 'r') as f:
      return json.load(f)

  logging.info("Fetching url %s, params %s", url, params)
  response = requests.get(url, params=params, headers=headers).json()
  with open(response_file, 'w') as f:
    logging.info('Writing response to file %s', response_file)
    json.dump(response, f, indent=2)
  return response


def write_csv(csv_file_path, csv_columns, csv_rows):
  if (len(csv_rows) == 0):
    logging.warning('No rows found. SKIPPED writing to CSV file: %s', csv_file_path)
    return

  if csv_columns is None or len(csv_columns) == 0:
    csv_columns = list(csv_rows[0].keys())

  logging.info('Writing CSV file: %s', csv_file_path)
  with open(csv_file_path, 'w', newline='') as out:
    csv_writer = csv.DictWriter(out,
                                fieldnames=csv_columns,
                                lineterminator='\n')
    csv_writer.writeheader()
    csv_writer.writerows(csv_rows)
