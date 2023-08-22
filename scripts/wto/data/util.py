import csv
import json
import os

import requests
from absl import logging
from google.cloud import storage


def fetch_and_write_json(url,
                         params,
                         headers,
                         response_file: str,
                         remote_call_lock_func=None):
    response = _fetch(url, params, headers, response_file, None,
                      remote_call_lock_func)
    if response:
        with open(response_file, 'w') as f:
            logging.info('Writing response to file %s', response_file)
            json.dump(response.json(), f, indent=2)


def fetch_and_write(url,
                    params,
                    headers,
                    response_file: str,
                    remote_call_lock_func=None):
    error_response_file = f"{response_file}.ERROR.json"
    response = _fetch(url, params, headers, response_file, error_response_file,
                      remote_call_lock_func)

    if response:
        if _is_error_response(response, response_file):
            with open(error_response_file, 'wb') as f:
                logging.info('Writing error response to file %s',
                             error_response_file)
                f.write(response.content)
        else:
            with open(response_file, 'wb') as f:
                logging.info('Writing response to file %s', response_file)
                f.write(response.content)


def _is_error_response(response: requests.Response, response_file_name) -> bool:
    return response.status_code != 200 or _content_type_file_ext_mismatch(
        response.headers.get('content-type'), response_file_name)


def _content_type_file_ext_mismatch(content_type, file_name):
    if content_type and \
        'application/json' in content_type.lower() and \
        not file_name.lower().endswith('.json'):
        return True
    return False


def _fetch(url,
           params,
           headers,
           response_file: str,
           error_response_file: str,
           remote_call_lock_func=None):
    if os.path.exists(response_file):
        logging.info('SKIPPING http call, response file already exists %s',
                     response_file)
        return None

    if error_response_file and os.path.exists(error_response_file):
        logging.info(
            'SKIPPING http call, error response file already exists %s',
            response_file)
        return None

    if remote_call_lock_func:
        remote_call_lock_func()

    logging.info("Fetching url %s, params %s", url, params)
    return requests.get(url, params=params, headers=headers)


def read_json(file_path):
    if os.path.exists(file_path):
        logging.info('Reading json from file %s', file_path)
        with open(file_path, 'r') as f:
            return json.load(f)


def write_csv(csv_file_path, csv_columns, csv_rows):
    if (len(csv_rows) == 0):
        logging.warning('No rows found. SKIPPED writing to CSV file: %s',
                        csv_file_path)
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


def read_csv(csv_file: str):
    logging.info("Reading csv file: %s", csv_file)
    with open(csv_file, 'r') as f:
        rows = list(csv.DictReader(f))
        logging.info("Read %s rows from csv file: %s", len(rows), csv_file)
        return rows


def fetch_json_from_gcloud(project_id: str, bucket_name: str, blob_name: str):
    logging.info('Getting cloud blob: %s/%s/%s', project_id, bucket_name,
                 blob_name)
    storage_client = storage.Client(project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return json.loads(blob.download_as_string(client=None))


def get_numeric(value):
    if value is None:
        return None

    if value.isdecimal():
        return int(value)

    try:
        return float(value)
    except:
        return None


def get_int(value):
    if value is None:
        return None

    if value.isdecimal():
        return int(value)

    return None
