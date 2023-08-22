import csv
import multiprocessing
import os
import zipfile
import codecs

from absl import logging

import api
import util


class StatVarKey:
    code = 'code'
    multiplier = 'multiplier'
    statVar = 'statVar'
    unit = 'svObsUnit'


class IndicatorDataKey:
    indicator_code = 'Indicator Code'
    reporting_economy_code = 'Reporting Economy Code'
    partner_economy_code = 'Partner Economy Code'
    product_or_sector_code = 'Product/Sector Code'
    year = 'Year'
    value = 'Value'


class ObservationKey:
    wto_code = 'wto_code'
    stat_var = 'statVar'
    reporting_country_code = 'reportingCountryCode'
    partner_country_code = 'partnerCountryCode'
    product_or_sector_code = 'productOrSectorCode'
    year = 'year'
    value = 'value'
    unit = 'unit'
    csv_columns = [
        wto_code, stat_var, reporting_country_code, partner_country_code,
        product_or_sector_code, year, value, unit
    ]


def _load_stat_vars():
    stat_vars_list = util.read_csv('statvars.csv')
    stat_vars = {}
    for stat_var in stat_vars_list:
        stat_var[StatVarKey.multiplier] = util.get_numeric(
            stat_var[StatVarKey.multiplier])
        stat_vars[stat_var[StatVarKey.code]] = stat_var
    return stat_vars


class Indicators:

    def __init__(self, api_client: api.ApiClient):
        self.api_client = api_client
        self.observations_output_dir = os.path.join(api_client.output_dir,
                                                    'observations')
        os.makedirs(self.observations_output_dir, exist_ok=True)
        self.indicators_csv_file = os.path.join(api_client.output_dir,
                                                'indicators.csv')
        self.stat_vars = _load_stat_vars()

    def fetch_all_indicator_data(self):
        indicator_codes = self.fetch_all_indicator_codes()
        qps_lock = multiprocessing.Lock()
        with multiprocessing.Pool(processes=None,
                                  initializer=api._init_qps_lock,
                                  initargs=(qps_lock,)) as pool:
            pool.starmap(self.api_client.fetch_indicator_data,
                         zip(indicator_codes))

    def fetch_all_indicator_codes(self):
        codes = []
        indicators = self.api_client.fetch_indicators()
        for indicator in indicators:
            codes.append(indicator['code'])
        return codes

    def write_indicators(self):
        rows = self.api_client.fetch_indicators()
        util.write_csv(csv_file_path=self.indicators_csv_file,
                       csv_columns=None,
                       csv_rows=rows)

    def write_all_observations(self):
        data_zip_files = []
        for file_name in os.listdir(
                self.api_client.indicator_data_responses_dir):
            if file_name.endswith('.zip'):
                data_zip_files.append(
                    os.path.join(self.api_client.indicator_data_responses_dir,
                                 file_name))

        with multiprocessing.Pool(processes=None) as pool:
            pool.starmap(self.write_observations, zip(data_zip_files))

    def write_observations(self, data_zip_file):
        obs_file_name = f"{os.path.basename(data_zip_file).split('.')[0]}.csv"
        obs_file_path = os.path.join(self.observations_output_dir,
                                     obs_file_name)
        util.write_csv(
            csv_file_path=obs_file_path,
            csv_columns=ObservationKey.csv_columns,
            csv_rows=self.get_observation_csv_rows_from_zip(data_zip_file))

    def get_observation_csv_rows_from_zip(self, zip_file):
        data_rows = self._get_data_from_zip(zip_file)
        if len(data_rows) == 0:
            return []

        wto_code = data_rows[0].get(IndicatorDataKey.indicator_code)
        stat_var = self.stat_vars.get(wto_code)
        if stat_var is None:
            logging.warning('SKIPPED zip file %s, no statVar mapped: %s',
                            zip_file, wto_code)
            return []

        csv_rows = []
        for data_row in data_rows:
            csv_row = self._to_obs_csv_row(data_row, stat_var, zip_file)
            if csv_row:
                csv_rows.append(csv_row)

        return csv_rows

    def _to_obs_csv_row(self, data_row, stat_var, zip_file):
        value = util.get_numeric(data_row.get(IndicatorDataKey.value))
        if value is None:
            return None

        value = value * stat_var[StatVarKey.multiplier]

        reporting_country_code = util.get_int(
            data_row[IndicatorDataKey.reporting_economy_code])
        if reporting_country_code is None:
            logging.warning(
                'SKIPPED data row in file %s, unsupported reporting country code %s',
                zip_file, data_row[IndicatorDataKey.reporting_economy_code])
            return None

        return {
            ObservationKey.wto_code:
                data_row[IndicatorDataKey.indicator_code],
            ObservationKey.stat_var:
                stat_var[StatVarKey.statVar],
            ObservationKey.reporting_country_code:
                reporting_country_code,
            ObservationKey.partner_country_code:
                data_row[IndicatorDataKey.partner_economy_code],
            ObservationKey.product_or_sector_code:
                data_row[IndicatorDataKey.product_or_sector_code],
            ObservationKey.year:
                data_row[IndicatorDataKey.year],
            ObservationKey.value:
                value,
            ObservationKey.unit:
                stat_var.get(StatVarKey.unit)
        }

    def _get_data_from_zip(self, zip_file):
        with zipfile.ZipFile(zip_file, 'r') as zip:
            files = zip.namelist()
            if len(files) != 1:
                logging.warning('SKIPPED zip file %s, expected 1 file, got %s',
                                zip_file, len(files))
                return []

            logging.info("Reading csv file: %s", files[0])
            with zip.open(files[0], 'r') as bytes:
                rows = list(
                    csv.DictReader(
                        codecs.iterdecode(bytes, 'utf-8', errors='ignore')))
                logging.info("Read %s rows from csv in zip file: %s", len(rows),
                             files[0])
                return rows
