from absl import app
from absl import logging
from absl import flags
import api
import indicators
import util

FLAGS = flags.FLAGS


class Mode:
    WRITE_INDICATORS = 'write_indicators'
    FETCH_DATA = 'fetch_data'
    WRITE_OBSERVATIONS = 'write_observations'


class FlagKeys:
    wto_api_key = 'wto_api_key'


flags.DEFINE_string(
    'mode', Mode.WRITE_INDICATORS,
    f"Specify one of the following modes: {Mode.WRITE_INDICATORS}, {Mode.FETCH_DATA}, {Mode.WRITE_OBSERVATIONS}"
)

flags.DEFINE_string(FlagKeys.wto_api_key, None, 'WTO API Key.')

OUTPUT_DIR = 'output'


def load_wto_api_key():
    if FLAGS.wto_api_key is None:
        cloud_config = util.fetch_json_from_gcloud(
            project_id='datcom-204919',
            bucket_name='datcom-csv',
            blob_name='wto/data/wto-config.json')
        wto_api_key = cloud_config[FlagKeys.wto_api_key]
        FLAGS.set_default(FlagKeys.wto_api_key, wto_api_key)

    logging.info('Using WTO API Key: %s', FLAGS.wto_api_key)


def main(_):
    load_wto_api_key()
    api_client = api.ApiClient(FLAGS.wto_api_key, OUTPUT_DIR)
    wto_indicators = indicators.Indicators(api_client)
    match FLAGS.mode:
        case Mode.WRITE_INDICATORS:
            wto_indicators.write_indicators()
        case Mode.FETCH_DATA:
            wto_indicators.fetch_all_indicator_data()
        case Mode.WRITE_OBSERVATIONS:
            # wto_indicators.write_observations('output/responses/data/HS_M_0010.zip')
            wto_indicators.write_all_observations()
        case _:
            logging.error('No mode specified.')


if __name__ == '__main__':
    app.run(main)
