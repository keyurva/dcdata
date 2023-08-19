from absl import app
from absl import logging
from absl import flags
import api
import indicators

FLAGS = flags.FLAGS


class Mode:
  WRITE_INDICATORS = 'write_indicators'


flags.DEFINE_string(
    'mode', Mode.WRITE_INDICATORS,
    f"Specify one of the following modes: {Mode.WRITE_INDICATORS}"
)

flags.DEFINE_string('wto_api_key', 'ada8e0b9a1e846a2a84a0b345286a636',
                    'WTO API Key.')

OUTPUT_DIR = 'output'

def main(_):
  api_client = api.ApiClient(FLAGS.wto_api_key, OUTPUT_DIR)
  wto_indicators = indicators.Indicators(api_client)
  match FLAGS.mode:
    case Mode.WRITE_INDICATORS:
      wto_indicators.write_indicators()
    case _:
      logging.error('No mode specified.')


if __name__ == '__main__':
  app.run(main)