# standard libraries
import json, logging, os
from json.decoder import JSONDecodeError

# entry-level job modules need to be one-level beneath root
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger(__name__)


# Set up ENV

config_path = os.path.join(ROOT_DIR, os.getenv('ENV_PATH', os.path.join('config', 'secrets', 'env.json')))

try:
    with open(config_path) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')
    ENV = None

logging.basicConfig(level=ENV['LOG_LEVEL'])

# Add ENV key-value pairs to environment, skipping if the key is already set
logger.debug(os.environ)
for key, value in ENV.items():
    if key in os.environ:
        os_value = os.environ[key]
        try:
            os_value = json.loads(os_value)
            logger.info('Found valid JSON and parsed it')
        except JSONDecodeError:
            logger.debug('Valid JSON was not found')
        ENV[key] = os_value
        logger.info('ENV value overidden')
        logger.info(f'key: {key}; os_value: {os_value}')
