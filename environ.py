# standard libraries
import json, logging, os

# entry-level job methods need to be one-level beneath root
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger(__name__)


config_path = os.path.join(ROOT_DIR, os.getenv('ENV_PATH', os.path.join('config', 'secrets', 'env.json')))

try:
    with open(config_path) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')
    ENV = None

logging.basicConfig(level=ENV['LOG_LEVEL'])

logger.debug(os.environ)

# Add ENV key-value pairs to environment, skipping if the key is already set
for key, value in ENV.items():
    if key in os.environ:
        ENV[key] = value
        logger.info('ENV value overidden')
        logger.info(f'key: {key}; value: {value}')
