# standard libraries
import json, logging, os, sys
from json.decoder import JSONDecodeError
from typing import Dict, Any

# third-party libraries
import hjson
from jsonschema import validate


logger = logging.getLogger(__name__)

# Set up ENV and ENV_SCHEMA
ROOT_DIR: str = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR: str = os.path.join(ROOT_DIR, os.getenv('ENV_DIR', os.path.join('config', 'secrets')))
CONFIG_PATH: str = os.path.join(CONFIG_DIR, os.getenv('ENV_FILE', 'env.hjson'))

try:
    with open(CONFIG_PATH) as env_file:
        ENV: Dict[str, Any] = hjson.loads(env_file.read())
except FileNotFoundError:
    logger.error(f'Configuration file could not be found; please add file "{CONFIG_PATH}".')
    ENV = dict()

with open(os.path.join(ROOT_DIR, 'config', 'env_schema.hjson')) as schema_file:
    ENV_SCHEMA: Dict[str, Any] = hjson.loads(schema_file.read())

LOG_LEVEL: str = ENV.get('LOG_LEVEL', 'INFO')
logging.basicConfig(level=LOG_LEVEL)

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
        logger.info('ENV value overridden')
        logger.info(f'key: {key}; os_value: {os_value}')

logger.debug(ENV)

# Validate ENV using ENV_SCHEMA
try:
    validate(instance=ENV, schema=ENV_SCHEMA)
    logger.info('ENV is valid; the program will continue')
except Exception as e:
    logger.error(e)
    logger.error('ENV is invalid; the program will exit')
    sys.exit(1)
