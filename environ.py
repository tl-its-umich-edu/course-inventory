# standard libraries
import json, logging, os, sys
from json.decoder import JSONDecodeError
from typing import Any, Dict

# third-party libraries
import hjson
from jsonschema import draft7_format_checker, validate


logger = logging.getLogger(__name__)

# Set up path variables
ROOT_DIR: str = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR: str = os.path.join(ROOT_DIR, os.getenv('ENV_DIR', os.path.join('config', 'secrets')))
DATA_DIR: str = os.path.join(ROOT_DIR, os.path.join("data"))
CONFIG_PATH: str = os.path.join(CONFIG_DIR, os.getenv('ENV_FILE', 'env.hjson'))

# Set up ENV and ENV_SCHEMA
try:
    with open(CONFIG_PATH) as env_file:
        ENV: Dict[str, Any] = hjson.loads(env_file.read())
except FileNotFoundError:
    logger.error(f'Configuration file could not be found; please add file "{CONFIG_PATH}".')
    ENV = dict()

with open(os.path.join(ROOT_DIR, 'config', 'env_schema.hjson')) as schema_file:
    ENV_SCHEMA: Dict[str, Any] = hjson.loads(schema_file.read())

LOG_LEVEL: str = ENV.get('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(funcName)s - %(levelname)s  - %(message)s'
)

# Override ENV key-value pairs with values from os.environ if set
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
    validate(instance=ENV, schema=ENV_SCHEMA, format_checker=draft7_format_checker)
    logger.info('ENV is valid; the program will continue')
except Exception as e:
    logger.error(e)
    logger.error('ENV is invalid; the program will exit')
    sys.exit(1)
