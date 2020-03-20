# standard libraries
import json, logging, os

# third-party libraries
from sqlalchemy import create_engine

# local libraries
from db.db_creator import DBCreator


# Initializing settings and global variables

logger = logging.getLogger(__name__)

try:
    config_path = os.getenv("ENV_PATH", os.path.join('config', 'secrets', 'env.json'))
    with open(config_path) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

DB_PARAMS = ENV['INVENTORY_DB']


# Main Program

if __name__ == '__main__':
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    db_creator_obj = DBCreator(DB_PARAMS)
    db_creator_obj.set_up_database()
