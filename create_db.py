# standard libraries
import logging, json, os

# third-party libraries
from sqlalchemy import create_engine

# local libraries
from db.db_creator import DBCreator
from db.tables import tables as TABLES


# Initializing settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

DB_PARAMS = ENV['INVENTORY_DB']

conn_str = (
    'mysql' +
    f"://{DB_PARAMS['user']}" +
    f":{DB_PARAMS['password']}" +
    f"@{DB_PARAMS['host']}" +
    f":{DB_PARAMS['port']}" +
    f"/{DB_PARAMS['dbname']}?charset=utf8"
)
MYSQL_ENGINE = create_engine(conn_str)


# Main Program

if __name__ == '__main__':
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    db_creator_obj = DBCreator(DB_PARAMS['dbname'], MYSQL_ENGINE, TABLES)
    db_creator_obj.set_up_database()
