# standard libraries
import logging, json, os
from datetime import datetime
from typing import Dict, Sequence

# third-party libraries
import pandas as pd
from sqlalchemy import create_engine

# local libraries
from db.tables import tables


# Initializing settings and global variables

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

DB_PARAMS = ENV['INVENTORY_MYSQL']

conn_str = (
    'mysql' +
    f"://{DB_PARAMS['user']}" +
    f":{DB_PARAMS['password']}" +
    f"@{DB_PARAMS['host']}" +
    f":{DB_PARAMS['port']}" +
    f"/{DB_PARAMS['dbname']}?charset=utf8"
)

logger.info(conn_str)

MYSQL_ENGINE = create_engine(conn_str)


# Class(es)

class DBCreator:

    def __init__(self, db_name: str, table_dicts: Sequence[Dict[str, str]]):
        self.conn = None
        self.db_name = db_name
        self.tables = table_dicts

    def set_up(self):
        logger.debug('set_up')
        self.conn = MYSQL_ENGINE.connect()

    def tear_down(self):
        logger.debug('tear_down')
        self.conn.close()

    def drop_tables(self) -> None:
        logger.debug('drop_tables')
        self.conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        table_names = [table['name'] for table in tables]
        drop_statement = f'DROP TABLE IF EXISTS {", ".join(table_names)};'
        self.conn.execute(drop_statement)
        self.conn.execute('SET FOREIGN_KEY_CHECKS=1;')

    def create_tables(self) -> None:
        logger.debug('create_tables')
        for table_dict in self.tables:
            table_name = table_dict["name"]
            logger.debug(f'Table Name: {table_name}')
            self.conn.execute(table_dict['statement'])
            logger.info(f'Created table {table_name} in {self.db_name}')

    def set_up_database(self) -> None:
        self.set_up()
        self.drop_tables()
        self.create_tables()
        self.tear_down()


# Main Program

if __name__ == '__main__':
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    db_creator_obj = DBCreator('course_inventory', tables)
    db_creator_obj.set_up_database()
