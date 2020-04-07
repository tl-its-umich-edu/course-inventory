# standard libraries
import logging

# local libraries
from db.db_creator import DBCreator
from environ import ENV

# Initializing settings and global variables

logger = logging.getLogger(__name__)

DB_PARAMS = ENV['INVENTORY_DB']
APPEND_TABLE_NAMES = ENV.get('APPEND_TABLE_NAMES', ['job_run'])


# Main Program

if __name__ == '__main__':
    logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))
    db_creator_obj = DBCreator(DB_PARAMS, APPEND_TABLE_NAMES)
    db_creator_obj.set_up_database()
