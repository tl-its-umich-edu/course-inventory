# standard libraries
import logging, os
from typing import Dict, Sequence

# third-party libraries
from sqlalchemy.engine import create_engine
from yoyo import get_backend, read_migrations

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_PATH = os.path.join(ROOT_DIR, 'migrations')


class DBCreator:

    def __init__(
        self,
        db_params: Dict[str, str],
        append_table_names: Sequence[str] = []
    ) -> None:

        self.db_name = db_params['dbname']
        self.conn = None
        self.conn_str = (
            'mysql+mysqldb' +
            f"://{db_params['user']}" +
            f":{db_params['password']}" +
            f"@{db_params['host']}" +
            f":{db_params['port']}" +
            f"/{db_params['dbname']}?charset=utf8"
        )
        self.engine = create_engine(self.conn_str)
        self.append_table_names = append_table_names

    def set_up(self) -> None:
        logger.debug('set_up')
        self.conn = self.engine.connect()

    def tear_down(self) -> None:
        logger.debug('tear_down')
        self.conn.close()

    def get_table_names(self) -> Sequence[str]:
        logger.debug('get_table_names')
        return self.engine.table_names()

    def migrate(self) -> None:
        logger.debug('migrate')
        backend = get_backend(self.conn_str)
        migrations = read_migrations(MIGRATIONS_PATH)
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))

    def drop_records(self) -> None:
        logger.debug('drop_records')
        self.conn.execute('SET FOREIGN_KEY_CHECKS=0;')
        for table_name in self.get_table_names():
            if 'yoyo' not in table_name and table_name not in self.append_table_names:
                logger.debug(f'Table Name: {table_name}')
                self.conn.execute(f'DELETE FROM {table_name};')
                logger.info(f'Dropped records in {table_name} in {self.db_name}')
        self.conn.execute('SET FOREIGN_KEY_CHECKS=1;')

    def set_up_database(self) -> None:
        self.set_up()
        self.drop_records()
        self.migrate()
        self.tear_down()
