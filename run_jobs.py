# standard libraries
import logging, os, sys, time
from importlib import import_module
from typing import Sequence, Union

# third-party libraries
import pandas as pd
import sqlalchemy

# local libraries
from db.db_creator import DBCreator
from environ import ENV
from vocab import DataSourceStatus, JobError, ValidJobName

# Initialize settings and global variables
logger = logging.getLogger(__name__)


# Class(es)

class Job:

    def __init__(self, job_name: ValidJobName) -> None:
        self.name: str = job_name.name
        self.import_path: str = '.'.join(job_name.value.split('.')[:-1])
        self.method_name: str = job_name.value.split('.')[-1]
        self.started_at: Union[float, None] = None
        self.finished_at: Union[float, None] = None
        self.data_sources: Sequence[DataSourceStatus] = []

    def create_metadata(self) -> None:
        started_at_dt = pd.to_datetime(self.started_at, unit='s')
        finished_at_dt = pd.to_datetime(self.finished_at, unit='s')

        job_run_df = pd.DataFrame({
            'job_name': [self.name],
            'started_at': [started_at_dt],
            'finished_at': [finished_at_dt]
        })
        job_run_df.to_sql('job_run', db_creator_obj.engine, if_exists='append', index=False)
        logger.info(
            f'Inserted job_run record for job_name "{self.name}" '
            f'with finished_at value of "{finished_at_dt}"')
        job_run_id = pd.read_sql('job_run', db_creator_obj.engine).iloc[-1]['id']

        if len(self.data_sources) == 0:
            logger.warning('No valid data sources were identified')
        else:
            data_source_status_df = (
                pd.DataFrame.from_records(
                    data_source.copy() for data_source in self.data_sources)
                    .assign(**{'job_run_id': job_run_id}))

            data_source_status_df.to_sql(
                'data_source_status', db_creator_obj.engine, if_exists='append', index=False)
            logger.info(f'Inserted ({len(data_source_status_df)}) data_source_status records')

    def run(self) -> None:
        leaf_module = import_module(self.import_path)
        start_method = getattr(leaf_module, self.method_name)

        # Until we have a decorator for this
        self.started_at = time.time()
        try:
            self.data_sources = start_method()
            self.finished_at = time.time()

            delta = self.finished_at - self.started_at
            str_time = time.strftime('%H:%M:%S', time.gmtime(delta))
            logger.info(f'Duration of job run: {str_time}')

            self.create_metadata()
        except JobError as je:
            logger.error(f'JobError: {je}')
            logger.info(f'An error occurred that prevented the {self.name} job from finishing')
            logger.info('The program will continue running other jobs')


class JobManager:

    def __init__(self, job_names: Sequence[str]) -> None:
        self.jobs: Sequence[Job] = []
        for job_name in job_names:
            if job_name.upper() in ValidJobName.__members__:
                job_name_mem = ValidJobName[job_name.upper()]
                self.jobs.append(Job(job_name_mem))
            else:
                logger.error(f'Received an invalid job name: {job_name}; it will be ignored')

    def run_jobs(self) -> None:
        for job in self.jobs:
            logger.info(f'- - Running job {job.name} - -')
            job.run()


if __name__ == '__main__':
    db_creator_obj = DBCreator(ENV['INVENTORY_DB'])
    how_started = os.environ.get('HOW_STARTED', None)

    if how_started == 'DOCKER_COMPOSE':
        # Wait for MySQL container to finish setting up
        # If it's not ready in two minutes, exit
        num_loops = 40
        for i in range(num_loops + 1):
            try:
                conn = db_creator_obj.engine.connect()
                conn.close()
                logger.info('MySQL caught up')
                break
            except sqlalchemy.exc.OperationalError:
                if i == num_loops:
                    logger.error('MySQL was not available')
                    sys.exit(1)
                else:
                    if i == 0:
                        logger.info('Waiting for the MySQL snail')
                    else:
                        logger.debug('Still waiting!')
                    time.sleep(3.0)

    # Apply any new migrations
    logger.info('Applying any new migrations')
    db_creator_obj.migrate()

    # Run those jobs
    manager = JobManager(ENV['JOB_NAMES'])
    manager.run_jobs()
