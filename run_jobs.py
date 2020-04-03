# standard libraries
import json, logging, os, time
from importlib import import_module
from enum import Enum
from typing import Dict, Sequence, Union

# local libraries
import course_inventory
from db.db_creator import DBCreator
from environ import ENV
import online_meetings


# Initialize settings and global variables
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=ENV.get('LOG_LEVEL', 'DEBUG'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# Enum(s)

# Each job should be defined in AvailableJob.
# NAME_OF_JOB = 'path.to.method'

class AvailableJob(Enum):
    COURSE_INVENTORY = 'course_inventory.inventory.run_course_inventory'
    # ONLINE_MEETINGS = 'online_meetings.report...'
    # ZOOM = 'online_meetings.canvas_zoom_meetings...'


# Class(es)

class Job:

    def __init__(self, job: AvailableJob) -> None:
        self.name = job.name
        self.import_path = '.'.join(job.value.split('.')[:-1])
        self.method_name = job.value.split('.')[-1]

    def run(self):
        leaf_module = import_module(self.import_path)
        start_method = getattr(leaf_module, self.method_name)

        # Until we have a decorator for this
        start_time = time.time()
        start_method()
        delta = time.time() - start_time
        str_time = time.strftime("%H:%M:%S", time.gmtime(delta))
        logger.info(f'Duration of job run: {str_time}')


class JobManager:

    def __init__(self, job_names: Sequence[str]) -> None:
        self.jobs = []
        for job_name in job_names:
            try:
                avail_job = AvailableJob[job_name.upper()]
                self.jobs.append(Job(avail_job))
            except KeyError:
                logger.error(f'{job_name} is not a valid job name')

    def run_jobs(self):
        for job in self.jobs:
            logger.info(f'- - Running job {job.name} - -')
            job.run()


if __name__ == '__main__':
    how_started = os.environ.get('HOW_STARTED', None)

    if how_started == 'DOCKER_COMPOSE':
        # Wait for MySQL container to finish setting up
        time.sleep(5.0)

    # Apply any new migrations
    db_creator_obj = DBCreator(ENV['INVENTORY_DB'])
    db_creator_obj.migrate()

    # Run those jobs
    manager = JobManager(ENV['JOB_NAMES'])
    manager.run_jobs()
