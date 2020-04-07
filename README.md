# course-inventory


## Overview

The course-inventory application is designed to gather current-term Canvas LMS data about courses, enrollments, and users in order to inform leadership at the University of Michigan about the status and utilization of Canvas courses. Currently, the application collects data from the Canvas API and the Unizin Data Warehouse and then stores the data in an external MySQL database. Tableau dashboards and other processes then consume that data to generate reports and visualizations.


## Development


### Pre-requisities

The sections below provide instructions for configuring, installing, using, and changing the application. Depending on the environment you plan to run the application in, you may also need to install some or all of the following:
   * [Python 3.7](https://docs.python.org/3/)
   * [MySQL](https://dev.mysql.com/doc/)
   * [Docker Desktop](https://www.docker.com/products/docker-desktop)
   * [OpenShift CLI](https://docs.openshift.com/enterprise/3.1/cli_reference/get_started_cli.html)

While performing any of the actions described below, use a terminal, text editor, or file utility as necessary. Some sample command-line instructions are provided for some steps.


### Configuration

To configure the application before installation and usage (see the next section), you must first perform a few steps, including the creation of a configuration file called `env.json`. Complete the following items in order.

1. Clone and navigate into the repository.
    ```
    git clone https://github.com/tl-its-umich-edu/course-inventory.git  # HTTPS
    git clone git@github.com:tl-its-umich-edu/course-inventory.git      # SSH
    
    cd course-inventory
    ```

2. Set up a MySQL database. 
    
    If you plan to run the application using `virtualenv`, you will need to have MySQL installed on your machine and create a test database and user. 
    
    If you use Docker, instead you will use the database credentials specified in the `docker-compose.yml` in the `environment` block (ignoring `MYSQL_ROOT_PASSWORD`) for the `mysql` service. 
    
    Whether you use `virtualenv` or Docker, you should provide the database credentials within the `INVENTORY_DB` object, described in step 4.

3. Copy the template configuration file, `env_blank.json` from the `config` directory, re-name it `env.json`, and place it inside the `secrets` subdirectory.
    ```
    mv config/env_blank.json config/secrets/env.json
    ```

4. Change the default values inside `env.json` (empty strings, `0`s, and provided values) with the desired values, ensuring they are the same data type. The table below describes the meaning and expected values of each key-value pair.

    **Key** | **Description**
    ----- | -----
    `LOG_LEVEL` | The minimum level for log messages that will appear in output. `INFO` or `DEBUG` is recommended for most use cases; see [Python's logging module](https://docs.python.org/3/library/logging.html).
    `CANVAS_ACCOUNT_ID` | The Canvas instance root account ID number associated with the courses for which data will be collected.
    `CANVAS_TERM_ID` | The Canvas instance term ID number that will be used to limit the query for Canvas courses. Set to 0 to just use ADD_COURSE_IDS.
    `ADD_COURSE_IDS` | Additional Canvas course ID's to retrieve. Duplicates found in CANVAS_TERM_ID (if defined) will be removed.
    `API_BASE_URL` | The base URL for making requests using the U-M API Directory; the default value should be correct.
    `API_SCOPE_PREFIX` | The scope prefix that will be added after the `API_BASE_URL`; this is usually an acronym for the university location and the API Directory subscription name in CamelCase, separated by a `/`.
    `API_SUBSCRIPTION_NAME` | The name of the API Directory subscription all in lowercase.
    `API_CLIENT_ID` | The client ID for authenticating to the API Directory.
    `API_CLIENT_SECRET` | The client secret for authenticating to the API Directory.
    `MAX_REQ_ATTEMPTS` | The number of times a specific request will be attempted.
    `CANVAS_URL` | The Canvas instance URL to be used as the base URL for API requests that use the `CANVAS TOKEN`.
    `CANVAS_TOKEN` | The Canvas token used for authenticating to the API when not using the U-M API Directory.
    `NUM_ASYNC_WORKERS` | Number of workers for asynchronous API calls; the default is 8.
    `ZOOM_BASE_URL` | The base URL for calls to the Zoom API.
    `ZOOM_TOKEN` | The token for authenticating to the Zoom API.
    `ZOOM_EARLIEST_FROM` | The earliest date to retrieve zoom content from.
    `DEFAULT_SLEEP_TIME` | Amount of time to sleep between re-tries when given a 429 error and no Retry-After in the response headers.
    `WAREHOUSE_INCREMENT` | A 17-digit number that can be added to some Canvas IDs to create Unizin Data Warehouse IDs (not in use currently).
    `UDW` | An object containing the necessary credential information for connecting to the Unizin Data Warehouse, where data will be pulled from.
    `CREATE_CSVS` | A boolean (`true` or `false`) indicating whether CSVs should be generated by the execution.
    `INVENTORY_DB` | An object containing the necessary credential information for connecting to a MySQL database, where output data will be inserted.


### Installation & Usage


#### With Docker

This project provides a `docker-compose.yml` file to help simplify the development and testing process. Invoking `docker-compose` will set up MySQL and a database in a container, and then it will create a separate container for the job, which will ultimately insert records into the MySQL container's database.

Before beginning, perform the following additional steps to configure the project for Docker.

1. Create two paths at your user's root level (i.e. `~`): `secrets/course-inventory` and `data/course-inventory`.

    The `docker-compose.yml` file specifies two volumes that are mapped to these directories. The first, `secrets/course-inventory`, is mapped to `config/secrets`, where the application expects to find the `env.json` file. The second, `data/course-inventory`, is mapped to the project's `data` directory, which allows later access to CSV files optionally generated by the application.

2. Move the `env.json` file to `secrets/course-inventory` so it will be mapped into the `job` container.
    ```
    mv config/secrets/env.json ~/secrets/course-inventory
    ```

Once these steps are completed, you can use the standard `docker-compose` commands to build and run the application.

1. Build the images for the `mysql` and `job` services.
    ```
    docker-compose build
    ```

2. Start up the services.
    ```
    docker-compose up
    ```

`docker-compose-up` will first start the MySQL container and then the job container. When the job finishes, the job container will stop, but the MySQL container will continue running, allowing you to enter the container and execute queries.

```
docker exec -it course_inventory_mysql /bin/bash
mysql --user=ci_user --password=ci_pw
```

Use `^C` to stop the running MySQL container, or -- if you used the detached flag `-d` with `docker-compose up` -- use `docker-compose down`.

Data in the MySQL database will persist after the container is stopped, as MySQL data is stored in a volume that is mapped to a `.data/` directory in the project. To completely reset the database, delete the `.data` directory.


#### With a Virtual Environment

You can also set up the application using `virtualenv` by doing the following:

1. Create a virtual environment using `virtualenv`.
    ```
    virtualenv venv
    source venv/bin/activate  # for Mac OS
    ```

2. Install the dependencies specified in `requirements.txt`.
    ```
    pip install -r requirements.txt
    ```

3. Initialize the database using `create_db.py`.
    ```
    python create_db.py
    ```

4. Run the application.
    ```
    python inventory.py
    ```


#### OpenShift Deployment

Deploying the application as a job using OpenShift and Jenkins involves several steps, which are beyond the scope of
this README. However, a few details about how the job is configurd are provided below.

* The `env.json` file described in the **Configuration** section above needs to be made available to 
  running course-inventory containers via an OpenShift ConfigMap, a type of Resource. A volume containing the ConfigMap 
  should be mapped to the `config/secrets` subdirectory. These details will be specified in a configuration file
  (.yaml) defining the pod.

* By default, the application will run with the assumption that the JSON configuration file will be named `env.json`. 
  However, `inventory.py` will also check for the environment variable `ENV_PATH`. This variable can be set using the 
  OpenShift pod configuration file. To use a different name for the JSON file, set `ENV_PATH` to a path beginning with `config/secrets/` and ending with the file name. 
  
  * To ensure that the `yoyo-migrations` dependency can run successfully in a containerized environment, the environment variable `USER` should be defined with the name of the project running the job. The `yoyo-migrations` library will obtain this value by using the [`getpass.getuser` function](https://docs.python.org/3/library/getpass.html) from the Python standard library.

  With the above two variables set, the `env` block in the `.yaml` will look something like this:
    ```
  - env:
      - name: ENV_FILE
        value: /config/secrets/env_test.json
      - name: USER
        value: project_name
  ```


### Database Management and Schema Changes

Currently, the database is version-controlled and managed using the [`yoyo-migrations` Python library](https://ollycope.com/software/yoyo/latest/). The migration files are located in the `db/migrations` directory.

To make changes to the database schema, perform the follow steps in order.

1. Add a new migration file to the `migrations` directory called `XXXX.add_something.py`, where `XXXX` is the next migration number (preceded by `0`s until the number is four digits) and `add_something` is an action describing the change made.

2. Within the file, import the `step` function from `yoyo`, and then for each desired schema change, pass a SQL string to `step`. Multiple step invocations can be enclosed in a list and assigned to a `steps` variable. Place each `step` in the order it should be applied. Migrations can also specify dependencies on previous migrations using the format `__depends__ = {"000X.migration_name_without_file_ending"}`.

 Refer to the existing migrations if examples are needed.

## Other Resources

Relevant Canvas API Documentation
* https://canvas.instructure.com/doc/api/accounts.html#method.accounts.courses_api
* https://canvas.instructure.com/doc/api/courses.html#Course
