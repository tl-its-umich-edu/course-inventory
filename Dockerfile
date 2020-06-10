# FROM directive instructing base image to build upon
FROM python:3.8-slim

RUN apt-get update && apt-get --no-install-recommends install --yes \
    build-essential default-libmysqlclient-dev git && \
    apt-get clean -y 

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

WORKDIR /app/

# This works even if docker-compose has already mounted "." as "/app".
# It appears to just skip this step.  Or maybe it finds that "/app" already contains
# everything from ".".
COPY . /app/

# Sets the local timezone of the docker image
ENV TZ=America/Detroit
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["python", "run_jobs.py"]

# Done!