# FROM directive instructing base image to build upon
FROM python:3.7

RUN apt-get update && apt-get --no-install-recommends install --yes \
    libaio1 libaio-dev xmlsec1 libffi-dev \
    libldap2-dev libsasl2-dev \
    build-essential default-libmysqlclient-dev git netcat

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