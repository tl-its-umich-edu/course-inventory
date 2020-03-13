# FROM directive instructing base image to build upon
FROM python:3.7

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

WORKDIR /app/
COPY . /app/

# Sets the local timezone of the docker image
ENV TZ=America/Detroit
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["python", "inventory.py"]

# Done!