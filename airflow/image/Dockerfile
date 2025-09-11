FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow

# Install provider packages from requirements.txt
COPY gcp.secrets.json /gcp.secrets.json
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt