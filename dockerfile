FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow

# Install provider packages from requirements.txt
#COPY requirements.txt /tmp/requirements.txt
#RUN pip install -r /tmp/requirements.txt

# install the github provider package
COPY requirements.txt /tmp/requirements.txt
RUN pip install apache-airflow-providers-github