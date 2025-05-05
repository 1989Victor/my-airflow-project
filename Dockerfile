FROM apache/airflow:2.9.0

WORKDIR /

COPY requirements.txt /


RUN pip install -r requirements.txt 