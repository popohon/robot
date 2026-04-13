FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-core==1.8.* dbt-trino==1.8.*

WORKDIR /opt/dbt
COPY . .
