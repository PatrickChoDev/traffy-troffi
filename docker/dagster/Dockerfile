FROM python:3.12-slim

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker

ENV DAGSTER_HOME=/dagster

WORKDIR $DAGSTER_HOME