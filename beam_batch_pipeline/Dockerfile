FROM python:3.9-slim

COPY ./app /app
COPY ./requirements.txt /app
COPY ./fetched_tweets.txt /app

WORKDIR /app

RUN pip3 install -r requirements.txt
