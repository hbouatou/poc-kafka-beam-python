import asyncio
import os
import uvicorn
import json
import time
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'localhost:9092')

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/')
def ping():
    return "ping"


async def get_topic_message(topic, request):
    for message in KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVERS):
        if await request.is_disconnected():
            logging.info('Client closed the connection')
            break
        word, count = json.loads(message.value)
        yield json.dumps({
            'word': word,
            'count': count,
            'offset': message.offset,
            'partition': message.partition
        })
        await asyncio.sleep(1)


@app.get('/stream-events/{topic}')
async def get_topic_events(topic: str, request: Request):
    kafka_messages = get_topic_message(topic, request)
    return EventSourceResponse(kafka_messages)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
