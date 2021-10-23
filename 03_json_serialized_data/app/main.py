import uuid
import logging
import faust
import json
import config_loader as config_loader
import models
import time
import pydantic_serializer

SERVICE_NAME = "data-producer"
config = config_loader.Config()

logging.basicConfig(
    level=logging.getLevelName(config.get(config_loader.LOGGING_LEVEL)),
    format=config.get(config_loader.LOGGING_FORMAT))

logger = logging.getLogger(__name__)

app = faust.App(SERVICE_NAME, broker=config.get(config_loader.KAFKA_BROKER),
                value_serializer=pydantic_serializer.PydanticSerializer(models.Message),
                web_host=config.get(config_loader.WEB_HOST), web_port=config.get(config_loader.WEB_PORT))
src_data_topic = app.topic(config.get(config_loader.SRC_DATA_TOPIC), partitions=8)


@app.agent(src_data_topic)
async def on_event(stream) -> None:
    async for msg_key, message in stream.items():
        logger.info(f"<<< Received message: {message}")


@app.timer(interval=1.0)
async def request_data() -> None:
    author = models.Author(
        first_name="FName",
        last_name="LName"
    )

    message = models.Message(
        author=author,
        data="Hello world",
        created_ts=int(time.time())
    )
    logger.info(f">>> Created message: {message}")
    await src_data_topic.send(key=uuid.uuid1().bytes, value=message)
