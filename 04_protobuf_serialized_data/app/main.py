# https://github.com/hemantkashniyal/faust-protobuf/blob/main/app/proto_serializer.py
import uuid
import logging
import faust
import config_loader as config_loader
import time
import pb_python.message_pb2 as message_pb2
from proto_serializer import ProtobufSerializer

SERVICE_NAME = "data-producer"
config = config_loader.Config()

logging.basicConfig(
    level=logging.getLevelName(config.get(config_loader.LOGGING_LEVEL)),
    format=config.get(config_loader.LOGGING_FORMAT))

logger = logging.getLogger(__name__)

app = faust.App(SERVICE_NAME, broker=config.get(config_loader.KAFKA_BROKER),
                value_serializer=ProtobufSerializer(pb_type=message_pb2.Message),
                web_host=config.get(config_loader.WEB_HOST), web_port=config.get(config_loader.WEB_PORT))
src_data_topic = app.topic(config.get(config_loader.SRC_DATA_TOPIC), partitions=8)


@app.agent(src_data_topic)
async def on_event(stream) -> None:
    async for msg_key, message in stream.items():
        logger.info(f'<<< Received message {message}')


@app.timer(interval=1.0)
async def request_data() -> None:
    author = message_pb2.Author(first_name='FName', last_name="LName")
    proto_message = message_pb2.Message(author=author, data="Hello world", created_ts=int(time.time()))
    print(f">>> Created message: {proto_message}")
    await src_data_topic.send(key=uuid.uuid1().bytes, value=proto_message)
