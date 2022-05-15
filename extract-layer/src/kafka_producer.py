from distutils.command import clean
from email import message_from_binary_file
import os
from kafka import KafkaProducer as KProducer
import json
from pprint import pprint
from src.__init__ import logger


class KafkaProducer:

    TOPIC = "vhxh8ncz-data-transform"

    def __init__(
        self,
        bootstrap_server,
        security_protocol,
        sasl_plain_username=None,
        sasl_plain_password=None,
        sasl_mechanism=None,
    ):
        self.counter = 0
        self.producer = KProducer(
            # bootstrap_servers=os.environ.get("KAFKA_SERVER"),
            bootstrap_servers=[bootstrap_server],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol=security_protocol,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_mechanism=sasl_mechanism
            # ssl_cafile=os.environ.get("KAFKA_SSL_CAFILE"),
            # ssl_certfile=os.environ.get("KAFKA_SSL_CERTFILE"),
            # ssl_keyfile=os.environ.get("KAFKA_SSL_KEYFILE"),
        )

    def _write_to_topic(self, key, message):
        self.counter += 1
        self.producer.send(
            self.TOPIC, key=str(key).encode("utf-8"), value=message
        )  # key ensures that all logs with same key are in the same partition, gauranteeing order


class KafkaProducerFaker:

    TOPIC = "data-transform"

    def __init__(self, clean_dump_file=True):
        self.counter = 0

        if clean_dump_file:
            with open(self.dump_file_path, "w") as f:
                f.write("")

    def _write_to_topic(self, key, message):
        # logger.info(f"WRITING: {key}, Message: {message}")
        self.counter += 1

        f = open(self.dump_file_path, "a+")
        f.write(message + " \n")

        return message
