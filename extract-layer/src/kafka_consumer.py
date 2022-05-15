import json
from kafka import KafkaConsumer as KConsumer


class KafkaConsumer:
    def __init__(
        self,
        kafka_server,
        kafka_security_protocol,
        ssl_cafile,
        ssl_certfile,
        ssl_keyfile,
    ):
        TOPIC = "data-transform-ack"

        self.consumer = KConsumer(
            TOPIC,
            group_id="tranformation-layer",
            bootstrap_servers=[
                "tricycle-01.srvs.cloudkafka.com:9094",
                "tricycle-02.srvs.cloudkafka.com:9094",
                "tricycle-03.srvs.cloudkafka.com:9094",
            ],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            # key_deserializer=lambda m: json.loads(m.decode("ascii")),
            auto_offset_reset="earliest",
            security_protocol="SASL_SSL",
            sasl_plain_username="vhxh8ncz",
            sasl_plain_password="UrjWUFz7D1D_SQu1zJDi54kJLZ4IMS9D",
            sasl_mechanism="SCRAM-SHA-256"
            # ssl_cafile=ssl_cafile,
            # ssl_certfile=ssl_certfile,
            # ssl_keyfile=ssl_keyfile,
        )


class KafkaConsumerFaker:
    def __init__(self, fixture_file="fixture1.json"):
        TOPIC = "data-load"
        self.fixture_file = "dump_file.txt"
        with open(fixture_file, "r") as f:
            data = json.load(f)

        self.consumer = data
