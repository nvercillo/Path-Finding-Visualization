import os

from .kafka_producer import KafkaProducer, KafkaProducerFaker
from .kafka_consumer import KafkaConsumer, KafkaConsumerFaker
from .listener import get_listener_cls


class CommonConsumer:
    # THREAD COUNTS
    max_num_consumers = int(os.getenv("MAX_NUM_CONSUMERS", 10))
    max_num_listeners = int(os.getenv("MAX_NUM_LISTENERS", 3))

    # GPG
    gpg_passphrase = os.getenv("GPG_PASSPHRASE", "passphrase")
    gpg_pub_key_path = os.getenv("GPG_PUB_KEY_PATH", "../gpg/pub_key.asc")
    gpg_priv_key_path = os.getenv("GPG_PRIV_KEY_PATH", "../gpg/priv_key.asc")
    gpg_passphrase = os.getenv("GPG_PASSPHRASE", "passphrase")
    gpg_pub_key_path = os.getenv("GPG_PUB_KEY_PATH", "../gpg/pub_key.asc")
    gpg_priv_key_path = os.getenv("GPG_PRIV_KEY_PATH", "../gpg/priv_key.asc")

    # KAFKA
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL")
    sasl_plain_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_plain_password = os.getenv("KAFKA_SASL_PASSWORD")
    sasl_mechanism = os.getenv("KAKFA_SASL_MECHANISM")


class CNAConsumer(KafkaProducer, CommonConsumer):
    bucket_name = os.getenv("BUCKET_NAME", "example-customer-json")
    dump_file_path = "N/A"
    file_delimeter = "/"

    def __init__(self):

        print(
            self.bootstrap_servers,
            self.security_protocol,
            self.sasl_plain_username,
            self.sasl_plain_password,
            self.sasl_mechanism,
        )
        # self.listener = get_listener_cls(KafkaConsumer, self.max_num_listeners)
        super().__init__(
            bootstrap_server=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password,
            sasl_mechanism=self.sasl_mechanism,
        )


class FakerConsumer(KafkaProducerFaker, CommonConsumer):
    bucket_name = os.getenv("BUCKET_NAME", "example-customer-json")
    dump_file_path = "dump_file.txt"
    file_delimeter = "-"

    def __init__(self):
        # self.listener = get_listener_cls(KafkaConsumerFaker, self.max_num_listeners)
        super().__init__()
