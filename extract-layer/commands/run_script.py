from src.consumer import get_consumer_cls
from src.event_processor import get_event_processor_cls
from src.kafka_producer import KafkaProducer


get_consumer_cls(get_event_processor_cls(KafkaProducer)())()._process_event_queue()


# vhxh8ncz-data-transform
