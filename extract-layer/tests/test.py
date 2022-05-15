# REQ: In order to run this test, pytest is pip library needs to be installed
import os
import json
from google.cloud import storage  # Imports the Google Cloud client library

import sys
from random import randint
from datetime import timedelta
import datetime
from precondition_checker import precondition_checker

sys.path.insert(0, "../")  # import parent folder

from src.consumer_types import CNAConsumer, FakerConsumer
from src.consumer import get_consumer_cls
from src.structs import DataType
from src.event_processor import get_event_processor_cls

from src.__init__ import logger


precondition_types = [
    "mul_val_json_uploaded",
    "single_auto_e2e",
    "single_gl_e2e",
    "single_property_e2e",
    "single_wc_e2e",
    "single_account",
    "single_auto",
    "single_gl",
    "single_property",
    "single_wc",
]


def get_test_struct_cls(base):
    class TestStruct(base):
        def __init__(self):
            self.client = storage.Client()  # Instantiates a client
            self.eventq = []  # events ready to be processed
            self.bucket = self.client.get_bucket(self.bucket_name)  # connect to bucket

    return TestStruct


def test_functionality(
    consumerCls,
    precondition="single_auto",
    read_from_dump=False,
    frequency=1,
    sanitize=True,
    scale=None,
):

    logger.info(f"RUNNING TEST ! (depenency injection: {consumerCls.__name__})")

    ts = get_test_struct_cls(consumerCls)()
    file_delimeter = consumerCls.file_delimeter

    def generate_expected(ts, consumer):

        # this function generates
        blobs = ts.client.list_blobs(ts.bucket_name)

        processed_blobs = {}  # processed timestamps and datatype order
        events = {}

        last_ts = -1
        for blob in blobs:

            if not blob.name.startswith("ingested") or blob.name.endswith(".txt"):
                continue

            timestamp = int(blob.name.split(file_delimeter)[1])
            file_name = blob.name.split(file_delimeter)[2]

            assert timestamp >= last_ts, "Ordering of timestamps incorrect"

            blob_datatype = None
            for datatype in DataType:
                if file_name == datatype.value["file_name"]:
                    blob_datatype = datatype

            assert blob_datatype is not None

            if timestamp not in processed_blobs:
                processed_blobs[timestamp] = blob_datatype
            else:
                last_sort_order_updated = processed_blobs[timestamp].value["sort_order"]
                assert (
                    blob_datatype.value["sort_order"] >= last_sort_order_updated
                ), "Ordering of file types incorrect"

            last_ts = timestamp

            unencrypt_data = consumer._get_unencrypted_data(blob)

            data = json.loads(str(unencrypt_data))
            data_keys = [d for d in data.keys()]

            assert (
                len(data.keys()) == 1
                and data_keys[0] == blob_datatype.value["json_key"] == data_keys[0]
            )

            for item in data[blob_datatype.value["json_key"]]:
                if blob_datatype.name == "account":
                    events[(item["ACCT_NBR"], blob_datatype.name.upper())] = item
                else:
                    events[
                        (item["ACCT_NBR"], item["POL_NBR"], blob_datatype.name.upper())
                    ] = item

        return events

    def check_correct_results(expected_events, dump_file_path):
        with open(dump_file_path) as f:
            lines = f.readlines()

        account = True
        for line in lines:
            event = json.loads(line)

            type = [k for k in event.keys()][0]

            if account and type != "ACCOUNT":
                account = False

            elif not account and type == "ACCOUNT":
                raise (
                    "EXECPTION, ordering of events incorrect. Accounts should be processed first."
                )

            if type.lower() not in [d.name for d in DataType]:
                raise (f"Data type {type} not expected")

            if account:
                key_tpl = (event[type]["ACCT_NBR"], type)
            else:
                key_tpl = (event[type]["ACCT_NBR"], event[type]["POL_NBR"], type)

            assert (
                key_tpl in expected_events
            ), f"Key tuple: {key_tpl} not in expected events"

            assert (
                expected_events[key_tpl] == event[type]
            ), f"EXPECTED  {expected_events[key_tpl]}, GOT {event[type]} "

        logger.info("ALL EVENTS PROCESSED SUCCESSFULLY!\n")

    # try:
    precondition_checker(
        ts,
        consumerCls.file_delimeter,
        precondition,
        read_from_dump,
        frequency=frequency,
        sanitize=sanitize,
        scale=scale,
    )
    # except:
    #     pass  # skip invalid file

    consumer = get_consumer_cls(
        get_event_processor_cls(consumerCls)  # test dependency injection
    )()
    consumer._process_event_queue()

    print("NUM EVENTS INGESTED", consumer.counter)

    # if faker
    dump_file_path = consumerCls.dump_file_path
    if dump_file_path != "N/A":
        expected_events = generate_expected(ts, consumer)
        check_correct_results(expected_events, dump_file_path)
    # fi


# precondition_types = [
#     "mul_val_json_uploaded",
#     "single_auto_e2e",
#     "single_gl_e2e",
#     "single_property_e2e",
#     "single_wc_e2e",
#     "single_account",
#     "single_auto",
#     "single_gl",
#     "single_property",
#     "single_wc",
# ]

ConsumerCls = (
    FakerConsumer if os.getenv("FAKE_KAFKA_CONSUMER_DEP_INJ") == "true" else CNAConsumer
)

test_functionality(
    ConsumerCls,
    "mul_val_json_uploaded",
    read_from_dump=True,
    frequency=1,
    sanitize=False,
    # scale=False  # number of accounts
    # FakerCon sumer, "single_account", frequency=1, sanitize=True, scaled=False
)
# test_functionality(
#     CNAConsumer,
#     "mul_val_json_uploaded",
#     read_from_dump=False,
#     frequency=1,
#     sanitize=False,
#     # scale=False  # number of accounts
#     # FakerCon sumer, "single_account", frequency=1, sanitize=True, scaled=False
# )
