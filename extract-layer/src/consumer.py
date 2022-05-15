from email import message
import os
import json
from google.cloud import storage  # Imports the Google Cloud client library

from src.validator import EventValidator
from src.structs import DataType, ErrorMessage, Event
from src.__init__ import logger
from src.gpg_client import GPGClient


def get_consumer_cls(base):
    class Consumer(base):

        rand_unix_time = 0

        def __init__(self):
            logger.info("Processing events... \n")

            super().__init__(self.max_num_consumers)  # event processor init

            self.batch_events = []  # sorted array of array of events

            self.locks = {}
            self.eventq = []

            self.storage_client = storage.Client()  # Instantiates a storage client

            self.gpg_client = GPGClient(
                gpg_pub_key_path=self.gpg_pub_key_path,
                gpg_priv_key_path=self.gpg_priv_key_path,
                passphrase=self.gpg_passphrase,
            )  # Instantiates a gpg client

            self.bucket = self.storage_client.get_bucket(
                self.bucket_name
            )  # connect to bucket

        def _list_file_queue(self):
            res = self.storage_client.list_blobs(self.bucket_name)

            self.locked_blobs = []
            self.ingested_blobs = set({})  # set of names
            self.rejected_blobs = set({})  # set of names

            self.eventq = []
            for blob in res:

                if self.file_delimeter in blob.name:
                    if ".txt" in blob.name:
                        continue

                if (
                    blob.name[-1] != self.file_delimeter
                    and not blob.name.startswith("locks")
                    and not blob.name.startswith("ingested")
                    and not blob.name.startswith("rejected")
                ):  # filter out unneeded

                    data_type_names = []
                    blob_data_type = None
                    for datatype in DataType:

                        if datatype.name != DataType.null.name:
                            data_type_names.append(datatype.name)
                            if datatype.name in blob.name:
                                blob_data_type = datatype

                    error_msg = None
                    if blob_data_type is None:
                        error_msg = ErrorMessage(
                            f"Given file type not found. GOT: {blob.name}, EXPECTED: one of {data_type_names}"
                        )

                        blob_data_type = DataType.null

                    event = Event(blob, blob_data_type, error_msg)

                    self.eventq.append(event)

                elif blob.name.startswith("ingested"):
                    self.ingested_blobs.add(blob.name)

                elif blob.name.startswith("rejected"):
                    self.rejected_blobs.add(blob.name)

                elif blob.name.startswith("locks"):
                    self.locked_blobs.append(blob)

            self.eventq.sort()

            if len(self.eventq) > 0:
                prio = self.eventq[0].type.value["sort_order"]
                batch = []

                for event in self.eventq:
                    if event.type.value["sort_order"] > prio:
                        self.batch_events.append(batch)
                        batch = [event]
                        prio = event.type.value["sort_order"]
                    else:
                        batch.append(event)
                self.batch_events.append(batch)

            logger.info(f"Event Queue Length: {len(self.eventq)}")
            logger.info(f"Event Queue Content: {(self.eventq)}")

        def _process_event_queue(self):
            self._list_file_queue()

            self._process_event_queue

            # process files
            for batch in self.batch_events:
                self._process_all_events(batch, self._process_blob)

            # move processed files
            self._process_all_events(self.eventq, self._post_process_blob)

            # self.listener._recieve_messages(self.eventq)

        def _post_process_blob(self, event):

            blob = event.blob
            rejected = event.rejected

            if not rejected:
                logger.info(f"Blob {blob.name} ingested ...")
                blob_copy = self.bucket.copy_blob(
                    blob, self.bucket, f"ingested{self.file_delimeter}{blob.name}"
                )
                blob.delete()  # delete the original blob
            else:  # rejected
                logger.info(f"Blob {blob.name} rejected ...")

                try:
                    exception_string = (
                        f'ERROR: "{event.error_msg}", occured for file {blob.name}'
                    )

                    blob_copy = self.bucket.copy_blob(
                        blob, self.bucket, f"rejected{self.file_delimeter}{blob.name}"
                    )

                    log_blob = self.bucket.blob(
                        f"rejected{self.file_delimeter}{blob.name}.log"
                    )
                    with log_blob.open("w") as f:
                        f.write(f"ERROR MESSAGE: {exception_string}")
                    raise Exception(exception_string)
                except:
                    logger.exception(exception_string)
                finally:
                    blob.delete()  # delete the original blob

        def _process_blob(self, event: Event):

            logger.info(f"Processing blob {event.blob.name}")
            res = self._get_unencrypted_data(event.blob)

            if type(res) == ErrorMessage:
                event._set_error_msg(res)  # sets rejected as well

            if event.rejected:
                return

            # try:
            # data = EventValidator(self.file_delimeter)._validate_event(
            #     event, res
            # )  # returns json
            # except:
            #     event._set_error_msg(
            #         ErrorMessage("Event validator failed for unknown reason")
            #     )

            data = json.loads(res)

            if event.rejected:
                return

            elements = data[event.type.value["json_key"]]
            for ele in elements:

                account_num = ele["ACCT_NBR"]  # should never fail checked by validator

                self._write_to_topic(
                    account_num,
                    json.dumps(
                        {
                            event.type.name.upper(): ele,
                        }
                    ),
                )

        def _get_unencrypted_data(self, blob):
            try:
                with blob.open("r") as f:
                    enc_data = f.read()

                decrypted_data = self.gpg_client._get_unencrypted_data(
                    enc_data
                )  # may return error message

                if isinstance(decrypted_data, ErrorMessage):
                    return decrypted_data
                else:
                    logger.info(f"Decrypted data succeeded: {decrypted_data.ok}")
                    return str(decrypted_data)
            except Exception as err:
                return ErrorMessage(str(err))

    return Consumer
