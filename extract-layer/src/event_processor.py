from asyncio import events
import time
import threading
from threading import Thread, Lock

# from src.structs import DataType, BlobEvent, ErrorMessage
import json
from pprint import pprint


def get_event_processor_cls(base):
    class BlobEventProcessor(base):
        def __init__(self, max_num_consumers):
            super().__init__()

            self.max_num_consumers = max_num_consumers
            self.worker_threads = []

            self.mutex = Lock()
            self.work_ind = 0

        def _process_all_events(self, events, process_event_fn):
            self.work_ind = 0
            self.event_queue = events
            for self.work_ind in range(
                min(self.max_num_consumers, len(self.event_queue))
            ):
                thread = Thread(
                    target=self._worker_thread, args=(self.work_ind, process_event_fn)
                )
                self.worker_threads.append(thread)
            self.work_ind += 1

            for thread in self.worker_threads:
                thread.start()

            for thread in self.worker_threads:
                thread.join()

            self.worker_threads = []

        def _worker_thread(self, init_work_ind, process_event_fn):
            event = self.event_queue[init_work_ind]
            process_event_fn(event)

            while True:
                self.mutex.acquire()
                if self.work_ind < len(self.event_queue):
                    work_ind = self.work_ind
                    self.work_ind += 1
                    event = self.event_queue[work_ind]
                    self.mutex.release()
                    process_event_fn(event)

                else:
                    self.mutex.release()
                    break

    return BlobEventProcessor
