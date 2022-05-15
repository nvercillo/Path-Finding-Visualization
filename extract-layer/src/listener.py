from threading import Lock


def get_listener_cls(kafka_consumer):
    class Listener:
        def __init__(self):
            self.mutex = Lock()
            super().__init__()

        def _recieve_msg(self):
            pass

    return Listener
