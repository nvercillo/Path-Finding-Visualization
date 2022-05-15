import logging
import os
import datetime

from library.log_handler import LogHandler


class Logger:
    _logger = None

    def __new__(cls, *args, **kwargs):

        if cls._logger is None:
            cls._logger = super().__new__(cls, *args, **kwargs)
            cls._logger = logging.getLogger("crumbs")
            cls._logger.setLevel(logging.DEBUG)

            formatter = logging.Formatter(
                "%(asctime)s \t [%(levelname)s | %(filename)s:%(lineno)s] > %(message)s"
            )

            now = datetime.datetime.now()
            dirname = "./log"

            if not os.path.isdir(dirname):
                os.mkdir(dirname)

            fileHandler = logging.FileHandler(
                dirname + "/log_" + now.strftime("%Y-%m-%d") + ".log"
            )

            streamHandler = logging.StreamHandler()

            logHandler = LogHandler(
                os.getenv("SLACK_BOT_TOKEN"),
                "test",
                stack_trace=True
                # os.getenv("SLACK_BOT_TOKEN"), "test", stack_trace=True
            )

            logHandler.setLevel(logging.ERROR)
            streamHandler.setLevel(logging.INFO)

            fileHandler.setFormatter(formatter)
            streamHandler.setFormatter(formatter)

            cls._logger.addHandler(fileHandler)
            cls._logger.addHandler(streamHandler)
            cls._logger.addHandler(logHandler)

        return cls._logger
