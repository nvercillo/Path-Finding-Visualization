import random
from string import digits
from threading import Lock
from asyncio.log import logger
import json
import traceback
import six
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from logging import (
    Handler,
    Formatter,
    CRITICAL,
    ERROR,
    WARNING,
    INFO,
    FATAL,
    DEBUG,
    NOTSET,
    Logger,
)

digits = [f"{i}" for i in range(1, 10)]


ERROR_COLOR = "danger"  # color name is built in to Slack API
WARNING_COLOR = "warning"  # color name is built in to Slack API
INFO_COLOR = "#439FE0"

COLORS = {
    CRITICAL: ERROR_COLOR,
    FATAL: ERROR_COLOR,
    ERROR: ERROR_COLOR,
    WARNING: WARNING_COLOR,
    INFO: INFO_COLOR,
    DEBUG: INFO_COLOR,
    NOTSET: INFO_COLOR,
}

DEFAULT_EMOJI = ":heavy_exclamation_mark:"


class LogHandler(Handler):  # Slack / Stream Handler
    def __init__(
        self,
        token,
        channel,
        stack_trace=True,
        username="Python logger",
        icon_url=None,
        icon_emoji=None,
        fail_silent=False,
        ping_level=None,
    ):
        Handler.__init__(self)

        self.etl_layer = "Extract-Layer"

        self.stack_trace = stack_trace
        self.fail_silent = fail_silent

        self.trace_id = "".join(random.choice(digits) for _ in range(6))
        self.mutex = Lock()
        self.client = WebClient(token=token)

        self.username = username
        self.icon_url = icon_url
        self.icon_emoji = icon_emoji if (icon_emoji or icon_url) else DEFAULT_EMOJI
        self.channel = channel
        if not self.channel.startswith("#") and not self.channel.startswith("@"):
            self.channel = "#" + self.channel

    def set_trace_id(self):
        self.trace_id = "".join(random.choice(digits) for _ in range(6))

    def build_msg(self, record):
        return six.text_type(self.format(record))

    def build_trace(self, record, fallback):
        trace = {"fallback": fallback, "color": COLORS.get(self.level, INFO_COLOR)}

        if record.exc_info:
            trace["text"] = "\n".join(traceback.format_exception(*record.exc_info))

        return trace

    def emit(self, record):
        message = self.build_msg(record)

        if self.stack_trace:
            trace = self.build_trace(record, fallback=message)
            attachments = json.dumps([trace])
        else:
            attachments = None

        traces = traceback.format_exception(*record.exc_info)

        exception_name = traces[-1].split(":")[0]

        self.mutex.acquire()

        arr = message.split("\n")
        arr[0] += f"     -Traceid: {self.trace_id}  -Error Type: {exception_name}"

        message = "\n".join(arr)

        non_sensitive_msg = arr[0]

        try:
            self.client.chat_postMessage(
                text=non_sensitive_msg,
                channel=self.channel,
                username=self.username,
                icon_url=self.icon_url,
                icon_emoji=self.icon_emoji,
                attachments=json.dumps(
                    [
                        {
                            "text": f"TRACE ID: {self.trace_id}\nCHECK GCP LOGS\nError occured in: {self.etl_layer}",
                            "color": ERROR_COLOR,
                        }
                    ]
                ),
            )
        except SlackApiError as e:
            if self.fail_silent:
                pass
            else:
                raise e

        # print to console
        print(non_sensitive_msg)

        self.set_trace_id()
        self.mutex.release()
