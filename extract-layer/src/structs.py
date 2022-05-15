import os
from enum import Enum


POSSIBLE_POLICIES = [
    "Rated Not Quoted",
    "Quote Not Taken",
    "Pending Quote",
    "Invalid",
    "Declined",
    "Issued",
]


class ErrorMessage:
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.msg


class DataType(Enum):
    account = {
        "file_name": "account.json",
        "sort_order": 1,
        "json_key": "Account",
        "POL_LOB": "N/A",
    }
    wc = {
        "file_name": "wc.json",
        "sort_order": 2,
        "json_key": "WC",
        "POL_LOB": "WC",
    }
    property = {
        "file_name": "property.json",
        "sort_order": 2,
        "json_key": "Property",
        "POL_LOB": "PMT",
    }
    auto = {
        "file_name": "auto.json",
        "sort_order": 2,
        "json_key": "Auto",
        "POL_LOB": "BUA",
    }
    gl = {
        "file_name": "gl.json",
        "sort_order": 2,
        "json_key": "GL",
        "POL_LOB": "GL",
    }

    null = {
        "file_name": "N/A",
        "sort_order": 99,
        "json_key": "N/A",
        "POL_LOB": "N/A",
    }


class Event:
    def __init__(self, blob, type: DataType, error_msg=None):
        self.blob = blob
        self.type = type

        if error_msg is not None:
            self.rejected = True
        else:
            self.rejected = False

        self.error_msg = error_msg

    def __lt__(self, other):
        # sort by type first, then get most recent
        if self.type is None or self.type.name == DataType.null.name:
            return False
        if other.type is None:
            return True
        if self.type.value["sort_order"] == other.type.value["sort_order"]:
            return self.blob.name < other.blob.name
        else:
            return self.type.value["file_name"] < other.type.value["file_name"]

    def _set_error_msg(self, error_msg: ErrorMessage):
        self.rejected = True
        self.error_msg = error_msg

    def __repr__(self) -> str:
        return f"<Event type: {self.type.name if self.type is not None else 'NONE'},  blob: {self.blob.name},  rejected: {self.rejected} >"

    def __str__(self) -> str:
        return self.__repr__()
