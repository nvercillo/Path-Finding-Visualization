import json
from datetime import datetime

from src.structs import ErrorMessage
from src.__init__ import logger
import time


reference_folder = "../models/reference"


class EventValidator:
    def __init__(self, file_delimeter):
        self.file_delimeter = file_delimeter

    def _validate_event(self, event, data_string):
        logger.info(f"Validating event {event}")
        self._validate_file_name(event)
        return self._validate_object(event, data_string)

    def _validate_object(self, event, data_string):
        try:
            data = json.loads(data_string)
        except:
            event._set_error_msg(
                error_msg=ErrorMessage(
                    f"Invalid json for file {event.blob.name}, failed parsing"
                )
            )

            return

        if not isinstance(data, dict):

            event._set_error_msg(
                error_msg=ErrorMessage(
                    f"Invalid json for file {event.blob.name}, failed parsing"
                )
            )

            return

        if data == {}:

            event._set_error_msg(
                error_msg=ErrorMessage(
                    f"File named {event.blob.name} contains empty JSON object"
                )
            )

            return

        init_key = [_ for _ in data.keys()][0]
        if init_key != event.type.value["json_key"]:

            event._set_error_msg(
                error_msg=ErrorMessage(
                    f"Expected data type: {event.type.value}, in JSON object"
                )
            )

            return

        with open(f'{reference_folder}/{event.type.value["file_name"]}') as f:
            reference_object = json.load(f)

        result = self._compare_object_types(reference_object, data)

        if isinstance(result, ErrorMessage):
            event._set_error_msg(error_msg=result)

        return data

    def _compare_object_types(self, reference_object, data):

        ref_type_map = {}
        data_type_map = {}

        arrays_to_delete = []

        def dfs(obj, key, type_map):
            object = obj[key]

            if isinstance(object, dict):
                for k in object.keys():
                    type_map[k] = {}
                    dfs(object, k, type_map[k])

            elif isinstance(object, list) and not isinstance(object, str):

                if len(object) > 0:
                    object = object[0]
                    object["array"] = True

                    arrays_to_delete.append(object)

                    for k in object.keys():
                        type_map[k] = {}
                        dfs(object, k, type_map[k])
            else:
                type_map = type(object)

        initial_key = [k for k in reference_object.keys()][0]

        dfs({initial_key: reference_object}, initial_key, ref_type_map)
        dfs({initial_key: data}, initial_key, data_type_map)

        diff_path = self.compare_dicts(
            {"first": ref_type_map}, {"first": data_type_map}, "first"
        )

        if diff_path is not None:
            diff_path = diff_path[::-1]
            arr = ".".join(diff_path)
            return ErrorMessage(f"Object was missing mandatory field: {arr}")

        print("\ndiff_path", diff_path)
        print(ref_type_map)
        print(data_type_map)

        diff_path = self.compare_dicts(
            {"first": data_type_map}, {"first": ref_type_map}, "first"
        )

        if diff_path is not None:
            diff_path = diff_path[::-1]
            arr = ".".join(diff_path)
            return ErrorMessage(f"Object contained extra unexpected field: {arr}")

        for obj in arrays_to_delete:
            obj.pop("array", None)

        print("\ndiff_path", diff_path)
        print(ref_type_map)
        print(data_type_map)

        return True

    def compare_dicts(self, obj1, obj2, key):

        object1 = obj1[key]

        if key not in obj2:
            return [key]

        object2 = obj2[key]

        if isinstance(object1, dict):
            if not isinstance(object2, dict):
                return [k]

            shared_keys = set([])
            for k in object1.keys():
                if k not in object2:
                    return [k]

                shared_keys.add(k)
                res = self.compare_dicts(object1, object2, k)

                if isinstance(res, list):
                    res.append(k)
                    return res

    def _validate_file_name(self, event):
        file_name = event.blob.name

        if self.file_delimeter in file_name:
            unix_time = file_name.split(self.file_delimeter)[0]

            now_time = time.time()

            if float(unix_time) > now_time:
                event._set_error_msg(
                    ErrorMessage(
                        f"Timestamp cannot be dated in the future. GOT: {unix_time}, EXPECTED: < {int(now_time)}"
                    )
                )

            dt = datetime.fromtimestamp(int(unix_time))
            approx_start = datetime.strptime("2022-03", "%Y-%m")

            if dt < approx_start:
                event._set_error_msg(
                    ErrorMessage(
                        f"Timestamp cannot be dated before existence of pipeline. GOT: {unix_time}, EXPECTED: > 2022-03"
                    )
                )

        else:
            event._set_error_msg(
                ErrorMessage(
                    f"Invalid file format. GOT: {file_name}, EXPECTED: <unix_time>{self.file_delimeter}<file_type>"
                )
            )


{
    "Account": {
        "ACCT_NBR": {},
        "NAMED_INSURED_NM": {},
        "CNA_SIC": {},
        "SIC_DESCRIPTION": {},
        "AGENCY_NAME": {},
        "SYS_GEN_DUNS": {},
        "CUSTOMER_SEGMENT": {},
        "YEARS_IN_BUSINESS": {},
        "BANKRUPTCY_INDICATOR": {},
        "PAYDEX": {},
        "PERCENT_NEGATIVE_PAYMENT": {},
        "PERCENT_SLOW_PAYMENT": {},
        "PERCENT_TOTAL_PAYMENT": {},
        "POLICY": {
            "TAP_POLICY_STATUS": {},
            "POL_NBR": {},
            "TERM_EFF_DT": {},
            "TERM_EXP_DT": {},
            "POL_SYMBOL_CD": {},
            "SBU_NAME": {},
            "PRODUCER_NBR": {},
            "EFFECTIVE_TYPE_CD": {},
            "POL_SIC": {},
            "POL_INS_LINE_TS": {},
            "array": {},
        },
        "LOSS": {},
        "array": {},
    }
}
{
    "Account": {
        "ACCT_NBR": {},
        "NAMED_INSURED_NM": {},
        "CNA_SIC": {},
        "SIC_DESCRIPTION": {},
        "AGENCY_NAME": {},
        "SYS_GEN_DUNS": {},
        "CUSTOMER_SEGMENT": {},
        "YEARS_IN_BUSINESS": {},
        "BANKRUPTCY_INDICATOR": {},
        "PAYDEX": {},
        "PERCENT_NEGATIVE_PAYMENT": {},
        "PERCENT_SLOW_PAYMENT": {},
        "PERCENT_TOTAL_PAYMENT": {},
        "POLICY": {
            "TAP_POLICY_STATUS": {},
            "POL_NBR": {},
            "TERM_EFF_DT": {},
            "TERM_EXP_DT": {},
            "POL_SYMBOL_CD": {},
            "SBU_NAME": {},
            "PRODUCER_NBR": {},
            "EFFECTIVE_TYPE_CD": {},
            "POL_SIC": {},
            "POL_INS_LINE_TS": {},
            "array": {},
        },
        "LOSS": {},
        "array": {},
    }
}
