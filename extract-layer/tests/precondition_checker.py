from pprint import pprint

# REQ: In order to run this test, pytest is pip library needs to be installed
import os
import random
import sys
import time
import datetime
from random import randint
import json
import copy

# from misc_functions import scale_json

sys.path.insert(0, "../")  # import parent folder

from src.utils import Utils
from library.faker_factory import RandomizationUtils
from src.structs import DataType, ErrorMessage, POSSIBLE_POLICIES
from src.__init__ import logger


digits = [str(d) for d in range(1, 10)]
path_to_sanitized_models = "../models/sanitized"
dump_folders = [
    "../models/real/1559365200",
    "../models/real/1590987600",
    "../models/real/1622523600",
    "../models/real/1654059600",
]


def precondition_checker(
    ts,
    file_delimeter,
    precondition_type,
    read_from_dump,
    frequency,
    sanitize=True,
    scale=None,
):
    # fn returns true if precondition was apriorily satisfied
    # else fixture data generated

    if scale is None:
        path_to_scaled_models = path_to_sanitized_models

    logger.info("Cleaning, preparing bucket for test case... \n")
    blobs = ts.client.list_blobs(ts.bucket_name)
    ts_dict = set({})

    root_files = set(
        [
            f"locks{file_delimeter}",
            f"ingested{file_delimeter}",
            f"rejected{file_delimeter}",
        ]
    )
    for blob in blobs:

        if (
            blob.name.startswith("locks")
            and blob.name != f"locks{file_delimeter}"
            or blob.name.startswith("ingested")
            and blob.name != f"ingested{file_delimeter}"
            or blob.name.startswith("rejected")
            and blob.name != f"rejected{file_delimeter}"
        ):

            # remove anything not processed cause of clean suffix
            blob.delete()
            pass

        if (
            blob.name[-1] != file_delimeter
            and not blob.name.startswith("locks")
            and not blob.name.startswith("ingested")
            and not blob.name.startswith("rejected")
        ):  # filter out unneeded:

            if blob.name in root_files:
                root_files.pop(blob.name)
            arr = blob.name.split(file_delimeter)

            if len(arr) == 2:
                timestamp = arr[0]
                ts_dict.add(timestamp)

    out_folder = "./json_objs"
    try:
        os.mkdir(out_folder)
    except FileExistsError as err:
        pass  # folder already exists

    encrypted = "./encrypted"
    try:
        os.mkdir(encrypted)
    except FileExistsError as err:
        pass  # folder already exists

    if len(ts_dict) >= frequency:
        logger.info("Randomized fixtures already set .... ")
        return True

    datatypes = {
        "single_auto_e2e": [DataType.account, DataType.auto],
        "single_gl_e2e": [DataType.account, DataType.gl],
        "single_property_e2e": [DataType.account, DataType.property],
        "single_wc_e2e": [DataType.account, DataType.wc],
        "single_account": [DataType.account],
        "single_auto": [DataType.auto],
        "single_gl": [DataType.gl],
        "single_property": [DataType.property],
        "single_wc": [DataType.wc],
        "mul_val_json_uploaded": sorted(
            [datatype for datatype in DataType],
            key=lambda k: k.value["sort_order"],
        ),
    }[precondition_type]

    if scale is not None:
        logger.info(f"Scaling jsons with a scaling factor of {scale}")
        # for datatype in DataType:
        #     if datatype.name != DataType.null.name:
        #         scale_json(
        #             in_folder_path=path_to_sanitized_models,
        #             datatype=datatype,
        #             out_folder_path=path_to_sanitized_models,
        #             scale=scale,
        #         )

    logger.info("Generating randomized fixtures .... ")
    if read_from_dump:
        for _folder in dump_folders:
            for datatype in DataType:
                if datatype.name == DataType.null.name:
                    continue

                time_stamp = _folder.split("/")[-1]
                in_file_path = f"{_folder}/{datatype.value['file_name']}"

                with open(in_file_path) as f:
                    json_obj = f.read()

                file_name = in_file_path.split("/")[-1]

                file_path = f"{out_folder}/{file_name}"

                with open(file_path, "w") as f:
                    f.write(json_obj)

                encrypted_file_path = Utils._encrypt_local_file(
                    file_path=file_path, output_folder_path=encrypted
                )

                with open(encrypted_file_path, "r") as f:
                    encrypt_data = f.read()

                new_blob = ts.bucket.blob(
                    f"{time_stamp}{file_delimeter}{datatype.value['file_name']}"
                )

                with new_blob.open("w") as f:
                    f.write(encrypt_data)

        return True

    for _ in range(frequency - len(ts_dict)):

        tod = datetime.datetime.now()
        d = datetime.timedelta(days=randint(-10, -1))
        new_dt = tod + d

        rand_unix_time = int(time.mktime(new_dt.timetuple()))

        account_policies = {
            # account_id: {
            #   type: [policy]
            # }
        }

        for i, datatype in enumerate(datatypes):
            if datatype.name == "null":
                continue

            in_file_path = f"{path_to_sanitized_models}/{datatype.value['file_name']}"

            obj = RandomizationUtils().sanitize_json(
                in_file_path=in_file_path,
                return_dict=True,
                sanitize=sanitize,
            )

            if sanitize:
                if datatype.name == DataType.account.name:
                    for account in obj[datatype.value["json_key"]]:
                        account_id = account["ACCT_NBR"]
                        account_policies[account_id] = {}

                        policy_types = [d for d in DataType]
                        policy_types.remove(DataType.account)
                        policy_types.remove(DataType.null)

                        policy_types.sort(key=lambda k: k.name)
                        if len(account["POLICY"]) < len(policy_types):
                            raise Exception("make more policies")

                        for i, policy in enumerate(account["POLICY"]):

                            new_policy = copy.deepcopy(policy)

                            new_policy["POL_NBR"] = "".join(
                                random.choice(digits) for _ in range(8)
                            )

                            new_policy["STATUS"] = random.choice(POSSIBLE_POLICIES)

                            new_policy["POL_SYMBOL_CD"] = policy_types[
                                i % len(policy_types)
                            ].value["POL_LOB"]

                            if (
                                new_policy["POL_SYMBOL_CD"]
                                not in account_policies[account_id]
                            ):
                                account_policies[account_id][
                                    new_policy["POL_SYMBOL_CD"]
                                ] = []

                            account_policies[account_id][
                                new_policy["POL_SYMBOL_CD"]
                            ].append(new_policy)
                else:
                    account_ids = [k for k in account_policies.keys()]
                    acct_ind = 0
                    for policy_ind in range(len(obj[datatype.value["json_key"]])):

                        skip = False

                        if len(account_ids) > 0:

                            obj[datatype.value["json_key"]][policy_ind][
                                "ACCT_NBR"
                            ] = account_id

                            if (
                                len(
                                    account_policies[account_id][
                                        datatype.value["POL_LOB"]
                                    ]
                                )
                                == 0
                            ):
                                acct_ind += 1
                                if acct_ind > len(account_ids):
                                    skip = True

                            if skip is False:
                                if (
                                    len(
                                        account_policies[account_ids[acct_ind]][
                                            datatype.value["POL_LOB"]
                                        ]
                                    )
                                    == 0
                                ):
                                    acct_ind += 1
                                    if acct_ind > len(account_ids):
                                        break
                                    continue

                                policy = account_policies[account_ids[acct_ind]][
                                    datatype.value["POL_LOB"]
                                ].pop()

                                obj[datatype.value["json_key"]][policy_ind][
                                    "POL_NBR"
                                ] = policy["POL_NBR"]

                                obj[datatype.value["json_key"]][policy_ind][
                                    "ACCT_NBR"
                                ] = account_ids[acct_ind]

                                obj[datatype.value["json_key"]][policy_ind][
                                    "TERM_EFF_DT"
                                ] = policy["TERM_EFF_DT"]

                                obj[datatype.value["json_key"]][policy_ind][
                                    "TERM_EXP_DT"
                                ] = policy["TERM_EXP_DT"]

            json_obj = json.dumps(obj)

            arr = in_file_path.split("/")
            file_name = arr[len(arr) - 1]

            file_path = f"{out_folder}/{file_name}"

            with open(file_path, "w") as f:
                f.write(json_obj)

            encrypted_file_path = Utils._encrypt_local_file(
                file_path=file_path, output_folder_path=encrypted
            )

            with open(encrypted_file_path, "r") as f:
                encrypt_data = f.read()

            new_blob = ts.bucket.blob(
                f"{rand_unix_time}{file_delimeter}{datatype.value['file_name']}"
            )

            with new_blob.open("w") as f:
                f.write(encrypt_data)

    for file in root_files:
        blob = ts.bucket.blob(f"{file}.txt")

        logger.info(f"writing file {blob.name}")
        with blob.open("w") as f:
            f.write(" ")

    # os.system(f"rm -rf {out_folder}")
    # os.system(f"rm -rf {encrypted}")
    return False


# TODO: try create 3 files on init
# ingested
