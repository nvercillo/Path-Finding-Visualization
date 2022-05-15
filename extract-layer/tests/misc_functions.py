# REQ: In order to run this test, pytest is pip library needs to be installed
import copy
from ast import expr
import json
import os
import time
from google.cloud import storage  # Imports the Google Cloud client library
from datetime import datetime, timedelta, date
import sys
import random

sys.path.insert(0, "../")  # import parent folder

from src.utils import Utils
from src.__init__ import logger

from src.structs import DataType
from library.faker_factory import RandomizationUtils

# misc function
def cleanse_folder(ts):  # ts: TestStruct

    logger.info("Cleansing testing folder")
    blobs = ts.client.list_blobs(ts.bucket_name)

    for blob in blobs:

        if (
            blob.name[-1] != "/"
            and not blob.name.startswith("locks")
            and not blob.name.startswith("ingested")
            and not blob.name.startswith("rejected")
        ):  # filter out unneeded:
            blob.delete()


# misc function
def sanitize_all_json(in_folder, out_folder=None):
    if out_folder is None:
        out_folder = in_folder

    for datatype in DataType:
        # if datatype == DataType.account:
        if datatype != DataType.null:
            file_path = f'{in_folder}/{datatype.value["file_name"]}'

            RandomizationUtils().sanitize_json(file_path, out_folder)


# sanitize_all_json("../models/sanitized", "../models/sanitized")


class Sanitize:
    def sanitize_dict(self, d):
        for k in d:
            if "NAME" in k and isinstance(d[k], list):
                d[k] = d[k][0]

            if isinstance(d[k], dict):
                self.sanitize_dict(d[k])

            elif isinstance(d[k], list):
                self.sanitize_arr(d)

    def sanitize_obj(self, obj):
        if isinstance(obj, dict):
            self.sanitize_dict(obj)
        elif isinstance(obj, list):
            self.sanitize_arr(obj)

    def sanitize_arr(self, arr):
        for ele in arr:
            if isinstance(ele, dict):
                self.sanitize_dict(ele)
            elif isinstance(ele, list):
                self.sanitize_arr(ele)


def remove_arrays(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)

    Sanitize().sanitize_obj(data)

    with open(file_path + "new", "w") as f:
        json.dump(data, f)


# remove_arrays(
#     "/home/stefan/stefan/federato/data-pipeline/environment/cna/extract-layer/models/sanitized/gl.json"
# )


# # misc function
def scale_json(in_folder_path, datatype: DataType, out_folder_path, scale):

    file_path = f"{in_folder_path}/{datatype.value['file_name']}"
    logger.info(f"Scaling json file {file_path}")

    with open(file_path, "r") as f:
        obj = json.load(f)

    key = [k for k in obj.keys()][0]

    count = 0
    for i in range(scale):
        object = copy.deepcopy(obj[key][0])
        if datatype.name == DataType.account.name:  # scale policies as well
            num_each_category = scale // 3

            today = date.today()
            dt_format = "%Y-%m-%d %H:%M:%S"
            td_dt = datetime.strptime(today.strftime(dt_format), dt_format)

            # print(td_dt)
            # delta = timedelta(random.randint(-150, 150))

            for j in range(scale // 10):
                policy = copy.deepcopy(object["POLICY"][0])
                count += 1
                if j // num_each_category == 0:  # both dates in past
                    # print("A")
                    delta1 = timedelta(random.randint(-550, -350))
                    delta2 = timedelta(random.randint(-150, -50))
                elif j // num_each_category == 1:  # exp in future, eff in past
                    # print("B")
                    delta1 = timedelta(random.randint(-350, -150))
                    delta2 = timedelta(random.randint(150, 350))
                elif j // num_each_category >= 2:  # both in future
                    # print("C")
                    delta1 = timedelta(random.randint(50, 100))
                    delta2 = timedelta(random.randint(350, 550))

                if delta1 > delta2:
                    temp = delta1
                    delta1 = delta2
                    delta2 = temp

                # print(j // num_each_category, delta1, delta2)

                eff_dt = td_dt + delta1
                exp_dt = td_dt + delta2

                eff_date = eff_dt.strftime(dt_format)
                exp_date = exp_dt.strftime(dt_format)

                policy["TERM_EFF_DT"] = eff_date
                policy["TERM_EXP_DT"] = exp_date
                object["POLICY"].append(policy)

        obj[key].append(object)

    out_file_path = f"{out_folder_path}/{datatype.value['file_name']}"
    with open(out_file_path, "w") as f:
        json.dump(obj, f)


def scale_all_json(in_folder, out_folder, scale):

    for datatype in DataType:
        if datatype == DataType.null:
            continue

        scale_json(in_folder, datatype, out_folder, scale)
        # break


# scale_all_json("../models/sanitized", "../models/scaled", scale=20)


folder = "../models/real/1654059600"
out_folder = "./models/outfolder"
sanitized_folder = "../models/real/1654059600"


def sanitize_files(encrypted=True):
    print("Sanitizing files")
    for datatype in DataType:

        if datatype == DataType.null:
            continue

        # if datatype != DataType.gl:
        #     continue

        in_file_path = f'{folder}/{datatype.value["file_name"]}'
        gpg_key_path = f'/home/{os.path.abspath(__file__).split("/")[2]}/.gnupg'

        if encrypted:
            Utils._decrypt_local_file(
                in_file_path,
                out_folder,
                gpg_key_path=os.getenv("GPG_KEY_PATH", gpg_key_path),
                passphrase=os.getenv("GPG_PASSPHRASE", "password"),
            )

            RandomizationUtils().sanitize_json(
                in_file_path=out_folder,
                out_folder_path=sanitized_folder,
                return_dict=False,
                sanitize=True,
            )
        else:
            RandomizationUtils().sanitize_json(
                in_file_path=in_file_path,
                out_folder_path=sanitized_folder,
                return_dict=False,
                sanitize=True,
            )


# sanitize_files(encrypted=False)
