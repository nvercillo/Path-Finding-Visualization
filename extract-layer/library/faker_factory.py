# This class is intended to scrub potentally compromising values from any json file

# EXAMPLE USAGE:
# RandomizationUtils().sanitize_json(
#     "/home/stefan/stefan/federato/data-pipeline/environment/cna/extract-layer/models/wc.json"
# )

# A file wc_santized.json is then created in the dir that this file was run from


import json
import random
from datetime import datetime, timedelta

digits = [i for i in range(1, 10)]


with open("../models/address/unique_addresses.json") as f:
    unique_addresses = json.load(f)["addresses"]


# TODO: DASHBOARD_DISCRETIONARY_PRICE_CHANGE in auto
# DASHBOARD_RATE_CHANGE


class RandomizationUtils:
    def sanitize_json(
        self,
        in_file_path,
        out_folder_path=None,
        return_dict=False,
        sanitize=True,
    ):
        with open(in_file_path, "r") as f:
            data = json.load(f)
            if sanitize:
                self.sanitize_obj(data)

        if return_dict:
            return data

        json_obj = json.dumps(data, indent=4)

        arr = in_file_path.split("/")
        file_name = arr[len(arr) - 1]

        file_path = f"{out_folder_path}/{file_name}"
        with open(file_path, "w") as f:
            f.write(json_obj)

        return file_path

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

    def sanitize_dict(self, d):
        for k in d:

            if k in ["ACCT_NBR", "POL_NBR", "TERM_EFF_DT", "TERM_EXP_DT"]:
                continue

            if isinstance(d[k], dict):
                self.sanitize_dict(d[k])
            elif isinstance(d[k], list):
                self.sanitize_arr(d[k])
            elif isinstance(d[k], str):
                if d[k].isnumeric():
                    string = "".join(
                        str(random.choice(digits)) for _ in range(len(d[k]))
                    )

                    # TODO change to string??? depends on spec
                    # d[k] = int(string)

                    d[k] = string

                else:
                    if d[k] == "null":
                        d[k] = None
                    elif (
                        len(d[k]) >= 3
                        and d[k].count(".") == 1
                        and d[k].index(".") != 0
                        and d[k].index(".") != len(d[k]) - 1
                        and d[k][: d[k].index(".")].isnumeric()
                        and d[k][d[k].index(".") + 1 :].isnumeric()
                    ):

                        # check if float
                        prefix = "".join(
                            str(random.choice(digits))
                            for _ in range(len(d[k][: d[k].index(".")]))
                        )

                        suffix = "".join(
                            str(random.choice(digits))
                            for _ in range(len(d[k][d[k].index(".") + 1 :]))
                        )

                        # TODO change to string??? depends on spec
                        d[k] = str(float(f"{prefix}.{suffix}"))

                    else:
                        # check if valid datetime
                        dt_format = "%Y-%m-%d %H:%M:%S"  # suspected datetime dt_format

                        # using try-except to check for truth value
                        try:
                            res = bool(datetime.strptime(d[k], dt_format))
                        except ValueError:
                            res = False

                        if res:  # valid datetime
                            dt = datetime.strptime(d[k], dt_format)

                            delta = timedelta(random.randint(-150, 150))

                            dt_new = dt - delta

                            d[k] = dt_new.strftime(dt_format)

                        else:

                            if len(d[k]) > 1 and d[k][0] == "-":
                                if "." in d[k]:
                                    prefix = "".join(
                                        str(random.choice(digits))
                                        for _ in range(len(d[k][: d[k].index(".")]))
                                    )

                                    suffix = "".join(
                                        str(random.choice(digits))
                                        for _ in range(len(d[k][d[k].index(".") + 1 :]))
                                    )

                                    d[k] = f"-{prefix}.{suffix}"
                                else:
                                    string = "".join(
                                        str(random.choice(digits))
                                        for _ in range(len(d[k]))
                                    )

                                    string += f"-{string}"

                                continue

                            sensitive_map = {
                                "AGENCY_NAME": "Agency "
                                + "".join(str(random.choice(digits)) for _ in range(4)),
                                "NAMED_INSURED_NM": "Company "
                                + "".join(str(random.choice(digits)) for _ in range(4)),
                                "CARRIER": "Carrier "
                                + "".join(str(random.choice(digits)) for _ in range(4)),
                            }

                            rand_addr = random.choice(unique_addresses)
                            while "city" not in rand_addr:
                                rand_addr = random.choice(unique_addresses)

                            if k in sensitive_map:
                                d[k] = sensitive_map[k]

                            elif "DESCRIPTION" in k:
                                d[k] = "Long description... "

                            elif "NAME" in k:
                                d[k] = "Name " + "".join(
                                    str(random.choice(digits)) for _ in range(4)
                                )
                            elif "CODE" in k:
                                d[k] = "".join("X" for _ in range(len(d[k])))

                            elif "STATE" in k or k.endswith("_ST"):
                                d[k] = rand_addr["state"]

                            elif "ADDRESS" in k:
                                d[k] = rand_addr["address1"]

                            elif "GEO_STREET" in k:
                                d[k] = None

                            elif "CITY" in k:
                                print("rand_addr", rand_addr)
                                d[k] = rand_addr["city"]
                            pass
                            # # TODO: build this out
                            # # treat like regular string
                            # d[k] = "".join("X" for _ in range(len(d[k])))

            elif isinstance(d[k], float):
                d[k] = str(d[k])

                prefix = "".join(
                    str(random.choice(digits))
                    for _ in range(len(d[k][: d[k].index(".")]))
                )

                suffix = "".join(
                    str(random.choice(digits))
                    for _ in range(len(d[k][d[k].index(".") + 1 :]))
                )

                d[k] = f"{prefix}.{suffix}"

            elif isinstance(d[k], int):
                d[k] = str(d[k])

                string = "".join(
                    str(random.choice(digits)) for _ in range(len(str(d[k])))
                )

                d[k] = string
