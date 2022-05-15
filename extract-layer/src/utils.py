import os
import gnupg
from src.__init__ import logger


class Utils:
    @staticmethod
    def _encrypt_local_file(file_path, output_folder_path, gpg_key_path=None):
        # THIS FILES REQUIRES YOU TO HAVE THIS FILE IN A HIGHER THAN ROOT DIRECTORY
        # AND TO HAVE A GPG KEY ALREADY CREATED W USER "test@test.com"

        logger.info(f"Encrypting file: {file_path}")
        if gpg_key_path is None:
            user = os.path.abspath(__file__).split("/")[2]
            gpg_key_path = f"/home/{user}/.gnupg"

        gpg = gnupg.GPG(gnupghome=gpg_key_path)

        with open("../gpg/pub_key.asc", "r") as f:
            pub_key_data = f.read()

        with open("../gpg/pub_key.asc", "r") as f:
            priv_key_data = f.read()

        import_result_pub = gpg.import_keys(pub_key_data)
        import_result_priv = gpg.import_keys(
            priv_key_data,
            passphrase="passphrase",
        )

        gpg.trust_keys(import_result_pub.fingerprints, "TRUST_ULTIMATE")
        gpg.trust_keys(import_result_priv.fingerprints, "TRUST_ULTIMATE")

        if "/" in file_path:
            file_name = file_path.split("/")[-1]
        else:
            file_name = file_path

        output_file_path = f"{output_folder_path}/{file_name}.pgp"

        with open(file_path, "rb") as f:
            status = gpg.encrypt_file(
                f,
                recipients=["harry@uwaterloo.ca"],
                output=output_file_path,
            )
        return output_file_path

    @staticmethod
    def _decrypt_local_file(
        file_path, output_folder_path, gpg_key_path=None, passphrase="password"
    ):

        gpg = gnupg.GPG(gnupghome=gpg_key_path)

        logger.info(f"Decrypting file: {file_path}")

        if gpg_key_path is None:
            user = os.path.abspath(__file__).split("/")[2]
            gpg_key_path = f"/home/{user}/.gnupg"

        if "/" in file_path:
            file_name = file_path.split("/")[:-1]
        else:
            file_name = file_path

        assert file_name.endswith(".pgp"), "Got non-gpg encrypted file"

        file_name = file_name[: file_name.index(".pgp")]  # remove gpg extension

        output_file_path = f"{output_folder_path}/{file_name}"
        with open(file_path, "rb") as f:
            status = gpg.decrypt_file(f, passphrase=passphrase, output=output_file_path)
            logger.info(f"Decrypted data succeeded: {status.ok}")


# Utils._encrypt_local_file("test.py", "./")
# Utils._decrypt_local_file("test.py.gpg", "./")
