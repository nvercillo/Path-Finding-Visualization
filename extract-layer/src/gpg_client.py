import os
import gnupg
from src.structs import ErrorMessage
import gnupg
from pprint import pprint


class GPGClient:
    def __init__(self, gpg_pub_key_path, gpg_priv_key_path, passphrase):

        user = os.path.abspath(__file__).split("/")[2]
        gpg_path = f"/home/{user}/.gnupg"
        self.gpg = gnupg.GPG(gnupghome=gpg_path)
        self.passphrase = passphrase

        with open(gpg_pub_key_path, "r") as f:
            self.pub_key_data = f.read()

        with open(gpg_priv_key_path, "r") as f:
            self.priv_key_data = f.read()

        self._import_keys()

    def _import_keys(self):
        import_result_pub = self.gpg.import_keys(self.pub_key_data)
        import_result_priv = self.gpg.import_keys(
            self.priv_key_data, passphrase=self.passphrase
        )

        self.gpg.trust_keys(import_result_pub.fingerprints, "TRUST_ULTIMATE")
        self.gpg.trust_keys(import_result_priv.fingerprints, "TRUST_ULTIMATE")

    def _get_unencrypted_data(self, enc_data):
        decrypted_data = self.gpg.decrypt(enc_data, passphrase=self.passphrase)

        if not decrypted_data.ok:
            return ErrorMessage(
                f"Failed to decrypt data for the reason: {decrypted_data.stderr}"
            )

        return decrypted_data


# GPGClient(
#     "/home/stefan/data-pipeline/environment/cna/extract-layer/gpg/priv_key.asc",
#     "/home/stefan/data-pipeline/environment/cna/extract-layer/gpg/priv_key.asc",
#     "passphrase",
# )._import_keys()
