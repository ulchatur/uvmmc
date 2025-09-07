# ==============================================================================
# Created:		28 June 2018
# @author:		Jesse Wilson  (Anaplan Asia Pte Ltd)
# Description:	This script reads a user's public and private keys in order to
# 				sign a cryptographic nonce. It then generates a request to
# 				authenticate with Anaplan, passing the signed and unsigned
# 				nonce in the body of the request.
# Input:		Public certificate file location, private key file location
# Output:		Authorization header string, request body string containing a nonce and its signed value
# ==============================================================================
import logging
import os
from base64 import b64encode
from typing import Dict

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .AnaplanAuthentication import AnaplanAuthentication

logger = logging.getLogger(__name__)


class CertificateAuthentication(AnaplanAuthentication):
    """
    Represents a certificate authentication request
    """

    # ===========================================================================
    # This function reads a user's public certificate as a string, base64
    # encodes that value, then returns the certificate authorization header.
    # ===========================================================================
    def auth_header(self, pub_cert: str) -> Dict[str, str]:
        """Create the Auth API request header

        :param pub_cert: Path to public certificate or public certificate as a string
        :type pub_cert: str
        :return: Auth-API request authorization header
        :rtype: dict
        """

        if not os.path.isfile(pub_cert):
            my_pem_text = pub_cert
        else:
            with open(pub_cert, "r") as my_pem_file:
                my_pem_text = my_pem_file.read()

        header_string = {
            "Authorization": "".join(
                [
                    "CACertificate ",
                    b64encode(my_pem_text.encode("utf-8")).decode("utf-8"),
                ]
            )
        }

        return header_string

    # ===========================================================================
    # This function takes a private key, calls the function to generate the nonce,
    # then the function to sign the nonce, and finally returns the Anaplan authentication
    # POST body value
    # ===========================================================================
    @staticmethod
    # def generate_post_data(priv_key: bytes) -> str:
    def generate_post_data(
        priv_key: bytes,
        TxtPassword: str = "",
        TxtFilePathBin: str = "",
        TxtFilePathkey: str = "",
    ) -> str:
        """Create the body of the Auth-API request

        :param priv_key: Private key text or path to key
        :type priv_key: bytes
        """

        unsigned_nonce = CertificateAuthentication.create_nonce()
        # signed_nonce = str(CertificateAuthentication.sign_string(unsigned_nonce, priv_key))
        signed_nonce = str(
            CertificateAuthentication.sign_string(unsigned_nonce, priv_key, TxtPassword, TxtFilePathBin, TxtFilePathkey)
        )

        json_string = "".join(
            [
                '{ "encodedData":"',
                str(b64encode(unsigned_nonce).decode("utf-8")),
                '", "encodedSignedData":"',
                signed_nonce,
                '"}',
            ]
        )

        return json_string

    # ===========================================================================
    # The function generates a pseudo-random alpha-numeric 150 character nonce
    # and returns the value
    # ===========================================================================
    @staticmethod
    def create_nonce() -> bytes:
        """Create a random 150-character byte array

        :return: Bytes object containing 150 characters
        :rtype: bytes
        """
        rand_arr = os.urandom(150)

        return rand_arr

    # ===========================================================================
    # This function reads a pseudo-randomly generated nonce and signs the text
    # with the private key.
    # ===========================================================================
    @staticmethod
    def sign_string(
        message: bytes,
        priv_key: bytes,
        TxtPassword: str = "",
        TxtFilePathBin: str = "",
        TxtFilePathkey: str = "",
    ) -> str:
        # def sign_string(message: bytes, priv_key: bytes) -> str:
        """Signs a string with a private key

        :param message: 150-character pseudo-random byte-array of characters
        :type message: bytes
        :param priv_key: Private key text, used to sign the message.
        :type priv_key: bytes
        :param TxtPassword: If the key is password protected and you have a string to encrypt it
        :type TxtPassword: string
        To create the Bin and key
        from cryptography.fernet import Fernet
        # Generate a key (do this only once and store it securely)
        key = Fernet.generate_key()
        # Save this key securely; you'll need it for decryption
        with open('C:/.../1.key', 'wb') as key_file:
                key_file.write(key)
        # Create a cipher instance
        cipher_suite = Fernet(key)
        # Your password to encrypt
        password = "your_password"
        # Encrypt the password
        encrypted_password = cipher_suite.encrypt(password.encode())
        # Write the encrypted password to a file
        with open('C:/.../2.bin', 'wb') as file_out:
                file_out.write(encrypted_password)
        # Load the key
        with open('C:/.../1.key', 'rb') as key_file:
                key = key_file.read()
        # Create a cipher instance
        cipher_suite = Fernet(key)
        # Read the encrypted password
        with open('C:/.../2.bin', 'rb') as file_in:
                encrypted_password = file_in.read()
        # Decrypt the password
        decrypted_password = cipher_suite.decrypt(encrypted_password).decode()
        # Use the decrypted password as needed
        :param TxtFilePathBin: If the key is password protected and you have the files with the encrypted one
        :type TxtPassword: string to .bin path
        :param TxtFilePathkey: If the key is password protected and you have the files with the encrypted one
        :type TxtPassword: string to .key path
        :raises ValueError: Error loading the private or signing the message.
        :return: Base64 encoded signed string value
        :rtype: str
        """
        backend = default_backend()
        if TxtPassword != "":  # 1. if TxtPassword!=""
            TxtDecodedPass = TxtPassword.encode()
        elif TxtFilePathBin != "" and TxtFilePathkey != "":  # 1. if TxtPassword!=""
            try:
                with open(TxtFilePathkey, "rb") as FileKey:
                    Objkey = FileKey.read()
                cipher_suite = Fernet(Objkey)
                with open(TxtFilePathBin, "rb") as file_in:
                    Objbin = file_in.read()
                TxtDecodedPass = cipher_suite.decrypt(Objbin).decode().encode()
            except Exception as TxtException:
                logger.error(f"Error loading private key {TxtException}", exc_info=True)
                raise ValueError(f"Error loading private key {TxtException}")
        else:  # 1. if TxtPassword!=""
            TxtDecodedPass = None
        try:
            if not os.path.isfile(priv_key):
                key = serialization.load_pem_private_key(priv_key, TxtDecodedPass, backend=backend)
            else:
                with open(priv_key, "r") as key_file:
                    key = serialization.load_pem_private_key(
                        open(priv_key, "r").read().encode("utf-8"),
                        TxtDecodedPass,
                        backend=backend,
                    )
            try:
                signature = key.sign(message, padding.PKCS1v15(), hashes.SHA512())
                return b64encode(signature).decode("utf-8")
            except ValueError as e:
                logger.error(f"Error signing message {e}", exc_info=True)
                raise ValueError(f"Error signing message {e}")
        except ValueError as e:
            logger.error(f"Error loading private key {e}", exc_info=True)
            raise ValueError(f"Error loading private key {e}")
