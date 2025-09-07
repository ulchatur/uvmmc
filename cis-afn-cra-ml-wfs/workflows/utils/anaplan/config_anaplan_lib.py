import os
from dotenv import load_dotenv

load_dotenv(dotenv_path ='/Workspace/Shared/.env')

HOME_PATH = os.environ["HOME_PATH"]
path_cert = f"{HOME_PATH}/certificates/Sectigo.cert.pem"
path_private_key = f"{HOME_PATH}/certificates/Sectigo.privatekey.key"
path_bin = f"{HOME_PATH}/certificates/2.bin"
path_key = f"{HOME_PATH}/certificates/1.key"
container_name_creds = os.environ["CONTAINER_NAME_CREDS"]