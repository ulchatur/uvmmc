import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import anaplan_api_updated

from utils.anaplan.config_anaplan_lib import (
    container_name_creds,
    path_bin,
    path_cert,
    path_key,
    path_private_key,
)

# Load credentials
# load_dotenv()

azure_blob_connection_string = os.environ["OLD_AZURE_CONNECTION_STRING"]

def cert_pull(container_name: str, local_folder: str):
    file_names = ["1.key", "2.bin", "Sectigo.cert.pem", "Sectigo.privatekey.key"]

    blob_service_client = BlobServiceClient.from_connection_string(azure_blob_connection_string)
    blob_client = blob_service_client.get_container_client(container=container_name)

    for file_name in file_names:
        try:
            blob_data = blob_client.download_blob(file_name)
            file_content = blob_data.content_as_bytes()
            local_file_path = os.path.join(local_folder, file_name)
            print(f"{file_name} was saved!")
            with open(local_file_path, "wb") as file:
                file.write(file_content)
            # AzurePullCustomLogData = createAzurePullCustomLogData(
            #     container_name,
            #     file_name,
            # )
            # streamlogger.SIEM(siemEvents.Sensitive_Operation, AzurePullCustomLogData)
        except Exception as e:
            # AzurePullCustomLogData.update({"Severity": "Error"})
            # AzurePullCustomLogData.update({"Message": f"Failure on Pulling {file_name} from Azure"})
            # streamlogger.SIEM(siemEvents.Sensitive_Operation, AzurePullCustomLogData)
            print(e)
            raise Exception(f"Failed to pull {file_name}")


def startup():
    cert_pull(container_name=container_name_creds, local_folder=Path("../certificates"))
