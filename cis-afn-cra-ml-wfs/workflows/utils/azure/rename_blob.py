import os
import time
from azure.storage.blob import BlobServiceClient

connection_string = os.environ['OLD_AZURE_CONNECTION_STRING']

def rename_azure_blob(container_name, old_blob_name, new_blob_name, old_file_extension, new_file_extension):
    try:
        old_blob_name_final = old_blob_name + "." + old_file_extension
        new_blob_name_final = new_blob_name + "." + new_file_extension

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # Copy the blob to a new name
        source_blob = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{old_blob_name_final}"
        destination_blob_client = container_client.get_blob_client(new_blob_name_final)

        # Start copy operation
        copy_props = destination_blob_client.start_copy_from_url(source_blob)

        # Optionally, wait for the copy to complete
        while True:
            props = destination_blob_client.get_blob_properties()
            if props.copy.status == 'success':
                break
            elif props.copy.status == 'pending':
                time.sleep(1)
            else:
                raise Exception(f"Copy failed with status: {props.copy.status}")

        # Delete the original blob
        container_client.delete_blob(oold_blob_name_final)

        print(f"Blob renamed from {oold_blob_name_final} to {new_blob_name_final}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        raise e
