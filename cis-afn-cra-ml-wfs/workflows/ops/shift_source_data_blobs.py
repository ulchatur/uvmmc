import os
from utils.azure.rename_blob import rename_azure_blob

deployment_mode = os.environ['DEPLOYMENT_MODE'].lower()

def shift_source_data_blobs():
    try:
        for year in [3,2,1,0]:
            # rename/shift hub data
            renamed_blob = rename_azure_blob(
                container_name=f'data/AFN_ML/{deployment_mode}/cra/raw_data/hub',
                old_blob_name=f'year_{year}',
                old_file_extension='parquet',
                new_blob_name=f'year_{year+1}',
                new_file_extension='parquet'
            )
            print(f'Renamed Hub data Azure blob from year_{year} to year_{year+1}')
            # rename/shift spoke data
            renamed_blob = rename_azure_blob(
                container_name=f'data/AFN_ML/{deployment_mode}/cra/raw_data/spoke',
                old_blob_name=f'year_{year}',
                old_file_extension='txt',
                new_blob_name=f'year_{year+1}',
                new_file_extension='txt'
            )
            print(f'Renamed Hub data Azure blob from year_{year} to year_{year-1}')
        return True
    
    except Exception as e:
        print(e)
        raise e