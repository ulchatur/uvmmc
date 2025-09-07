import json
import os
import tempfile
import time
import pandas as pd
import requests
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from dotenv import load_dotenv

spark = SparkSession.builder \
    .appName("CRA_Renewal") \
    .getOrCreate()
dbutils = DBUtils(spark)

SERVER_NAME = 'csvnuset2saadxuc01'
scope = f"{SERVER_NAME}-scope"
client_id = dbutils.secrets.get(scope=scope, key="storage-blob-client-id")
client_secret = dbutils.secrets.get(scope=scope, key="storage-blob-client-secret")
tenant_id = dbutils.secrets.get(scope=scope, key="storage-blob-tenant-id")

spark.conf.set("fs.azure.account.auth.type.csvnuset2saadxuc01.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.csvnuset2saadxuc01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.csvnuset2saadxuc01.dfs.core.windows.net", f"{client_id}")
spark.conf.set("fs.azure.account.oauth2.client.secret.csvnuset2saadxuc01.dfs.core.windows.net", f"{client_secret}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.csvnuset2saadxuc01.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

def get_df(
    container_name: str,
    blob_name: str,
    file_extension: str,
    delimiter: str = ""

) -> pd.DataFrame:
    """
   Gets data from single Azure Blob as pandas df

    Args:
        container_name (str): The name of the container in Azure Blob.
        blob_name (str): The name of the blob in Azure Blob.
        file_extension (str): The extension of the file.
    Returns:
        pd.DataFrame: A Pandas DataFrame containing data from the Azure Blob.
    """
    try:
        blob_name_final = blob_name + "." + file_extension
        file_path = f'abfss://data@csvnuset2saadxuc01.dfs.core.windows.net/{container_name}/{blob_name_final}'
        if file_extension == 'csv':
            df = spark.read.csv(file_path, header=True)
        if file_extension == 'parquet':
            df = spark.read.parquet(file_path, header=True)
        if file_extension =='txt':
           df = spark.read.format("csv") \
            .option("delimiter", "\t") \
            .option("header", "true") \
            .option("skipRows", 1) \
            .load(file_path)
        pandas_df = df.toPandas()
        return pandas_df
    except Exception as e:
        print(f"Failed to fetch {blob_name}.{file_extension}")
        print(e)
        raise Exception("Failed to get data!")

