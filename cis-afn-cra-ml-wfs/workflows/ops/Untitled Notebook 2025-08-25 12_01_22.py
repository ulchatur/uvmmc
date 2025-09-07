# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pandas as pd# from dotenv import load_dotenv


spark = SparkSession.builder \
    .appName("MMA_Rew") \
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

        # blob connection
        blob_name_final = blob_name + "." + file_extension
        file_path = f'abfss://data@csvnuset2saadxuc01.dfs.core.windows.net/{container_name}/{blob_name_final}'
        if file_extension == 'csv':
            df = spark.read.csv(file_path, header=True)
        if file_extension == 'parquet':
            df = spark.read.parquet(file_path)
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


# COMMAND ----------

import json
import os
import time
import pandas as pd
import numpy as np
import requests
from azure.storage.blob import BlobClient, BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import logging
from datetime import datetime
from azure.identity import ClientSecretCredential

spark = SparkSession.builder \
    .appName("MMA_Rew") \
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


storage_account_name_new = "csvnuset2saadxuc01"

def post_df(
    container_name: str,
    blob_name: str,
    file_extension_azure: str,
    dataframe: pd.DataFrame,
    dtypes_dict: str | None = None,
) -> dict:

    try:
        # set df orient
        dataframe_orient='tight'
        # blob connection
        blob_name_final = blob_name + "." + file_extension_azure

        # Build credential using OAuth.
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        # Create a BlobServiceClient using the credential
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name_new}.blob.core.windows.net", credential=credential
        )
       
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_final)
        print(blob_client)
        # dtypes
        if dtypes_dict is None:
            dtypes_dict = {}

        df_json = dataframe.to_dict(orient=dataframe_orient)
        df_json = json.dumps(df_json)
        data_dict = json.loads(df_json)
        df = pd.DataFrame.from_dict(data_dict, orient=dataframe_orient)

        # post dataframe case
        post_dataframe_case(
            dataframe=df,
            blob_client=blob_client,
            file_extension_azure=file_extension_azure,
            dtypes_dict=dtypes_dict,
        )


        time.sleep(3)

        time.sleep(2)
    except Exception as e:


        raise e


def post_dataframe_case(
    dataframe: pd.DataFrame,
    blob_client: BlobClient,
    file_extension_azure: str,
    dtypes_dict=dict,
):
    """
    Completes post operation based on file extension
    """
    if file_extension_azure == "parquet":
        df = fix_dytpes(dataframe=dataframe, dtypes_dict=dtypes_dict)
        blob_client.upload_blob(
            df.to_parquet(),
            overwrite=True,
            blob_type="BlockBlob",
            connection_timeout=3600,
        )
    elif file_extension_azure == "csv" or file_extension_azure == "CSV":
        blob_client.upload_blob(
            dataframe.to_csv(),
            overwrite=True,
            blob_type="BlockBlob",
            connection_timeout=3600,
        )
    elif file_extension_azure == "txt":
        blob_client.upload_blob(
            dataframe.to_string(),
            overwrite=True,
            blob_type="BlockBlob",
            connection_timeout=3600,
        )




def fix_dytpes(dataframe: pd.DataFrame, dtypes_dict: dict):
    """
    Converts dataframe columns to specified dtypes based on dtypes_dict
    """
    df = dataframe.copy()

    features_str = []
    features_float = []
    features_int = []
    features_bool = []
    features_datetime = []

    for key, value in dtypes_dict.items():
        if value == "int":
            features_int.append(key)
        elif value == "float":
            features_float.append(key)
        elif value == "str":
            features_str.append(key)
        elif value == "bool":
            features_bool.append(key)
        elif value == "datetime":
            features_datetime.append(key)

    for i in features_str:
        df[i] = df[i].astype(str)
    for i in features_float:
        df[i] = pd.to_numeric(df[i], downcast="float", errors="coerce").astype("float64")
    for i in features_int:
        df[i] = pd.to_numeric(df[i], downcast="integer", errors="coerce").fillna(0).astype("int64")
    for i in features_bool:
        df = bool_dtype(dataframe=df, feature=i)
    for i in features_datetime:
        df[i] = pd.to_datetime(df[i].astype(str), errors="coerce")

    return df


def bool_dtype(dataframe: pd.DataFrame, feature: str):
    df = dataframe.copy()
    feat = df[feature].astype(str).str.lower()

    conditions = [feat == "true", feat == "false"]
    choices = [True, False]
    df[feature] = np.select(conditions, choices, default=False)
    return df

# COMMAND ----------


post_df(container_name='data/AFN_ML/dev/cra/regression_reports/latest',
            blob_name='test',
            file_extension_azure='csv',
            dataframe_orient='tight',
            dataframe=df)

# COMMAND ----------



# COMMAND ----------

df= get_df(container_name='AFN_ML/dev/cra/raw_data/hub', blob_name='year_0', file_extension='parquet')
df

# COMMAND ----------

df = df[0:100]
df