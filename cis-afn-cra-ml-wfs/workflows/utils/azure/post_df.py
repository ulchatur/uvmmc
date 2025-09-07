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
            account_url=f"https://{SERVER_NAME}.blob.core.windows.net", credential=credential
        )
       
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_final)

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
            df.to_parquet(index=False),
            overwrite=True,
            blob_type="BlockBlob",
            connection_timeout=3600,
        )
    elif file_extension_azure == "csv" or file_extension_azure == "CSV":
        blob_client.upload_blob(
            dataframe.to_csv(index=False),
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