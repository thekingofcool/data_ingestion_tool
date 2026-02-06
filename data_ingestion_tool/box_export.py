import logging
import pandas as pd
from boxsdk import Client, JWTAuth
from cerberus.client import CerberusClient
import os
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _log_task(catalog_name, schema_name, owner, spark, action, file_info):
    spark.sql(
        f"""INSERT INTO {catalog_name}.{schema_name}.box_ingestion_log 
        (task_owner, job_action, log_info, process_dt) VALUES 
        ('{owner}', '{action}', '{file_info}', from_utc_timestamp(current_timestamp(), 'Asia/Shanghai'))"""
    )
    logging.info(f"Owner: {owner}, Action: {action}, File Info: {file_info}")
    print(f"Owner: {owner}, Action: {action}, File Info: {file_info}")

def _initialize_box_client():
    cerberus_client = CerberusClient('https://prod.cerberus.nikecloud.com')
    auth = JWTAuth(
        client_id=cerberus_client.get_secrets_data("app/eda-gc-box/jwt_auth")["client_id"],
        client_secret=cerberus_client.get_secrets_data("app/eda-gc-box/jwt_auth")["client_secret"],
        enterprise_id=cerberus_client.get_secrets_data("app/eda-gc-box/jwt_auth")["enterprise_id"],
        jwt_key_id=cerberus_client.get_secrets_data("app/eda-gc-box/jwt_auth")["jwt_key_id"],
        rsa_private_key_data=(
            (cerberus_client.get_secrets_data("app/eda-gc-box/jwt_auth")["pemkey"]).replace('\\n', '\n')),
        rsa_private_key_passphrase=bytes((cerberus_client.get_secrets_data(
            "app/eda-gc-box/jwt_auth")["rsa_private_key_passphrase"]).encode('utf-8'))
    )
    box_client = Client(auth)
    return box_client

def _export_data_to_excel(catalog_name, schema_name, owner, spark, query, file_path, sheet_name=None, column_mapping=None):
    try:
        df = spark.sql(query)
        pdf = df.toPandas()
        if column_mapping:
            lower_col_map = {k.lower(): v for k, v in column_mapping.items()}
            pdf.columns = [col.lower() for col in pdf.columns]
            pdf = pdf.rename(columns=lower_col_map)
        if sheet_name:
            with pd.ExcelWriter(file_path) as writer:
                pdf.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            pdf.to_excel(file_path, index=False)
    except Exception as e:
        _log_task(catalog_name, schema_name, owner, spark, "Export to excel failed", f"{str(e)}")

def _upload_file_to_box(catalog_name, schema_name, owner, spark, client, folder_id, local_file_path, remote_file_name):
    try:
        folder = client.folder(folder_id)
        file_id = None
        for item in folder.get_items(limit=1000):
            if item.type == 'file' and item.name == remote_file_name:
                file_id = item.id
                break
        if file_id:
            file_obj = client.file(file_id)
            uploaded_file = file_obj.update_contents(local_file_path)
        else:
            uploaded_file = folder.upload(local_file_path, file_name=remote_file_name)
        return uploaded_file
    except Exception as e:
        _log_task(catalog_name, schema_name, owner, spark, "Upload to box failed", f"{str(e)}")
        raise
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

def execute_export(owner, spark, query, folder_id, file_name, sheet_name=None, column_mapping=None):
    client = _initialize_box_client()
    catalog_name = 'development' # dev
    schema_name = 'eda_gc_raw'

    temp_dir = os.environ.get("TEMP", "/tmp")
    unique_id = uuid.uuid4().hex
    temp_file_path = os.path.join(temp_dir, f"{unique_id}.xlsx")

    _export_data_to_excel(catalog_name, schema_name, owner, spark, query, temp_file_path, sheet_name, column_mapping)
    _upload_file_to_box(catalog_name, schema_name, owner, spark, client, folder_id, temp_file_path, f"{file_name}.xlsx")
    _log_task(catalog_name, schema_name, owner, spark, "Export to Box Success", f"Uploaded {file_name}.xlsx to Box folder {folder_id}")
