import logging
import re
import os
import shutil
import zipfile
from boxsdk import Client, JWTAuth
import pandas as pd
from cerberus.client import CerberusClient
from pyspark.sql.functions import current_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _log_task(catalog_name, schema_name, owner, spark, action, file_info):
    spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.box_ingestion_log (task_owner, job_action, log_info, process_dt) VALUES ('{owner}', '{action}', '{file_info}', current_timestamp())")
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

def _download_file(catalog_name, schema_name, owner, spark, client, table_name, folder_id, file_name_regex, latest):
    try:
        items = client.folder(folder_id).get_items()
        matched_files = [item for item in items if re.match(file_name_regex, item.name)]
        if not matched_files:
            _log_task(catalog_name, schema_name, owner, spark, "No files matched", f"Regex: {file_name_regex}")
            return None
        if latest:
            matched_files = sorted(
                matched_files,
                key=lambda x: x.get().created_at,
                reverse=True
            )
            matched_files = [matched_files[0]]
        temp_dir = f"/tmp/box/{table_name}"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        _log_task(catalog_name, schema_name, owner, spark, "Created temp directory", temp_dir)
        for file in matched_files:
            with open(temp_dir + '/' + file.name, 'wb') as file_open:
                client.file(file.id).download_to(file_open)
            _log_task(catalog_name, schema_name, owner, spark, "Downloaded file", f"{folder_id}/{file.name}")
        return [os.path.join(f"{temp_dir}", file.name) for file in matched_files]
    except Exception as e:
        _log_task(catalog_name, schema_name, owner, spark, "Download failed", str(e))
        return None

def _delete_file(catalog_name, schema_name, owner, spark, client, folder_id, file_name_regex, latest):
    try:
        items = client.folder(folder_id).get_items()
        matched_files = [item for item in items if re.match(file_name_regex, item.name)]
        if not matched_files:
            _log_task(catalog_name, schema_name, owner, spark, "No files matched", f"Regex: {file_name_regex}")
            return
        if latest:
            matched_files = sorted(
                matched_files,
                key=lambda x: x.get().created_at,
                reverse=True
            )
            matched_files = [matched_files[0]]
        for file in matched_files:
            client.file(file.id).delete()
            _log_task(catalog_name, schema_name, owner, spark, "Deleted file", f"{folder_id}/{file.name}")
    except Exception as e:
        _log_task(catalog_name, schema_name, owner, spark, "Delete failed", str(e))

def _validate_and_split_data(df, metadata, non_nullable_fields):
    valid_data = []
    invalid_data = []
    for _, row in df.iterrows():
        row = {k.lower(): v for k, v in row.items()}
        non_nullable_fields = [field.lower() for field in non_nullable_fields]
        errors = []
        converted_row = {}
        for col, col_type in metadata.items():
            if col == "non_nullable_fields":
                continue
            if col.lower() not in row:
                errors.append(f"Missing column: {col}")
            elif pd.isnull(row[col.lower()]):
                if col.lower() in non_nullable_fields:
                    errors.append(f"Null value in non-nullable column: {col}")
            else:
                try:
                    if col_type.lower() == "int":
                        converted_row[col.lower()] = int(row[col.lower()])
                    elif col_type.lower() == "float":
                        converted_row[col.lower()] = float(row[col.lower()])
                    elif col_type.lower() == "date":
                        converted_row[col.lower()] = pd.to_datetime(row[col.lower()]).date()
                    elif col_type.lower() == "timestamp":
                        converted_row[col.lower()] = pd.to_datetime(row[col.lower()], format='%Y-%m-%d %H:%M:%S')
                    else:
                        converted_row[col.lower()] = str(row[col.lower()])
                except ValueError:
                    errors.append(f"Type mismatch for column: {col}")
        if errors:
            invalid_data.append({
                "record": str(row),
                "error_type": "; ".join(errors)
            })
        else:
            valid_data.append(converted_row)
    return pd.DataFrame(valid_data), pd.DataFrame(invalid_data)

def _write_to_delta_table(spark, df, catalog_name, schema_name, table_name, error_table):
    delta_table = f"{catalog_name}.{schema_name}.{table_name}{'_error' if error_table else ''}"
    spark_df = spark.createDataFrame(df).withColumn("process_dt", current_timestamp())
    spark_df.write.format("delta").mode("append").saveAsTable(delta_table)
    logging.info(f"Data written to {'error ' if error_table else ''}Delta table: {delta_table}")
    spark_df.unpersist()

def _process_file(spark, owner, file_path, catalog_name, schema_name, table_name, metadata, just_copy, sheet_name):
    try:
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        if file_path.endswith('.zip'):
            temp_dir = f"/tmp/unzipped/{table_name}"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            _log_task(catalog_name, schema_name, owner, spark, "ZIP file extracted", f"{file_path} -> {temp_dir}")
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    extracted_file_path = os.path.join(root, file)
                    _process_file(spark, owner, extracted_file_path, catalog_name, schema_name, table_name, metadata, just_copy, sheet_name)
            return
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.json'):
            df = pd.read_json(file_path)
        elif file_path.endswith('.xlsx'):
            if sheet_name:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
            else:
                df = pd.read_excel(file_path)
        else:
            _log_task(catalog_name, schema_name, owner, spark, "Unsupported file type", f"{file_path} (Size: {file_size_mb:.2f} MB)")
            return
        df.columns = df.columns.str.strip()
        if just_copy:
            # Skip validation and directly write to Delta table
            from pyspark.sql.types import StructType
            delta_table = f"{catalog_name}.{schema_name}.{table_name}"
            target_df = spark.table(delta_table)
            spark_schema = StructType([field for field in target_df.schema if field.name != 'process_dt'])
            spark_df = spark.createDataFrame(df.astype(str), schema=spark_schema).withColumn("process_dt", current_timestamp())
            spark_df.write.format("delta").mode("append").saveAsTable(delta_table)
            _log_task(catalog_name, schema_name, owner, spark, "File copied directly to Delta table", f"{file_path} (Size: {file_size_mb:.2f} MB)")
            return
        valid_data, invalid_data = _validate_and_split_data(df, metadata, metadata.get('non_nullable_fields', []))
        if not valid_data.empty:
            _write_to_delta_table(spark, valid_data, catalog_name, schema_name, table_name, error_table=False)
        if not invalid_data.empty:
            _write_to_delta_table(spark, invalid_data, catalog_name, schema_name, table_name, error_table=True)
        _log_task(catalog_name, schema_name, owner, spark, "File processed", f"{file_path} (Valid: {len(valid_data)}, Invalid: {len(invalid_data)})")
    except Exception as e:
        _log_task(catalog_name, schema_name, owner, spark, "Processing failed", f"{file_path}, Error: {str(e)}")

def execute_ingest(owner, spark, table_name, folder_id, file_name_regex, sheet_name=None, metadata=None, latest=False, just_copy=True, delete=False):
    client = _initialize_box_client()
    catalog_name = 'development' # dev
    schema_name = 'eda_gc_raw'
    try:
        files = _download_file(catalog_name, schema_name, owner, spark, client, table_name, folder_id, file_name_regex, latest)
        if not files:
            _log_task(catalog_name, schema_name, owner, spark, "No files to process", table_name)
            return
        for file_path in files:
            _process_file(spark, owner, file_path, catalog_name, schema_name, table_name, metadata, just_copy, sheet_name)
        if delete:
            _delete_file(catalog_name, schema_name, owner, spark, client, folder_id, file_name_regex, latest)
        _log_task(catalog_name, schema_name, owner, spark, "Ingest completed", table_name)
    except Exception as e:
        _log_task(catalog_name, schema_name, owner, spark, "Ingest failed", str(e))
