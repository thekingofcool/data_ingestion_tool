import logging
import re
import os
import shutil
import zipfile
import subprocess
from boxsdk import Client, JWTAuth, OAuth2
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _log_task(owner, action, file_info):
    logging.info(f"Owner: {owner}, Action: {action}, File Info: {file_info}")
    print(f"Owner: {owner}, Action: {action}, File Info: {file_info}")

# For enterprise box account
# def _initialize_box_client():
#     auth = JWTAuth(
#         client_id = "client_id",
#         client_secret = "client_secret",
#         enterprise_id = "enterprise_id",
#         jwt_key_id = "jwt_key_id",
#         rsa_private_key_data=("pemkey").replace('\\n', '\n'),
#         rsa_private_key_passphrase=bytes("rsa_private_key_passphrase").encode('utf-8')
#         )
#     box_client = Client(auth)
#     return box_client

# For personal box account
def _initialize_box_client():
    oauth = OAuth2(
        client_id=os.environ.get('BOX_CLIENT_ID'),
        client_secret=os.environ.get('BOX_CLIENT_SECRET'),
        access_token=os.environ.get('BOX_ACCESS_TOKEN'),
        refresh_token=os.environ.get('BOX_REFRESH_TOKEN')
    )
    box_client = Client(oauth)
    return box_client

def _download_file(owner_name, client, table_name, folder_id, file_name_regex, latest=False):
    try:
        items = client.folder(folder_id).get_items()
        matched_files = [item for item in items if re.match(file_name_regex, item.name)]
        if not matched_files:
            _log_task(owner_name, "No files matched", f"Regex: {file_name_regex}")
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
        _log_task(owner_name, "Created temp directory", temp_dir)
        for file in matched_files:
            with open(temp_dir + '/' + file.name, 'wb') as file_open:
                client.file(file.id).download_to(file_open)
            _log_task(owner_name, "Downloaded file", f"{folder_id}/{file.name}")
        return [os.path.join(f"{temp_dir}", file.name) for file in matched_files]
    except Exception as e:
        _log_task(owner_name, "Download failed", str(e))
        return None

def _delete_file(owner_name, client, folder_id, file_name_regex, latest=False):
    try:
        items = client.folder(folder_id).get_items()
        matched_files = [item for item in items if re.match(file_name_regex, item.name)]
        if not matched_files:
            _log_task(owner_name, "No files matched", f"Regex: {file_name_regex}")
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
            _log_task(owner_name, "Deleted file", f"{folder_id}/{file.name}")
    except Exception as e:
        _log_task(owner_name, "Delete failed", str(e))

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

def _write_to_delta_table(spark, df, catalog, schema, table_name, error_table=False):
    delta_table = f"{catalog}.{schema}.{table_name}{'_error' if error_table else ''}"
    spark_df = spark.createDataFrame(df).withColumn("process_dt", current_timestamp())
    try:
        spark_df.write.format("delta").mode("append").saveAsTable(delta_table)
        logging.info(f"Data written to {'error ' if error_table else ''}Delta table: {delta_table}")
    finally:
        spark_df.unpersist()

def _process_file(spark, owner_name, file_path, table_name, metadata, copy=False):
    try:
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        if copy:
            file_dir = os.path.dirname(file_path)
            subprocess.check_call(f'aws s3 cp {file_dir}/* s3://s3_bucket/{table_name}/', shell=True)
            _log_task(owner_name, "File copied to S3", f"s3://s3_bucket/{table_name}/ (Size: {file_size_mb:.2f} MB)")
            return
        if file_path.endswith('.zip'):
            temp_dir = f"/tmp/unzipped/{table_name}"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            _log_task(owner_name, "ZIP file extracted", f"{file_path} -> {temp_dir}")
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    extracted_file_path = os.path.join(root, file)
                    _process_file(spark, owner_name, extracted_file_path, table_name, metadata, copy=False)
            return
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.json'):
            df = pd.read_json(file_path)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path)
        else:
            _log_task(owner_name, "Unsupported file type", f"{file_path} (Size: {file_size_mb:.2f} MB)")
            return
        df.columns = df.columns.str.strip()
        valid_data, invalid_data = _validate_and_split_data(df, metadata, metadata.get('non_nullable_fields', []))
        if not valid_data.empty:
            _write_to_delta_table(spark, valid_data, 'catalog_name', 'schema_name', table_name)
        if not invalid_data.empty:
            _write_to_delta_table(spark, invalid_data, 'catalog_name', 'schema_name', table_name, error_table=True)
        _log_task(owner_name, "File processed", f"{file_path} (Valid: {len(valid_data)}, Invalid: {len(invalid_data)})")
    except Exception as e:
        _log_task(owner_name, "Processing failed", f"{file_path}, Error: {str(e)}")

def execute_ingest(owner_name, table_name, folder_id, file_name_regex, metadata, latest=False, just_copy=False, delete=False):
    client = _initialize_box_client()
    spark = SparkSession.builder \
        .appName("DataIngestionDemo") \
        .getOrCreate()
    try:
        files = _download_file(owner_name, client, table_name, folder_id, file_name_regex, latest)
        if not files:
            _log_task(owner_name, "No files to process", table_name)
            return
        for file_path in files:
            _process_file(spark, owner_name, file_path, table_name, metadata, just_copy)
        if delete:
            _delete_file(owner_name, client, folder_id, file_name_regex, latest)
        _log_task(owner_name, "Ingest completed", table_name)
        spark.stop()
    except Exception as e:
        _log_task(owner_name, "Ingest failed", str(e))
        spark.stop()