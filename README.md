# Data Ingestion Tool

This project provides a set of functionalities for ingesting data from Box into a Delta table using PySpark. It is designed to facilitate the downloading, processing, and validation of data files, ensuring that valid data is written to the appropriate Delta tables while logging all actions for traceability.

## Features

- **Logging**: All actions are logged for monitoring and debugging purposes.
- **Box Client Initialization**: Connects to Box using JWT authentication.
- **File Downloading**: Downloads files from a specified Box folder based on a regex pattern.
- **File Deletion**: Deletes files from Box after processing if specified.
- **Data Validation**: Validates data against specified metadata and handles non-nullable fields.
- **Delta Table Writing**: Writes valid and invalid data to Delta tables for further analysis.

## Installation

To install the package, use the following command:

```bash
pip install data_ingestion_tool
```

## Usage

To use the data ingestion tool, import the `execute_ingest` function from the `box_ingest` module and call it with the required parameters:

```python
from data_ingestion_tool.box_ingest import execute_ingest

execute_ingest(
    owner_name="your_owner_name",
    table_name="your_table_name",
    folder_id="your_folder_id",
    file_name_regex="your_file_name_regex",
    sheet_name = 'your_excel_sheet_name',
    metadata={"your_metadata_key": "your_metadata_value"},
    latest=False,
    just_copy=True,
    delete=True
)
```

### Parameters

- `owner_name`: The name of the owner of the task.
- `table_name`: The name of the table to which data will be ingested.
- `folder_id`: The ID of the Box folder from which to download files.
- `file_name_regex`: A regex pattern to match file names for downloading.
- `sheet_name`: Excel sheet name if needed to be specified.
- `metadata`: A dictionary containing metadata for data validation.
- `latest`: A boolean indicating whether to download only the latest file.
- `just_copy`: A boolean indicating whether to only copy files to S3 without processing.
- `delete`: A boolean indicating whether to delete files from Box after processing.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any enhancements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
