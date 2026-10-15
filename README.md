## ERP-API ETL-process

 ## IMPORTANT!
>
> This repository was created as part of the TUOVA Project (see https://tuova.savonia.fi and https://tuovaproject.github.io/). Some of the information presented here might assume familiarity with other repositories created during the course of the project. For more details on those, see the "Additional Information" section at the bottom of this README.
>
> **Everything is provided as-is, use at your own risk.**
>
> TUOVA Project co-funded by the European Union / Etelä-Savon ELY-keskus.
>
> ![Co-funded by the EU](co-fundedbythe-eu.png)

## Overview

Python scripts for an ETL process that does the following:

1. Get raw data from (ERP) API and saves it locally as Parquet (Extract)
2. Transform the raw data to data warehouse compatible form (Transform)
3. Upload the data to a PostgreSQL data warehouse (Load).

Includes a DAG for Apache Airflow and a local test that creates Excel files from the data.

## Prerequisites

Database server and database. Database creation script can be found from here: TODO, and is available as both PostgreSQL compatible and sql compatible version.

## Dev setup

1. Clone the project
2. Create virtual environment with `uv init` or `python3 -m venv .venv`
3. Install dependencies with `uv sync` or `pip install -r requirements.txt`
4. Create .env with `cp .env.example .env` and modify as needed
5. Modify and run example visualization with `python -m scripts.test`

## Deployment setup

### General

When the dag is deployed and enabled, data is fetched once a day and is separated to three stages: raw, staged and combined. 
The data of the last two days is saved for each raw, staged and combined. The data is stored on docker under results-folder:
```
results/
├── combined/
│   └── category/
│       ├── yyyy-mm-dd-CombinedCategory1.parquet
│       ├── yyyy-mm-dd-CombinedCategory1.parquet
│       another-category/
│       ├── yyyy-mm-dd-CombinedAnotherCategory.parquet
│       ├── yyyy-mm-dd-CombinedAnotherCategory.parquet
│      ...
├── raw/
│   └── endpoint/
│       ├── category1
│       │    ├── yyyy-mm-dd/
│       │    │    └── raw-category1.parquet
│       │    └── yyyy-mm-dd/
│       │         └── raw-category1.parquet
│       ├── category2
│       │    ├── yyyy-mm-dd/
│       │    │    └── raw-category2.parquet
│       │    └── yyyy-mm-dd/
│       │         └── raw-category2.parquet
│       another-endpoint/
│       ├── another-category1
│       │   ├── yyyy-mm-dd/
│       │   │    └── raw-another-category1.parquet
│       │   └── yyyy-mm-dd/
│       │        └── raw-another-category1.parquet
│       ├─ another-category2
│       │    ├── yyyy-mm-dd/
│       │    │    └── raw-another-category2.parquet
│       │    └── yyyy-mm-dd/
│      ...        └── raw-another-category2.parquet
├── staged/
│   ├── endpoint/
│   │   ├── yyyy-mm-dd-Category1.parquet
│   │   ├── yyyy-mm-dd-Category2.parquet
│   ├── another-endpoint/
│   │   ├── yyyy-mm-dd-Category1.parquet
│   │   └── yyyy-mm-dd-Category2.parquet
│  ...
...
```
The project structure:

```
project/
│
├── src/   
│   ├─dag_test.py #Local run without airflow, saves data to Excel files using dataset_to_excel.py
│   ├─dataset_to_excel.py
│      ...
├── server_dag/ #both production and dev environment uses the same code.
│    └── python-venvs/
│        ├──erp_pipeline/
│        │  └──erp_pipeline/
│        │       ├──auth/
│        │       ├──config/
│        │       ├──database/
│        │       ...
│        ├─ .venv
│        ├─ .env
│        ├─ pyproject.toml
│
├── main.py
```

#### Notice that the local dag_test.py file uses dev.env-file. The .env and dev.env file must have these variables:
```
BASE_URL,
LOGIN_URL,
COMMON_URL,
TIMERECORDING_URL,
MANUFACTURING_URL,
SALES_URL,
INVENTORY_URL,
PURCHASE_URL,
USERNAME,
PASSWORD,
AUTHVALUE,
DB_SERVER,
DB_PORT,
DB_NAME,
DB_USER,
DB_PASSWORD,
OUTPUT_EXCEL //dev.env only
```
Install the `erp_pipeline` package into the current Python environment in editable mode. This makes the shared pipeline code available for both local development and Airflow DAG execution:

`pip install -e "server_dag/python-venvs/erp_pipeline/`

`src` folder contains testing for local environment and it doesn't ue airflow at all. Data won't be saved to database, it will be saved as Excel.

The DAG uses Airflow's `ExternalPythonOperator` running in a virtual environment to keep project dependencies from interfering with Airflow's own dependencies (as well as any other projects'). The DAG file authenticates to API using credentials defined in the .env file and creates and stores the access token to Airflow's variables

This setup is based on Airflow's example `docker-compose.yml` file that defines multiple containers for different services. All Airflow-specific volumes are assumed to be bind mounts that are shared between Airflow containers (e.g. `airflow-dag-processor`, `airflow-worker` etc.).

The virtual environment is created under Airflow's `config` folder, which in this case is expected to be mounted to the containers as follows:

`/home/user/projects/airflow/data/config:/opt/airflow/config`

An example deployment process would go as follows (adjust the paths as needed):

### The virtual environment

1. Access the Airflow host machine (e.g. with `ssh`)
2. Access Airflow container with `docker exec -it airflow-dag-processor bash` (adjust container name as needed)
3. Create directory for the virtual environment with `mkdir -p /opt/airflow/config/python-venvs/erp_pipeline/data`
4. Create the virtual environment with `cd /opt/airflow/config/python-venvs/erp_pipeline && python -m venv .venv`
5. Copy contents of the `./build` directory to the Airflow host machine, e.g.:

`scp -r ./build/* user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/erp_pipeline/`

`scp ./build/.env user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/erp_pipeline/`

(it's likely to get `permission denied` error, in that case either `chown` or `chmod` the target folder as appropriate, or `scp` the contents to somewhere else first, then `sudo cp` to the destination folder on the target host)

7. Access the Airflow host machine and Airflow container again as per steps 1 and 2 above, then `cd /opt/airflow/config/python-venvs/erp_pipeline`
8. Activate the python virtual environment with `source ./.venv/bin/activate`
9. Install the copied python modules and their dependencies to the virtual environment with `pip install -e .`


### Local test

On src/ folder we have `dag_test.py` and `dataset_to_excel.py`.

Local testing can be started from `main.py`.

dag_test.py file uses the same code as the airflow dag with few exceptions: 
1. Uses `.dev.env` instead of `.env`.
2. Instead of loading data into the database, it exports the resulting tables into an Excel file that mirrors the database structure.

`dag_test.py` simulates the Airflow DAG locally without requiring Airflow. It fetches, processes, stages, and combines ERP data, and exports the resulting fact and dimension tables into an Excel file instead of loading them into the database.

`dataset_to_excel.py` is a standalone utility that converts the latest available raw and combined datasets into Excel files. It does not fetch or process data and can therefore be executed independently as long as the required source data already exists.

#### Typical Workflow

For a full local test run:

Run `main.py` (executes `dag_test.py`).
Verify the generated database model Excel file.
Run `dataset_to_excel.py` to generate Excel exports of the latest raw and combined datasets for debugging and validation purposes.

The generated Excel files are written to the folder defined by dev.env variable `OUTPUT_EXCEL_FOLDER`.

### The DAG

The dag file is `server_erp_file_processor.py`. It calls files that are defined in `python-venvs/erp_pipeline/er_pipeline/` folder.

### ERP-pipeline folder content

#### auth

Authentication and API login logic are defined here. SSL verification is currently disabled (`verify=False`). Consider enabling certificate validation when connecting to external APIs. 
File requires that .env file is defined with the following values: `USERNAME`, `PASSWORD`, `LOGIN_URL` and `AUTHVALUE`.

#### combine_data

Contains `combine_data_files.py`. All the staged data combining is defined here. The staged datasets are combined and saved as Parquet files in the `results/combined folder`.

#### config

This folder contains 3 files: `config.py`, `erp_config.py`, `path_config.py`.

`config.py`: Contains all the variables that are defined on the `.env` file. This can be also used as helper to see what values `.env` file must have.

`erp_config.py`: Has all the columns that are used on the processing of the files. When data is fetched from the API, the whole data is stored to results/raw/ folder. These column definitions in the `erp_config.py` are used to define what data we need to use for staging and combining.  If additional fields are required later, update the column definitions in this file.

`path_config.py`: Contains path definitions.

#### database

The database-folder contains `connection.py`, `database_mapper.py`, `writers.py`

`connection.py`: The database-connection is defined here. File requires that .env file is defined with the following values: `DB_SERVER`, `DB_NAME`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`. Uses PostgreSQL.

`database_mapper.py`: The data is mapped to the datamodel. The database schema must match the column names and data types defined in the mapper. You can find the create-database-script from here: TODO: .

`writers.py`: Provides database write operations used by the ETL pipeline. It supports loading data into PostgreSQL tables, clearing existing data when required, truncating fact tables during full refreshes, and performing incremental loads through UPSERT logic to avoid duplicate records while updating existing ones.

#### fetch_erp_data

fetch_erp_data-folder contains `common.py`, `inventory.py`, `manufacturing.py`, `sales.py` and `time_recording.py`. 

Each endpoint category has its own file responsible for fetching data from the API. The API endpoints used by the category are defined within that file. To scale data retrieval, additional endpoints can be added or existing query parameters can be modified. 

Data is exported to separate files. During each run, old files are automatically removed according to the `days_to_keep_parameter` setting. For example, if `days_to_keep_parameter = 2`, files from the previous two days are retained and older files are deleted.

#### helpers

helpers folder contains `file_helpers.py` and `type_helpers.py`

`file_helpers.py`: Functions for API extraction file management, including endpoint-to-file name conversion, retrieval of the latest extracted files, date-based partition handling, and storage of API response data as Parquet files in a structured directory hierarchy.

`type_helpers.py`: Functions for data transformation, type conversion, and date/time key generation.

#### load_data

load_data folder contains `data_loaders.py`. 

`data_loaders.py`: Functions for loading, filtering, and retrieving the latest Parquet datasets based on the provided parameters.

#### models

models-folder is used to define all the classess and enums. Currently contains only the LoadMode enumeration.

#### process

process-folder contains `dataset_specific.py` and `process_erp.py`.

`dataset_specific.py`: Dataset-specific helper functions for resolving business logic and deriving missing values.

`process_erp.py`: Processes the saved files from raw to staged. Drops the unnecessary columns using `erp_source.config.py` column declarations and saves the data in staged folder. During each run, old files are automatically removed according to the `days_to_keep_parameter` setting. For example, if `days_to_keep_parameter = 2`, files from the previous two days are retained and older files are deleted.

#### file_cleaner.py

Functions for cleaning old Parquet files and dated folders while preserving recent history.

#### save.py

Functions for saving Parquet files.