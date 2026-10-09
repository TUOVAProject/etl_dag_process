## ERP-api ETL-process

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

1. Get raw data from (erp) api and saves it locally as parquet (Extract)
2. Transform the raw data to data warehouse compatible form (Transform)
3. Upload the data to a postgresql data warehouse (Load).

Includes a DAG for Apache Airflow and a local test that creates excel files from the data.

## Prerequisites

Dataabase server and database. Database creation script can be found from here: TODO

## Dev setup

1. Clone the project
2. Create virtual environment with `uv init` or `python3 -m venv .venv`
3. Install dependencies with `uv sync` or `pip install -r requirements.txt`
4. Create .env with `cp .env.example .env` and modify as needed
5. Modify and run example visualization with `python -m scripts.test`

## Deployment setup

### General

When dag is deployed and enabled, data is fetched once a day and is separated to three stages: raw, staged and combined. 
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
│
project/
├── dags/
│   └──erp_file_processor.py  #Dag that can be runned locally on your machine with docker
├── src/
│   ├──config/ #ERP-data models are defined here. This differs from production.
│   │   └── erp_config
│   │   
│   ├─dag_test.py #Local run without airflow, saves data to excel files using dataset_to_excel.py
│   ├─dataset_to_excel.py
│      ...
├── server_dag/ #production
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
Virtual environment copy command for local developing:
pip install -e "server_dag/python-venvs/erp_pipeline/"

src-folder contains testing for local environment and it doesn't ue airflow at all. Data won't be saved to database, it will be saved as excel.

The DAG uses Airflow's `ExternalPythonOperator` running in a virtual environment to keep project dependencies from interfering with Airflow's own dependencies (as well as any other projects'). The DAG file authenticates to api using credentials defined in the .env file and creates and stores the access token to Airflow's variables

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

### The DAG

The dag file is server_erp_file_processor.py. It calls files that are defined in python-venvs/erp_pipeline/ folder.

### erp-pipeline folder content

#### auth

Login method to api is defined here. On the request verify parameter is set to false. Consider changing this if accessing to external api. 
File requires that .env file is defined with the following values: USERNAME, PASSWORD, LOGIN_URL and AUTHVALUE.

#### combine_data

Contains combine_data_files.py. All the staged data combining is defined here. The staged data is combined and then saved to results/combined folder as parquet.

#### config

This folder contains 3 files: config.py, erp_config.py, path_config.

config.py: Contains all the variables that are defined on the .env file. This can be also used as helper to see what values .env file must have.

erp-config.py: Has all the columns that are used on the processing of the files. When data is fetched from the api, the whole data is stored to results/raw/ folder. These column definitions in the erp_config.py are used to define what data we need to use for staging and combining.  If you need more data or to add columns later, define them here.

path_config.py: Contains path definitions.

#### database

The database-folder contains connection.py, database_mapper.py, writers.py

connection.py: The database-connection is defined here. File requires that .env file is defined with the following values: DB_SERVER, DB_NAME, DB_PORT, DB_USER, DB_PASSWORD. Uses postgresql.

database_mapper.py: The data is mapped to the datamodel. The database needs to be defined with the same columns and column-types. You can find the create-database-script from here: TODO: .

writers.py: Provides database write operations used by the ETL pipeline. It supports loading data into PostgreSQL tables, clearing existing data when required, truncating fact tables during full refreshes, and performing incremental loads through UPSERT logic to avoid duplicate records while updating existing ones.

#### fetch_erp_data

fetch_erp_data-folder contains common.py, inventory.py, manufacturing.py, sales.py and time_recording.py. 

Each endpoint category has its own file responsible for fetching data from the API. The API endpoints used by the category are defined within that file. To scale data retrieval, additional endpoints can be added or existing query parameters can be modified. 

Data is exported to separate files. During each run, old files are automatically removed according to the days_to_keep_parameter setting. For example, if days_to_keep_parameter = 2, files from the previous two days are retained and older files are deleted.

#### helpers

helpers folder contains file_helpers.py and type_helpers.py

file_helpers.py: Functions for API extraction file management, including endpoint-to-file name conversion, retrieval of the latest extracted files, date-based partition handling, and storage of API response data as Parquet files in a structured directory hierarchy.

type_helpers.py: Functions for data transformation, type conversion, and date/time key generation.

#### load_data

load_data folder contains data_loaders.py. For loading, filtering, and retrieving the latest parquet datasets with given parameters.

#### models

models-folder is used to define all the classess and enums. Now it has only LoadMode enumeration that defines if the file is loaded from staged or combined folder.

#### process

process-folder contains dataset_specific.py and process_erp.py.

dataset_specific.py: Dataset-specific helper functions for resolving business logic and deriving missing values.

process_erp.py: Processes the saved files from raw to staged. Drops the unnecessary columns using erp_source.config.py column declarations and saves the data in steged folder. During each run, old files are automatically removed according to the days_to_keep_parameter setting. For example, if days_to_keep_parameter = 2, files from the previous two days are retained and older files are deleted.

#### file_cleaner.py

Functions for cleaning old parquet files and dated folders while preserving recent history.

#### save.py

Functions for saving parquet files.