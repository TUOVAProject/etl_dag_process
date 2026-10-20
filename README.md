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


Author : Sasu Eskelinen

## Overview

Python project implementing an ETL pipeline that:

1. Extracts raw data from the MONITOR ERP API and stores it as parquet files (Extract).
2. Transforms the raw data into a format compatible with the data warehouse (Transform).
3. Loads the transformed data into a PostgreSQL data warehouse (Load).

```
Monitor ERP API
      │
      ▼
     Raw
      │
      ▼
    Staged
      │
      ▼
   Combined
      │
      ▼
Database mapping
      │
      ▼
PostgreSQL Data Warehouse

```

The project also includes tools for running the pipeline locally and exporting the resulting data to Excel files.

### Note: The project is configured for the MONITOR ERP API. Using a different API requires modifications to the code and configuration.

## Prerequisites

- Git
- Python
- Docker
- PostgreSQL database server
- PostgreSQL database for the data warehouse 

The database creation scripts can be found from here: https://github.com/TUOVAProject/tuotantotiedon-tietomalli. Scripts are available for both PostgreSQL and SQL Server.

If you are interested in setting up an on-premises server, instructions can be found here: https://github.com/TUOVAProject/tuova_on_prem_server.

### General

When the DAG is deployed and enabled, data is fetched once per day and processed through three stages: raw, staged, and combined.
The retention period for raw, staged, and combined data is controlled by the days_to_keep_parameter setting. By default, data from the two most recent days is retained. 
The data is stored on Docker under results directory:
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

The project structure:

project/
│
├── src/   
│   ├─dag_test.py #Local run without Airflow, saves data to Excel files using dataset_to_excel.py
│   ├─dataset_to_excel.py
│   ├─dev.env
│      ...
├── server_dag/ #both production and dev environment uses the same code.
│    └── python-venvs/
│        ├──erp_pipeline/
│        │  ├─erp_pipeline/
│        │  │    ├──auth/
│        │  │    ├──config/
│        │  │    ├──database/
│        │  │   ...
│        │  │
│        │  ├─ .venv/
│        │  ├─ .env
│        │  ├─ pyproject.toml
│        │ ...
│       ...
├── .env
├── main.py
├── requirements-dev.txt

```

#### Notice that the local dag_test.py file uses dev.env-file. The .env and dev.env file must have these variables:

- `LOGIN_URL`: API authentication URL
- `COMMON_URL`: Common API endpoint URL
- `TIMERECORDING_URL`: Time recording API endpoint URL
- `MANUFACTURING_URL`: Manufacturing API endpoint URL
- `SALES_URL`: Sales API endpoint URL
- `INVENTORY_URL`: Inventory API endpoint URL
- `PURCHASE_URL`: Purchase API endpoint URL
- `USERNAME`: API username
- `PASSWORD`: API password
- `AUTHVALUE`: Header value added to the authentication request
- `DB_SERVER`: Database server address
- `DB_PORT`: Database server port
- `DB_NAME`: Database name
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password
- `OUTPUT_EXCEL_FOLDER`: Directory where generated Excel files are saved (`dev.env` only)

Adjust these values according to the API configuration being used.

## Local Excel file generation

The src/ directory contains `dag_test.py` and `dataset_to_excel.py`.

The test can be run from `main.py`.

`dag_test.py` file uses the same code as the Airflow DAG with a few exceptions: 
1. Instead of using `.env` file it uses `dev.env` file.
2. Instead of saving the data to database, it creates Excel files with database schema.

`dag_test.py` simulates the Airflow DAG locally without the need of Airflow. It fetches, processes, stages, and combines ERP data, and exports the resulting fact and dimension tables into an Excel file instead of loading them into the database.

`dataset_to_excel.py` is a standalone utility that converts the latest available raw and combined datasets into Excel files. It does not fetch or process data and can therefore be run independently as long as the source data already exists.

### Local development setup

1. Clone the project
2. In the root directory of etl_dag_process, create a virtual environment:
    ```bash
    python3 -m venv .venv
3. Activate the virtual environment
    ```bash
    source .venv/bin/activate
4. Install the erp_pipeline package and its dependencies:
    ```bash 
    pip install -e "server_dag/python-venvs/erp_pipeline/"
5. Create and set up `.env` and `dev.env` files. Required values are listed earlier on the instructions
6. Install development dependecies required by `dag_test.py` and `dataset_to_excel.py`:
    ```bash
    python -m pip install -r requirements-dev.txt
7. Optionally, create a Visual Studio Code launch configuration in `.vscode/launch.json` 
    ```
    {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "Python Debug: main.py",
                "type": "debugpy",
                "request": "launch",
                "program": "${workspaceFolder}/main.py",
                "console": "integratedTerminal",
                "cwd": "${workspaceFolder}"
            }
        ]
    }
    ```
8. Run the project:
    ```bash
    python main.py
9. Verify the generated data model Excel files.
10. Generated Excel files are written to the directory defined by the `OUTPUT_EXCEL_FOLDER` variable in `dev.env`.



## Local Airflow deploy configuration

The DAG uses Airflow's `ExternalPythonOperator` to run pipeline tasks in a separate Python virtual environment. This isolates the project's dependencies from Airflow's own dependencies and from other projects running in the same environment.

The project includes a default `docker-compose.yaml` file that defines the required Airflow services.

All Airflow-specific volumes are assumed to be bind mounts that are shared between Airflow containers (e.g. `airflow-dag-processor`, `airflow-worker` etc.).  

`docker-compose.yaml` mounts the following files and directories into the Airflow containers:

- `server_dag/server_erp_file_processor.py` → `/opt/airflow/dags/server_erp_file_processor.py`
- `server_dag/python-venvs/erp_pipeline` → `/opt/airflow/config/python-venvs/erp_pipeline`

When the environment is started with `docker compose up -d`, the `erp-pipeline-init` service runs `setup_venv.sh`, which creates the Python virtual environment (if it does not already exist) and installs the `erp_pipeline` package with its dependencies. The installation writes to the host-mounted directory `./server_dag/python-venvs/erp_pipeline`, so the container user (UID) must have write access to that directory. Otherwise, the startup fails with a permission error.

### 1. Set the Airflow user UID

Create a `.env` file in the same directory as `docker-compose.yaml`. First, check your UID with `id -u`.

- If the result is **0** (you are root), use the default UID 50000:
```bash
  echo "AIRFLOW_UID=50000" > .env
```
- Otherwise, use your own UID:
```bash
  echo "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Set directory ownership

The ownership of the mounted directories must be compatible with the `AIRFLOW_UID` configured in `.env`. If you are running as root, omit `sudo`.

```bash
sudo chown -R <AIRFLOW_UID>:0 ./server_dag ./logs ./plugins
```

### 3. Start the services

```bash
docker compose up -d
docker compose logs erp-pipeline-init
```

A successful installation ends with the line `erp_pipeline virtual environment is ready.`

After starting the Docker environment:

1. Open the Airflow web interface at http://localhost:8080.
2. Log in using the default credentials defined in docker-compose.yaml.
3. Locate the server_erp_file_processor DAG.
4. Select Trigger DAG to start the pipeline.

The DAG contains 29 tasks that are executed based on their dependencies. If a task fails, Airflow retries it automatically after five minutes. After a second failure, the task is marked as failed, and its dependent downstream tasks will not be executed.


## Production deployment setup

The example below assumes that /home/user/projects/airflow/data/config/python-venvs/erp_pipeline/ on the host is mounted to /opt/airflow/config/python-venvs/erp_pipeline/ inside the Airflow container. 
Adjust the paths according to your deployment.

An example deployment process would go as follows (adjust the paths as needed):

1. Access the Airflow host machine (e.g. with `ssh`)
2. Access Airflow container with `docker exec -it airflow-worker bash` (adjust container name as needed)
3. Create directory for the virtual environment with `mkdir -p /opt/airflow/config/python-venvs/erp_pipeline`
4. Create the virtual environment with `cd /opt/airflow/config/python-venvs/erp_pipeline && python -m venv .venv`
5. Copy contents of the `/server_dag/python-venvs/erp_pipeline` directory to the Airflow host machine, e.g.:

`scp -r ./build/* user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/erp_pipeline/`

`scp ./build/.env user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/erp_pipeline/`

(it's likely to get `permission denied` error, in that case either `chown` or `chmod` the target directory as appropriate, or `scp` the contents to somewhere else first, then `sudo cp` to the destination directory on the target host)

6. Access the Airflow host machine and Airflow container again as per steps 1 and 2 above, then `cd /opt/airflow/config/python-venvs/erp_pipeline`
7. Activate the python virtual environment with `source ./.venv/bin/activate`
8. Install the copied python modules and their dependencies to the virtual environment with `pip install -e .`

### The DAG

The dag file is he DAG file is `server_erp_file_processor.py`. 
It orchestrates the ETL pipeline by calling functions provided by the erp_pipeline package located under `python-venvs/erp_pipeline/erp_pipeline/`.

### ERP-pipeline directory content

#### auth

Contains the authentication and API login logic. SSL verification is currently disabled (`verify=False`). Consider enabling certificate validation when connecting to external APIs. 
The module requires the following variables to be defined in the `.env` file: `USERNAME`, `PASSWORD`, `LOGIN_URL` and `AUTHVALUE`.

#### combine_data

Contains the logic for combining staged datasets. The combined datasets are saved as Parquet files in the `results/combined/` directory.

#### config

This directory contains 3 files: `config.py`, `erp_config.py`, `path_config.py`.

`config.py`: Contains the configuration variables loaded from the `.env` file. It can also be used as a reference for the required environment variables.
This can be also used as helper to see what values `.env` file must have.

`erp_config.py`: Defines the columns used when processing ERP datasets. Raw API responses are stored in the `results/raw/ directory`, while the column definitions in `erp_config.py` determine which fields are retained during staging and combining. If additional fields are required, update the corresponding column definitions in this file.

`path_config.py`: Contains path definitions.

#### database

The database/ directory contains `connection.py`, `database_mapper.py`, `writers.py`

`connection.py`: Defines the PostgreSQL database connection. The following variables must be defined in `.env`: `DB_SERVER`, `DB_NAME`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`.

`database_mapper.py`: Maps processed datasets to the data warehouse schema. The database schema must match the column names and data types defined by the mapper. 
You can find the create-database-script from here: https://github.com/TUOVAProject/tuotantotiedon-tietomalli

`writers.py`: Provides the database write operations used by the ETL pipeline. It supports loading data into PostgreSQL tables, clearing existing data when required, truncating fact tables during full refreshes, and performing incremental loads using UPSERT logic to update existing records and avoid duplicates.

#### fetch_erp_data

The fetch_erp_data/ directory contains `common.py`, `inventory.py`, `manufacturing.py`, `sales.py` and `time_recording.py`. 

Each endpoint category has its own file responsible for fetching data from the API. The API endpoints used by the category are defined within that file. 
Additional endpoints can be added, or existing query parameters can be modified, to extend the data retrieved from the API. 

Data is exported to separate files. During each run, old files are automatically removed according to the `days_to_keep_parameter` setting. For example, if `days_to_keep_parameter = 2`, files from the previous two days are retained and older files are deleted.

#### helpers

helpers directory contains `file_helpers.py` and `type_helpers.py`

`file_helpers.py`: Functions for API extraction file management, including endpoint-to-file name conversion, retrieval of the latest extracted files, date-based partition handling, and storage of API response data as Parquet files in a structured directory hierarchy.

`type_helpers.py`: Functions for data transformation, type conversion, and date/time key generation.

#### load_data

load_data directory contains `data_loaders.py`. 

`data_loaders.py`: Functions for loading, filtering, and retrieving the latest Parquet datasets based on the provided parameters.

#### models

The models/ directory contains shared classes and enumerations used by the pipeline. It currently contains the LoadMode enumeration

#### process

process directory contains `dataset_specific.py` and `process_erp.py`.

`dataset_specific.py`: Dataset-specific helper functions for resolving business logic and deriving missing values.

`process_erp.py`: Processes raw datasets into staged datasets. It removes unnecessary columns according to the column definitions in `erp_config.py` and saves the resulting datasets to the `results/staged/` directory. During each run, old files are automatically removed according to the `days_to_keep_parameter` setting. For example, if `days_to_keep_parameter = 2`, files from the previous two days are retained and older files are deleted.

#### file_cleaner.py

Functions for cleaning old Parquet files and dated directorys while preserving recent history.

#### save.py

Functions for saving Parquet files.


## Additional Information

### The TUOVA Project

- https://tuova.savonia.fi
- https://tuovaproject.github.io/

### The data model ("Tuotantotiedon tietomalli")

- https://github.com/TUOVAProject/tuotantotiedon-tietomalli
- https://tuovaproject.github.io/tuotantotiedon-tietomalli/

### On-premises server environment

- https://github.com/TUOVAProject/tuova_on_prem_server