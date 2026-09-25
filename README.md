## ERP-rajapinnan ETL-prosessi

## Overview

Python scripts for an ETL process that does the following:

1. Get raw data from (erp) api and save it locally as parquet (Extract)
2. Transform the raw data to data warehouse compatible form (Transform)
3. Upload the data to a postgresql data warehouse (Load).

Includes a DAG for Apache Airflow.

## Dev setup

1. Clone the project
2. Create virtual environment with `uv init` or `python3 -m venv .venv`
3. Install dependencies with `uv sync` or `pip install -r requirements.txt`
4. Create .env with `cp .env.example .env` and modify as needed
5. Modify and run example visualization with `python -m scripts.test`

## Deployment setup

### General

Data is fetched once a day and is separated to three stages: raw, staged, combined. The data of the last two days is saved locally for each raw,staged and combined. The data is stored on docker under results-folder:

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

project/
├── dags/
│   └──local_dag.py
├── src/
│   ├──auth/
│   ├──config/
│   ├──database/
│   ├─dag_test.py
│   ├─dataset_to_excel.py
│      ...
├── server_dag(production)/
│    └── python-venvs/
│        ├──erp_pipeline/
│        │  └──erp_pipeline/
│        │       ├──auth/
│        │       ├──config/
│        │       ├──database/
│        │       ...
│        └──.venv

Production haaran virtual environmentin kopiointi:
pip install -e "server_dag/python-venvs/erp_pipeline/"

Src-kansio sisältää lokaalin testauksen, joka ei käytä airflowia ollenkaan. Tietoja ei tallenneta tietokantaan, mutta niistä luodaan excel-tiedostot.
Lokaalissa testausympäristössä käytetään tuotanto tiedostojen 


The DAG uses Airflow's `ExternalPythonOperator` running in a virtual environment to keep project dependencies from interfering with Airflow's own dependencies (as well as any other projects'). The DAG file authenticates to api using credentials defined in the .env file and creates and stores the access token to Airflow's variables


This setup is based on Airflow's example `docker-compose.yml` file that defines multiple containers for different services. All Airflow-specific volumes are assumed to be bind mounts that are shared between Airflow containers (e.g. `airflow-dag-processor`, `airflow-worker` etc.).

The virtual environment is created under Airflow's `config` folder, which in this case is expected to be mounted to the containers as follows:

`/home/user/projects/airflow/data/config:/opt/airflow/config`

An example deployment process would go as follows (adjust the paths as needed):

### The virtual environment

1. Access the Airflow host machine (e.g. with `ssh`)
2. Access Airflow container with `docker exec -it airflow-dag-processor bash` (adjust container name as needed)
3. Create directory for the virtual environment with `mkdir -p /opt/airflow/config/python-venvs/erp_pipeline/DATA`
4. Create the virtual environment with `cd /opt/airflow/config/python-venvs/erp_pipeline && python -m venv .venv`
5. On the local machine, bundle the required files to `./build` by running `./build.sh`
6. Copy contents of the `./build` directory to the Airflow host machine, e.g.:

`scp -r ./build/* user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/erp_pipeline/`

`scp ./build/.env user@AIRFLOW_HOST:/home/user/projects/airflow/data/config/python-venvs/erp_pipeline/`

(it's likely to get `permission denied` error, in that case either `chown` or `chmod` the target folder as appropriate, or `scp` the stuff to somewhere else first, then `sudo cp` to the destination folder on the target host)

7. Access the Airflow host machine and Airflow container again as per steps 1 and 2 above, then `cd /opt/airflow/config/python-venvs/erp_pipeline`
8. Activate the python virtual environment with `source ./.venv/bin/activate`
9. Install the copied python modules and their dependencies to the virtual environment with `pip install -e .`

### The DAG

The dag file is server_erp_file_processor.py. It calls files that are defined in python-venvs/erp_pipeline/ folder.
