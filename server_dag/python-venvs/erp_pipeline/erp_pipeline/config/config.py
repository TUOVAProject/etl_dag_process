from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

BASE_URL = os.environ["BASE_URL"]
LOGIN_URL = os.environ["LOGIN_URL"]
COMMON_URL = os.environ["COMMON_URL"]
TIMERECORDING_URL = os.environ["TIMERECORDING_URL"]
MANUFACTURING_URL = os.environ["MANUFACTURING_URL"]
SALES_URL = os.environ["SALES_URL"]
INVENTORY_URL = os.environ["INVENTORY_URL"]
PURCHASE_URL = os.environ["PURCHASE_URL"]
USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]
AUTHVALUE = os.environ["AUTHVALUE"]
DB_SERVER = os.environ["DB_SERVER"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
CA_BUNDLE_PATH = os.environ["ERP_CA_BUNDLE_PATH"]