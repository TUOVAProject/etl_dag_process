from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Any, Union
from urllib.parse import quote_plus
from config.config import DB_SERVER,DB_NAME,DB_PORT,DB_USER,DB_PASSWORD
from sqlalchemy import text as sa_text
import pandas as pd
def get_engine() -> Union[Engine, Any]:
    
    server = DB_SERVER
    port = DB_PORT
    database = DB_NAME
    username = DB_USER
    pw = quote_plus(DB_PASSWORD)
    connection_string = f"postgresql+psycopg2://{username}:{pw}@{server}:{port}/{database}"

    engine = create_engine(connection_string)
    return engine