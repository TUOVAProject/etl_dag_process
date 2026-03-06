import pandas as pd
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text
from typing import Literal, Union
def save_df(
    df: pd.DataFrame,
    table_name: str,
    conn : Union[Engine, Connection],
    if_exists: Literal["fail", "replace", "append", "delete_rows"] = "append",
):

    
    if isinstance(conn, Engine):
        # Oma per-kutsu transaktio
        with conn.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000,
            )
    else:
        df.to_sql(
            table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=1000,
        )





