import re

import pandas as pd
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text
from typing import Literal, Union


IdentifierMode = Literal["truncate", "delete"]
WriteMode = Literal["fail", "replace", "append", "delete_rows", "truncate_insert", "delete_insert"]


def _quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", identifier):
        raise ValueError(f"Invalid table identifier: {identifier}")

    parts = identifier.split(".")
    return ".".join(f'"{part}"' for part in parts)


def clear_table(
    table_name: str,
    conn: Union[Engine, Connection],
    mode: IdentifierMode = "truncate",
):
    table_ref = _quote_identifier(table_name)

    if mode == "truncate":
        statement = text(f"TRUNCATE TABLE {table_ref}")
    elif mode == "delete":
        statement = text(f"DELETE FROM {table_ref}")
    else:
        raise ValueError(f"Unknown clear mode: {mode}")

    if isinstance(conn, Engine):
        with conn.begin() as active_conn:
            active_conn.execute(statement)
    else:
        conn.execute(statement)


def save_df(
    df: pd.DataFrame,
    table_name: str,
    conn : Union[Engine, Connection],
    if_exists: WriteMode = "append",
):

    write_mode = if_exists

    if write_mode == "truncate_insert":
        clear_table(table_name, conn, mode="truncate")
        write_mode = "append"
    elif write_mode == "delete_insert":
        clear_table(table_name, conn, mode="delete")
        write_mode = "append"

    
    if isinstance(conn, Engine):
        # Oma per-kutsu transaktio
        with conn.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists=write_mode,
                index=False,
                method="multi",
                chunksize=1000,
            )
    else:
        df.to_sql(
            table_name,
            con=conn,
            if_exists=write_mode,
            index=False,
            method="multi",
            chunksize=1000,
        )





