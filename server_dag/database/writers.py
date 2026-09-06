import re

import pandas as pd
from sqlalchemy.engine import Engine, Connection
from sqlalchemy import text, func
from sqlalchemy.dialects.postgresql import insert
from typing import Literal, Union, List, Optional


IdentifierMode = Literal["truncate", "delete"]
WriteMode = Literal["fail", "replace", "append"]


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


def truncate_fact_tables(
    conn: Union[Engine, Connection],
    fact_tables: List[str],
):
    tables = ", ".join(f'"{table}"' for table in fact_tables)

    conn.execute(
        text(
            f"TRUNCATE TABLE {tables} RESTART IDENTITY"
        )
    )
def create_postgres_upsert_method(conflict_columns: List[str]):
    def upsert(table, conn, keys, data_iter):
        rows = [dict(zip(keys, row)) for row in data_iter]

        if not rows:
            return

        stmt = insert(table.table).values(rows)

        update_columns = {
            col: func.coalesce(
                stmt.excluded[col],
                table.table.c[col]
            )
            for col in keys
            if col not in conflict_columns
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_=update_columns,
        )

        conn.execute(stmt)

    return upsert

def save_df(
    df: pd.DataFrame,
    table_name: str,
    conn: Union[Engine, Connection],
    if_exists: WriteMode = "append",
    clear_before_insert: Optional[IdentifierMode] = None,
    conflict_columns: Optional[List[str]] = None
):
    method = "multi"

    if conflict_columns:
        method = create_postgres_upsert_method(
            conflict_columns=conflict_columns
        )

    if clear_before_insert:
        clear_table(
            table_name,
            conn,
            mode=clear_before_insert
        )

    if isinstance(conn, Engine):
        with conn.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists=if_exists,
                index=False,
                method=method,
                chunksize=1000,
            )
    else:
        df.to_sql(
            table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            method=method,
            chunksize=1000,
        )





