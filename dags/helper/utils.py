# helper/utils.py

from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging


def upsert_dataframe(df, target_table, pk_columns, postgres_conn_id):
    """
    Upsert a dataframe into a PostgreSQL table using a temporary table.
    Supports multiple primary keys.

    Args:
        df (pd.DataFrame): Data to load
        target_table (str): Schema.table target
        pk_columns (str | list): Column(s) to use as primary key
        postgres_conn_id (str): Airflow connection ID
    """

    if df.empty:
        logging.info(f"Nothing to upsert → {target_table}")
        return

    # Normalize pk_columns into list
    if isinstance(pk_columns, str):
        pk_columns = [pk_columns]

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()

    temp_table = target_table.replace(".", "_") + "_tmp"

    with engine.begin() as conn:
        # Load into temp table
        df.to_sql(temp_table, conn, if_exists="replace", index=False)

        cols = list(df.columns)
        cols_str = ", ".join(cols)

        # pk clause
        pk_clause = ", ".join(pk_columns)

        # all non-pk columns update on conflict
        update_cols = [c for c in cols if c not in pk_columns]
        excluded_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

        sql = f"""
            INSERT INTO {target_table} ({cols_str})
            SELECT {cols_str} FROM {temp_table}
            ON CONFLICT ({pk_clause})
            DO UPDATE SET {excluded_clause};

            DROP TABLE IF EXISTS {temp_table};
        """

        conn.execute(sql)

    logging.info(f"Upsert completed → {target_table} ({len(df)} rows) - PK: {pk_columns}")
