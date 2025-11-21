from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def upsert_dataframe(df, table_name, pk_column="uuid", postgres_conn_id="transjakarta"):
    if df.empty:
        logging.info(f"No data to upsert into {table_name}")
        return

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()

    # Ensure primary key exists
    if pk_column not in df.columns:
        raise ValueError(f"{pk_column} must exist in dataframe for UPSERT.")

    # Prepare temp table name
    temp_table = f"{table_name}_temp_upsert"

    with engine.begin() as conn:
        # 1. Write to temporary table
        df.to_sql(temp_table, conn, if_exists="replace", index=False)

        # 2. Build column lists
        cols = list(df.columns)
        cols_str = ", ".join(cols)
        excluded = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != pk_column])

        # 3. Execute UPSERT
        sql = f"""
            INSERT INTO {table_name} ({cols_str})
            SELECT {cols_str} FROM {temp_table}
            ON CONFLICT ({pk_column})
            DO UPDATE SET {excluded};
        """
        conn.execute(sql)

        # 4. Drop temp table
        conn.execute(f"DROP TABLE IF EXISTS {temp_table};")

    logging.info(f"Upsert into {table_name} completed: {len(df)} rows processed.")
