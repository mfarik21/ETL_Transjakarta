from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

from helper.cleaning import clean_no_body_var, convert_status_var
from helper.build_dw import build_dimensions, build_facts, build_cube
from helper.report import export_report_to_csv
from helper.utils import upsert_dataframe

import pandas as pd
import logging
import os


DATA_DIR = "/opt/airflow/data"


def apply_dtype_mapping(df: pd.DataFrame) -> pd.DataFrame:
    dtype_map = {
        "_var": "string",
        "_int": "Int64",  # nullable integer
        "_flo": "float",
        "_boo": "boolean",
    }

    for col in df.columns:
        for suffix, dtype in dtype_map.items():

            if col.endswith(suffix):
                # --- BOOLEAN: ONLY "true" -> True ---
                if dtype == "boolean":
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.lower()
                        .str.strip()
                        .replace(
                            {
                                "true": True,
                            }
                        )
                    )

                    # everything not True = False
                    df[col] = df[col].apply(lambda x: True if x is True else False)

                    df[col] = df[col].astype("boolean")
                    break

                # --- ALL OTHER TYPES ---
                df[col] = df[col].astype(dtype, errors="ignore")
                break
    return df


def stage_transform_table(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names + strip strings."""
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    return df


def extract(**context):
    logging.info("EXTRACT: Load CSVs → raw schema")

    pg_raw = PostgresHook(postgres_conn_id="pg_raw")
    engine_raw = pg_raw.get_sqlalchemy_engine()

    # Truncate raw tables BEFORE load
    all_tables = [
        "routes",
        "shelter_corridor",
        "realisasi_bus",
        "transaksi_bus",
        "transaksi_halte",
    ]

    for tbl in all_tables:
        pg_raw.run(f"TRUNCATE TABLE {tbl};")

    # Tables mapping
    csv_tables = {
        "routes": "dummy_routes.csv",
        "shelter_corridor": "dummy_shelter_corridor.csv",
        "realisasi_bus": "dummy_realisasi_bus.csv",
    }

    transaksi_tables = {
        "transaksi_bus": "dummy_transaksi_bus.csv",
        "transaksi_halte": "dummy_transaksi_halte.csv",
    }

    for table, filename in csv_tables.items():
        path = os.path.join(DATA_DIR, filename)
        logging.info(f"Reading {path}")

        df = pd.read_csv(path)
        df.to_sql(table, engine_raw, if_exists="append", index=False)

        logging.info(f"Wrote {len(df)} rows → raw.{table}")

    for table, filename in transaksi_tables.items():
        path = os.path.join(DATA_DIR, filename)
        logging.info(f"Reading {path}")

        df = pd.read_csv(path)

        if "waktu_transaksi" in df.columns:
            df["waktu_transaksi"] = pd.to_datetime(
                df["waktu_transaksi"], errors="coerce"
            )
            df = df.sort_values("waktu_transaksi")

        df.to_sql(table, engine_raw, if_exists="append", index=False)

        logging.info(f"Wrote {len(df)} rows → raw.{table}")

    logging.info("EXTRACT complete")


def transform(**context):
    logging.info("TRANSFORM: raw → staging")

    engine_raw = PostgresHook("pg_raw").get_sqlalchemy_engine()

    primary_keys = {
        "routes": "route_code",
        "shelter_corridor": "shelter_name",
        "realisasi_bus": ["bus_body_no", "rute_realisasi"],
        "transaksi_bus": "uuid",
        "transaksi_halte": "uuid",
    }

    metadata_cols = ["_ingest_ts"]

    master_tables = ["routes", "shelter_corridor", "realisasi_bus"]
    transaksi_tables = ["transaksi_bus", "transaksi_halte"]

    def drop_metadata(df):
        return df.drop(
            columns=[c for c in metadata_cols if c in df.columns], errors="ignore"
        )

    def clean_common(df):
        if "no_body" in df.columns:
            df["no_body"] = df["no_body"].apply(clean_no_body_var)
        if "bus_body_no" in df.columns:
            df["bus_body_no"] = df["bus_body_no"].apply(clean_no_body_var)
        if "status" in df.columns:
            df["status"] = df["status"].apply(convert_status_var)
        return df

    def convert_datetimes(df):
        for col in ["waktu_transaksi", "insert_on_dtm"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def dedupe(df, exclude):
        keep_cols = [c for c in df.columns if c not in exclude]
        return df.drop_duplicates(subset=keep_cols, keep="last")

    # master tables
    for table in master_tables:
        df = pd.read_sql_table(table, engine_raw)
        df = stage_transform_table(df)

        rename_map = {
            # Shelter corridor only
            "shelter_name_var": "shelter_name",
        }

        df = df.rename(columns=rename_map)
        df = drop_metadata(df)

        if table == "realisasi_bus":
            df["tanggal_realisasi"] = pd.to_datetime(
                df.get("tanggal_realisasi"), errors="coerce"
            ).dt.date

        df = clean_common(df)

        if table == "realisasi_bus":
            df = dedupe(df, {"tanggal_realisasi"})

        upsert_dataframe(
            df,
            target_table=f"staging.{table}",
            pk_columns=primary_keys[table],
            postgres_conn_id="pg_staging",
        )

    # transaksi tables
    exclude_cols = {"uuid", "waktu_transaksi", "insert_on_dtm"}

    for table in transaksi_tables:
        df = pd.read_sql_table(table, engine_raw)
        df = stage_transform_table(df)

        rename_map = {
            # Shared
            "waktu_transaksi": "waktu_transaksi",
            "card_number_var": "card_number",
            "card_type_var": "card_type",
            "balance_before_int": "balance_before",
            "fare_int": "fare",
            "balance_after_int": "balance_after",
            "transcode_txt": "transcode",
            "gate_in_boo": "gate_in",
            "p_latitude_flo": "latitude",
            "p_longitude_flo": "longitude",
            "status_var": "status",
            "free_service_boo": "free_service",
            "insert_on_dtm": "insert_on_dtm",
            # Bus only
            "armada_id_var": "armada_id",
            "no_body_var": "no_body",
            # Halte only
            "shelter_name_var": "shelter_name",
            "terminal_name_var": "terminal_name",
        }

        df = apply_dtype_mapping(df)
        df = df.rename(columns=rename_map)
        df = drop_metadata(df)
        df = convert_datetimes(df)
        df = clean_common(df)
        df = dedupe(df, exclude_cols)

        upsert_dataframe(
            df,
            target_table=f"staging.{table}",
            pk_columns="uuid",
            postgres_conn_id="pg_staging",
        )

        logging.info(f"Staged {len(df)} rows → staging.{table}")

    logging.info("TRANSFORM complete")


def load(**context):
    logging.info("LOAD: Build dimensions, facts, cube, and reports")

    build_dimensions()
    build_facts()
    build_cube()

    export_report_to_csv(postgres_conn_id="pg_dw")

    logging.info("LOAD complete")


with DAG(
    dag_id="tj_transaction_pipeline",
    start_date=days_ago(1),
    schedule_interval="0 7 * * *",
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)

    transform_task = PythonOperator(task_id="transform", python_callable=transform)

    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task
