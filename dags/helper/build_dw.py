# helper/build_dw.py

from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.utils import upsert_dataframe
import pandas as pd
import logging


def build_dimensions(postgres_conn_id="pg_dw"):
    pg = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = pg.get_sqlalchemy_engine()

    dim_sources = {
        "dim_route": {
            "source": "staging.routes",
            "natural_key": "route_code",
            "columns": ["route_code", "route_name"],
        },
        "dim_shelter": {
            "source": "staging.shelter_corridor",
            "natural_key": "shelter_name",
            "columns": ["shelter_name", "corridor_name"],
        },
        "dim_bus": {
            "source": "staging.realisasi_bus",
            "natural_key": "bus_body_no",
            "columns": ["bus_body_no", "rute_realisasi"],
        },
    }

    for dim_table, cfg in dim_sources.items():
        df = pd.read_sql(
            f"SELECT {','.join(cfg['columns'])} FROM {cfg['source']}", engine
        )

        # dedupe natural key
        df = df.drop_duplicates(subset=[cfg["natural_key"]])

        # use natural key for merge/upsert
        upsert_dataframe(
            df=df,
            target_table=f"dw.{dim_table}",
            pk_columns=cfg["natural_key"],
            postgres_conn_id=postgres_conn_id,
        )

    logging.info("Dimension tables built.")


def build_facts(postgres_conn_id="pg_dw"):
    pg = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = pg.get_sqlalchemy_engine()

    # =============================================================
    # 1. Load staging tables
    # =============================================================
    df_bus = pd.read_sql("SELECT * FROM staging.transaksi_bus", engine)
    df_halte = pd.read_sql("SELECT * FROM staging.transaksi_halte", engine)

    df_realisasi = pd.read_sql(
        "SELECT bus_body_no, rute_realisasi AS route_code, tanggal_realisasi FROM staging.realisasi_bus",
        engine,
    )

    df_corr = pd.read_sql(
        "SELECT shelter_name, corridor_code AS route_code FROM staging.shelter_corridor",
        engine,
    )

    df_bus["source"] = "bus"
    df_halte["source"] = "halte"

    # =============================================================
    # 2. Standardize columns
    # =============================================================
    df_bus = df_bus.rename(columns={"no_body": "bus_body_no"}).assign(
        shelter_name=None, terminal_name=None
    )

    df_halte = df_halte.assign(bus_body_no=None, armada_id=None)

    # =============================================================
    # 3. Align & combine
    # =============================================================
    common_cols = [
        "uuid",
        "waktu_transaksi",
        "bus_body_no",
        "armada_id",
        "shelter_name",
        "terminal_name",
        "card_number",
        "card_type",
        "fare",
        "gate_in",
        "latitude",
        "longitude",
        "free_service",
        "status",
        "source",
    ]

    df = pd.concat([df_bus[common_cols], df_halte[common_cols]], ignore_index=True)

    # =============================================================
    # 4. Map route_code for both BUS and HALTE
    # =============================================================

    # (A) For BUS → join to realisasi_bus using bus_body_no AND waktu_transaksi ~ tanggal_realisasi
    
    # Create a date column from timestamp
    df["date_transaksi"] = df["waktu_transaksi"].dt.date

    # Merge BUS with realisasi_bus on bus_body_no + date
    df = df.merge(
        df_realisasi,
        left_on=["bus_body_no", "date_transaksi"],
        right_on=["bus_body_no", "tanggal_realisasi"],
        how="left",
    )

    # Drop helper column
    df = df.drop(columns=["date_transaksi"])


    # (B) For HALTE → join on shelter_name
    df = df.merge(df_corr, on="shelter_name", how="left", suffixes=("", "_halte"))

    # route_code priority: BUS → realisasi, HALTE → corridor
    df["route_code"] = df["route_code"].fillna(df["route_code_halte"])
    df = df.drop(columns=["route_code_halte", "tanggal_realisasi"])

    # =============================================================
    # 5. Load dimensions
    # =============================================================
    dim_route = pd.read_sql(
        "SELECT id AS dim_route_id, route_code FROM dw.dim_route", engine
    )
    dim_shelter = pd.read_sql(
        "SELECT id AS dim_shelter_id, shelter_name FROM dw.dim_shelter", engine
    )
    dim_bus = pd.read_sql(
        "SELECT id AS dim_bus_id, bus_body_no FROM dw.dim_bus", engine
    )

    # =============================================================
    # 6. Join dimension keys
    # =============================================================
    df = df.merge(dim_bus, on="bus_body_no", how="left")
    df = df.merge(dim_route, on="route_code", how="left")
    df = df.merge(dim_shelter, on="shelter_name", how="left")

    # =============================================================
    # 7. Final fact record
    # =============================================================
    fact_df = df[
        [
            "uuid",
            "waktu_transaksi",
            "dim_route_id",
            "dim_shelter_id",
            "dim_bus_id",
            "card_number",
            "card_type",
            "fare",
            "gate_in",
            "latitude",
            "longitude",
            "free_service",
            "status",
            "source",
        ]
    ]

    # =============================================================
    # 8. Upsert into fact table
    # =============================================================
    upsert_dataframe(
        df=fact_df,
        target_table="dw.fact_transaction",
        pk_columns=["uuid"],
        postgres_conn_id=postgres_conn_id,
    )

    logging.info(
        "Fact tables built successfully"
    )


def build_cube(
    postgres_conn_id="pg_dw", sql_path="/opt/airflow/sql_scripts/build_cube.sql"
):
    pg = PostgresHook(postgres_conn_id=postgres_conn_id)

    with pg.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE dw.cube_trip_transaction")
            cur.execute(open(sql_path).read())
        conn.commit()

    logging.info("Cube built.")
