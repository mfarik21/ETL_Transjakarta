import os
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


def export_report_to_csv(
    postgres_conn_id="pg_dw",
    cube_table="dw.cube_trip_transaction",
    output_dir="/opt/airflow/reports",
):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = hook.get_sqlalchemy_engine()

    # ---------------------------------------------------------
    # Create dated folder: /reports/YYYY-MM-DD/
    # ---------------------------------------------------------
    today = datetime.now().strftime("%Y-%m-%d")
    report_dir = f"{output_dir}/{today}"
    os.makedirs(report_dir, exist_ok=True)

    # ---------------------------------------------------------
    # 1. Report by Card Type
    # ---------------------------------------------------------
    query_card = f"""
        SELECT 
            date_key,
            card_type,
            gate_in,
            SUM(total_passenger) AS total_passenger,
            SUM(total_fare) AS total_fare
        FROM {cube_table}
        GROUP BY 1,2,3
        ORDER BY 1,2,3;
    """
    df_card = pd.read_sql(query_card, engine)
    df_card.to_csv(f"{report_dir}/by_card_type.csv", index=False)

    # ---------------------------------------------------------
    # 2. Report by Route (route_code instead of route_id)
    # ---------------------------------------------------------
    query_route = f"""
        SELECT 
            c.date_key,
            r.route_code,
            r.route_name,
            c.gate_in,
            SUM(c.total_passenger) AS total_passenger,
            SUM(c.total_fare) AS total_fare
        FROM {cube_table} c
        LEFT JOIN dw.dim_route r
            ON c.route_id = r.id
        GROUP BY 1,2,3,4
        ORDER BY 1,2,3;
    """
    df_route = pd.read_sql(query_route, engine)
    df_route.to_csv(f"{report_dir}/by_route.csv", index=False)

    # ---------------------------------------------------------
    # 3. Report by Fare
    # ---------------------------------------------------------
    query_fare = f"""
        SELECT 
            date_key,
            fare,
            gate_in,
            SUM(total_passenger) AS total_passenger,
            SUM(total_fare) AS total_fare
        FROM {cube_table}
        GROUP BY 1,2,3
        ORDER BY 1,2,3;
    """
    df_fare = pd.read_sql(query_fare, engine)
    df_fare.to_csv(f"{report_dir}/by_fare.csv", index=False)

    logging.info(f"Reports generated in: {report_dir}")
    return report_dir
