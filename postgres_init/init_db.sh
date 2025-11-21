#!/bin/bash
set -e

echo ">>> Creating database transjakarta..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE transjakarta;
EOSQL

echo ">>> Creating schemas (raw, staging, dw)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d transjakarta <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS dw;
EOSQL

SQL_DIR="/opt/airflow/sql_scripts"

echo ">>> Running RAW SQL..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d transjakarta -f "$SQL_DIR/raw.sql"

echo ">>> Running STAGING SQL..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d transjakarta -f "$SQL_DIR/staging.sql"

echo ">>> Running DW SQL..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d transjakarta -f "$SQL_DIR/dw.sql"

echo ">>> All schemas and tables created successfully!"
