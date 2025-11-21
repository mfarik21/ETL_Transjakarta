#!/bin/bash

set -e

PGHOST="postgres"
PGUSER="airflow"
PGDATABASE="transjakarta"

SQL_DIR="/opt/airflow/sql_scripts"

echo "Creating schemas if not exists..."
psql -h $PGHOST -U $PGUSER -d $PGDATABASE <<EOF
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
EOF

echo "Running RAW SQL..."
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f "$SQL_DIR/raw.sql"

echo "Running STAGING SQL..."
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f "$SQL_DIR/staging.sql"

echo "Running DW SQL..."
psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f "$SQL_DIR/dw.sql"

echo "All schemas and tables created successfully!"
