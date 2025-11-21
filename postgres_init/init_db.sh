#!/bin/bash
set -e

echo "Creating database transjakarta..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE transjakarta;
EOSQL

echo "Creating schemas (raw, staging, cube)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d transjakarta <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS dw;
EOSQL

echo "Running schema.sql..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d transjakarta -f /docker-entrypoint-initdb.d/sql_scripts/schema.sql
