#!/bin/bash
set -e

# Ensure POSTGRES_USER is set, default to 'postgres' if not
POSTGRES_USER=${POSTGRES_USER:-postgres}

# List of databases to create
DATABASES=("airflow" "warehouse")

# Loop through the database list and create each one
for DB in "${DATABASES[@]}"; do
  echo "Creating database: $DB"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE $DB;
EOSQL
done