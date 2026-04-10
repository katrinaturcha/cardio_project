#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE ${APP_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${APP_DB}')\gexec

    SELECT 'CREATE DATABASE ${AIRFLOW_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${AIRFLOW_DB}')\gexec

    SELECT 'CREATE DATABASE ${SUPERSET_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${SUPERSET_DB}')\gexec
EOSQL