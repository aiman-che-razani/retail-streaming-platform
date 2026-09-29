#!/usr/bin/env bash
# Least-privilege roles (runs once at first start). Passwords come from the environment.
#   spark_writer   : DML on realtime.* plus CREATE for per-batch staging tables
#   grafana_reader : SELECT only
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v writer_pw="$POSTGRES_SPARK_PASSWORD" -v reader_pw="$POSTGRES_GRAFANA_PASSWORD" <<'SQL'
CREATE ROLE spark_writer LOGIN PASSWORD :'writer_pw';
CREATE ROLE grafana_reader LOGIN PASSWORD :'reader_pw';

GRANT CONNECT ON DATABASE :"DBNAME" TO spark_writer, grafana_reader;
GRANT USAGE, CREATE ON SCHEMA realtime TO spark_writer;
GRANT USAGE ON SCHEMA realtime TO grafana_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA realtime TO spark_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA realtime TO grafana_reader;
GRANT EXECUTE ON FUNCTION realtime.purge_old_windows(INTERVAL) TO spark_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA realtime GRANT SELECT ON TABLES TO grafana_reader;
SQL
