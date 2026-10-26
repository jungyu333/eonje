set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname postgres \
  -v eonje_pw="$EONJE_DB_PASSWORD" \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" << 'SQL'
CREATE ROLE eonje LOGIN PASSWORD :'eonje_pw';
CREATE DATABASE eonje OWNER eonje;

CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;
SQL
