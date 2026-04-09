-- init-postgres.sql
-- Создание трёх баз данных и пользователей для ducklake-in-practice.
-- Выполняется при первом запуске postgres контейнера.

-- ─── DuckLake catalog ────────────────────────────────────────────────
CREATE USER ducklake WITH PASSWORD 'ducklake_secret_change_me';
CREATE DATABASE ducklake_catalog OWNER ducklake;
GRANT ALL PRIVILEGES ON DATABASE ducklake_catalog TO ducklake;

-- ─── Airflow metadata ────────────────────────────────────────────────
CREATE USER airflow WITH PASSWORD 'airflow_secret_change_me';
CREATE DATABASE airflow_metadata OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow_metadata TO airflow;

-- ─── Superset app db ─────────────────────────────────────────────────
CREATE USER superset WITH PASSWORD 'superset_secret_change_me';
CREATE DATABASE superset_appdb OWNER superset;
GRANT ALL PRIVILEGES ON DATABASE superset_appdb TO superset;
