#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER test_user;
	CREATE DATABASE tasks;
	GRANT ALL PRIVILEGES ON DATABASE tasks TO test_user;
EOSQL
