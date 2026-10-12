#!/bin/bash
set -euo pipefail

for f in /docker-entrypoint-initdb.d/migrations/*.up.sql; do
  echo "Applying migration $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
done

for f in /docker-entrypoint-initdb.d/seeds/*.sql; do
  echo "Applying seed $f"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$f"
done
