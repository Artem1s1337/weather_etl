#!/bin/bash
# wait-for-postgres.sh

set -e

host="$1"
shift

echo "Wait for PostgreSQL on $host:5432..."

until PGPASSWORD=$DB_PASSWORD psql -h "$host" -U "$DB_USER" -d "$DB_NAME" -c '\q' 2>/dev/null; do
  echo "PostgreSQL unavailable, wait for 1 sec..."
  sleep 1
done

echo "PostgreSQL avaliable, launch extract..."
