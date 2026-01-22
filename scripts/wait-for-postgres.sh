#!/bin/sh

set -e

host="$1"
shift

echo "Waiting for PostgreSQL at $host:5432..."

while ! nc -z "$host" 5432; do
  echo "PostgreSQL unavailable, waiting 1 second..."
  sleep 1
done

echo "PostgreSQL is up! Continuing..."