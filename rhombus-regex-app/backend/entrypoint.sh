#!/bin/sh
set -e

echo "Waiting for Postgres at $POSTGRES_HOST:$POSTGRES_PORT..."
until python -c "
import socket, os, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(1)
try:
    s.connect((os.environ['POSTGRES_HOST'], int(os.environ.get('POSTGRES_PORT', 5432))))
except Exception:
    sys.exit(1)
"; do
  sleep 1
done
echo "Postgres is up."

python manage.py migrate --noinput

exec "$@"
