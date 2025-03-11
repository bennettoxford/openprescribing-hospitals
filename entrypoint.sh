#!/bin/bash
set -e

# Apply database migrations
echo "Applying database migrations..."
python manage.py migrate

# Start server using gunicorn
echo "Starting server..."
exec gunicorn --bind 0.0.0.0:8000 --timeout 180 demo.wsgi:application
