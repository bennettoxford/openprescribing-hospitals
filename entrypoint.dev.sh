#!/usr/bin/env bash
set -euo pipefail

mkdir -p assets/dist

python manage.py migrate
python manage.py createcachetable
exec python manage.py runserver 0.0.0.0:8000
