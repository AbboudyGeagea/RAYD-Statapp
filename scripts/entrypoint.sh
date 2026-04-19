#!/bin/bash
set -e

# Start Gunicorn (exec replaces shell so signals propagate correctly)
exec gunicorn \
    --bind 0.0.0.0:8080 \
    --workers 1 \
    --threads 4 \
    --timeout 300 \
    "app:create_app()"
