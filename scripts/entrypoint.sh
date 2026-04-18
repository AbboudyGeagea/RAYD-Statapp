#!/bin/bash
set -e

# Dump environment variables for the cron job (cron runs with empty env)
printenv | grep -v "^_" | sed 's/^\([^=]*\)=\(.*\)$/export \1="\2"/' > /etc/cron_env

# Ensure cron log file exists
touch /var/log/analytics_cron.log

# Start cron daemon
service cron start

echo "[entrypoint] cron started — analytics job scheduled at 05:15 daily"

# Start Gunicorn (exec replaces shell so signals propagate correctly)
exec gunicorn \
    --bind 0.0.0.0:8080 \
    --workers 1 \
    --threads 4 \
    --timeout 300 \
    "app:create_app()"
