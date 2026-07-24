# Use a Python 3.11 slim image for a smaller footprint
FROM python:3.11-slim

# 1. Install system dependencies
# libaio1 and libaio-dev are required for the Oracle Instant Client
# libpq-dev is required for the PostgreSQL connection
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    libaio1t64 \
    libaio-dev \
    tzdata \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    # Oracle Instant Client 21.x links against libaio.so.1. On Debian's 64-bit
    # time_t transition the runtime ships as libaio.so.1t64, so create the legacy
    # SONAME symlink the client expects when it is missing. No-op (skipped) on
    # images that already provide libaio.so.1, so this stays safe across bases.
    && if [ ! -e /usr/lib/x86_64-linux-gnu/libaio.so.1 ] \
       && [ -e /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then \
         ln -s libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1; \
       fi \
    && ldconfig

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy requirements first to leverage Docker cache
# Ensure 'apscheduler' and 'gunicorn' are in your requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the rest of the application code
COPY . .

# 4b. Install entrypoint
RUN chmod +x /app/scripts/entrypoint.sh

# 5. Set Environment Variables
# LD_LIBRARY_PATH must point to the folder we map in docker-compose.yml
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_13
# Where db.init_oracle_thick_mode() loads the Oracle Instant Client from.
# Override to relocate the client; must match the bind mount in docker-compose.yml.
ENV ORACLE_CLIENT_LIB_DIR=/opt/oracle/instantclient_21_13

# 6. Expose the internal port (Nginx will handle 443 externally)
EXPOSE 8080

# 7. Start via entrypoint (cron daemon + gunicorn)
CMD ["/app/scripts/entrypoint.sh"]
