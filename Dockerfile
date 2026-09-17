# Use a Python 3.11 slim image for a smaller footprint
FROM python:3.11-slim

# 1. Install system dependencies
# libpq-dev is required for the PostgreSQL connection.
#
# HL7 BRANCH: the Oracle Instant Client and its libaio dependencies are gone. The
# image cannot reach an Oracle database even if code tried to — that absence is the
# point, not an oversight. Removed with it: libaio1t64, libaio-dev, and the
# libaio.so.1 SONAME symlink the 21.x client needed on Debian's time_t transition.
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    tzdata \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

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
# LD_LIBRARY_PATH and ORACLE_CLIENT_LIB_DIR removed with the Instant Client.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 6. Expose the internal port (Nginx will handle 443 externally)
EXPOSE 8080

# 7. Start via entrypoint (cron daemon + gunicorn)
CMD ["/app/scripts/entrypoint.sh"]
