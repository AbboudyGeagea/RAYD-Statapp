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
    && rm -rf /var/lib/apt/lists/*

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy requirements first to leverage Docker cache
# Ensure 'apscheduler' and 'gunicorn' are in your requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy the rest of the application code
COPY . .

# 5. Set Environment Variables
# LD_LIBRARY_PATH must point to the folder we map in docker-compose.yml
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_19_19

# 6. Expose the internal port (Nginx will handle 443 externally)
EXPOSE 8080

# 7. Start the application using Gunicorn
# We use --workers 1 to prevent multiple 5 AM ETL triggers
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "120", "app:create_app()"]
