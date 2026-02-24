FROM python:3.10-slim

# 1. Install system dependencies
# Added 'libaio-dev' which replaces 'libaio1' in newer Debian versions
RUN apt-get update && apt-get install -y \
    libaio-dev \
    libpq-dev \
    gcc \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Oracle Instant Client
WORKDIR /opt/oracle
RUN curl -o instantclient.zip https://download.oracle.com/otn_pub/otl/instantclient/191000/instantclient-basic-linux.x64-19.10.0.0.0dbru.zip && \
    unzip instantclient.zip && \
    rm instantclient.zip && \
    # Using a wildcard in case the folder name slightly differs
    echo /opt/oracle/instantclient_19_10 > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig

ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_19_10:$LD_LIBRARY_PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "app:create_app()"]
