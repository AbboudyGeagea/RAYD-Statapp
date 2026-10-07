# CRN gateway: runs on the DMZ VM, behind a TLS reverse proxy on 443.
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY templates ./templates
COPY static ./static
RUN useradd --system --uid 10001 gateway && mkdir -p /data && chown gateway /data
USER gateway
EXPOSE 8443
CMD ["gunicorn", "--workers", "2", "--bind", "0.0.0.0:8443", "--access-logfile", "-", "app:app"]
