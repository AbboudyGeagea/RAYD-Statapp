# Use a Python 3.11 slim image for a smaller footprint
FROM python:3.11-slim

# 1. Install system dependencies
#
# HL7 BRANCH: the Oracle Instant Client and its libaio dependencies are gone. The
# image cannot reach an Oracle database even if code tried to — that absence is the
# point, not an oversight. Removed with it: libaio1t64, libaio-dev, and the
# libaio.so.1 SONAME symlink the 21.x client needed on Debian's time_t transition.
#
# ALSO GONE: gcc, libpq-dev and wget, none of which this image needs.
#
# They were there to build psycopg2 from source — but requirements.txt pins
# psycopg2-BINARY, a manylinux wheel that ships its own statically-linked libpq and
# compiles nothing. Every other dependency (pandas, scikit-learn, cryptography,
# psutil) also resolves to a cp311 wheel. So the image was installing a C toolchain
# it never invoked. wget existed only to download the Oracle client.
#
# --no-install-recommends matters as much as the removals: without it apt pulled
# recommended extras for the toolchain, turning this into 75 packages and 263 MB
# installed, including libssl-dev and manpages-dev. That unpack step is also where
# the build kept dying with "cannot allocate memory" on a host with 931 GB of disk
# and 6.7 GiB of RAM free, which is a strong hint the step was doing far more work
# than anything here required.
#
# If a future dependency genuinely needs to compile, add build-essential to a
# builder stage rather than reinstating a compiler in the runtime image.
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    curl \
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
