# RAYD Statistics Application — Installation & Deployment Guide

**Version:** 1.0  
**Date:** September 2026  
**Audience:** Implementation Engineers (ATH)  
**Purpose:** Complete offline installation and configuration for radiology statistics platform

---

## Table of Contents

1. [Overview](#overview)
2. [System Prerequisites](#system-prerequisites)
3. [Pre-Installation Checklist](#pre-installation-checklist)
4. [Environment Configuration](#environment-configuration)
5. [SSL Certificate Generation](#ssl-certificate-generation)
6. [Docker Build & Deployment](#docker-build--deployment)
7. [Initial User Setup](#initial-user-setup)
8. [HL7 Mapping & Configuration](#hl7-mapping--configuration)
9. [HL7 Connectivity Testing](#hl7-connectivity-testing)
10. [Data Ingestion Verification](#data-ingestion-verification)
11. [Troubleshooting](#troubleshooting)
12. [Post-Deployment Checklist](#post-deployment-checklist)
13. [Capacity & Performance](#capacity--performance)

---

## Overview

RAYD is a **HL7-native radiology statistics platform** built on Docker. It receives HL7 messages (ADT, ORM, ORU) from hospital systems over MLLP (Minimal Lower Layer Protocol) on port 6661, processes them, and serves analytics dashboards via HTTPS.

### Architecture

```
                    PACS/HIS/RIS (Remote)
                            |
                      HL7 Messages (MLLP)
                            |
                            v
          ┌─────────────────────────────────┐
          │   rayd_proxy (nginx)             │
          │   Port 443 (HTTPS)               │
          └──────────┬──────────────────────┘
                     |
          ┌──────────v──────────────────────┐
          │ rayd_service (Flask + Gunicorn)  │
          │ Port 6661 (HL7/MLLP Listener)   │
          │ Port 8080 (Internal HTTP)        │
          └──────────┬──────────────────────┘
                     |
        ┌────────────┴──────────────┐
        |                           |
    ┌───v────────────┐  ┌──────────v──────┐
    │ rayd_db        │  │ rayd_nlp         │
    │ PostgreSQL 15  │  │ NLP Worker       │
    │ Port 5432      │  │ (medspaCy)       │
    └────────────────┘  └──────────────────┘
```

**Four Containers:**
- **rayd_proxy** — nginx, TLS termination, reverse proxy
- **rayd_service** — Flask application + APScheduler, HL7 listener
- **rayd_nlp** — medspaCy worker (NLP processing)
- **rayd_db** — PostgreSQL database (persistent volume)

---

## System Prerequisites

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **CPU** | 4 cores | 8+ cores |
| **RAM** | 8 GB | 16+ GB |
| **Disk** | 50 GB | 100+ GB (SSD) |
| **Network** | 100 Mbps | 1 Gbps |

### Software Requirements

| Software | Version | Installation |
|----------|---------|--------------|
| **Docker Engine** | 20.10+ | https://docs.docker.com/engine/install/ |
| **Docker Compose** | 2.0+ | Included with Docker Desktop or `docker-compose-plugin` |
| **Linux** | Ubuntu 20.04 LTS+ | Tested on Ubuntu 20.04, 22.04 LTS |
| **OpenSSL** | 1.1.1+ | Pre-installed on most Linux systems |
| **Python 3** | 3.8+ | Required on the host only for certificate/license generation |

### Network Requirements

| Port | Protocol | Purpose | Source |
|------|----------|---------|--------|
| **6661** | TCP | HL7/MLLP Listener | Hospital PACS/HIS/RIS (restricted by IP whitelist) |
| **443** | TCP | HTTPS Web UI | End users (anywhere) |
| **80** | TCP | HTTP Redirect | Optional; recommend disabling if port in use |
| **5432** | TCP | PostgreSQL | Internal container network only; exposed for debugging |

### Firewall Configuration

Before starting, coordinate with your network team:

```bash
# Allow PACS/HIS/RIS to reach the server
ufw allow from 10.20.30.40/32 to any port 6661 proto tcp comment "PACS sender"
ufw allow from 10.20.30.41/32 to any port 6661 proto tcp comment "HIS/Mirth sender"

# Allow end users to the web UI
ufw allow 443/tcp comment "HTTPS UI"

# Optional: restrict 443 to corporate networks only
ufw allow from 10.0.0.0/8 to any port 443 proto tcp comment "Corporate HTTPS"
```

---

## Pre-Installation Checklist

**Before you begin, verify:**

- [ ] Server is Ubuntu 20.04 LTS or later (or compatible Linux distribution)
- [ ] Docker and Docker Compose are installed and working (`docker --version`, `docker compose version`)
- [ ] You have sudo/root access on the server
- [ ] At least 50 GB free disk space (`df -h`)
- [ ] All required ports (6661, 443, 5432) are available (`ss -tlnp`)
- [ ] Network connectivity to hospital PACS/HIS/RIS is confirmed
- [ ] Time zone on server is set correctly (`timedatectl`)
- [ ] SSL certificate hostname is decided (FQDN, e.g., `rayd.hospital.local`)
- [ ] License tier has been determined (Essential, Professional, or Enterprise)
- [ ] Super User (SU) account details are ready (password policy: 12+ chars)
- [ ] Master admin key for user registration has been generated or will be auto-generated
- [ ] IP addresses of all HL7 senders (PACS, HIS, RIS) are documented

---

## Environment Configuration

### Step 1: Clone the Repository

```bash
# If you haven't already
cd /opt  # or your chosen deployment directory
git clone https://github.com/intermedic/RAYD-Statapp.git
cd RAYD-Statapp
```

### Step 2: Create .env File

The `.env` file defines database credentials, secrets, and feature flags. Create it from the provided example:

```bash
cp .env.example .env
nano .env   # or your preferred editor
```

**Minimal .env configuration:**

```ini
# ── Core Secrets ─────────────────────────────────────────────
SECRET_KEY=c0f1a2b3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1
MASTER_ADMIN_KEY=a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6

# ── Database ─────────────────────────────────────────────────
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=SecureCrynBabe
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=etl_db

# ── Time Zone ────────────────────────────────────────────────
TZ=Asia/Beirut

# ── Features ─────────────────────────────────────────────────
LIVE_FEED_ENABLED=true

# ── HL7 Security (source IP whitelist) ───────────────────────
HL7_ALLOWED_IPS=10.20.30.40 10.20.30.41
HL7_BIND_ADDR=0.0.0.0

# ── Optional: Log Directory ──────────────────────────────────
# Uncomment to log to ./logs instead of /opt/rayd/logs
# RAYD_LOG_DIR=./logs
```

### Step 3: Adjust Values

| Variable | Value | Notes |
|----------|-------|-------|
| `POSTGRES_PASSWORD` | Strong password (20+ chars) | Write it down securely; you'll need it for backups |
| `MASTER_ADMIN_KEY` | `openssl rand -hex 16` | Allows first super user registration; not needed after setup |
| `HL7_ALLOWED_IPS` | Space or comma-separated IPs | PACS, HIS, RIS sender addresses; **required before HL7 firewall setup** |
| `TZ` | Your timezone | Use IANA timezone (Asia/Beirut, US/Eastern, etc.) |

**Example: Secure password generation**

```bash
# Generate a strong database password
openssl rand -base64 20
# Output: 8zR5+kL9X2mQ1vZ/nY3pF4wJ8sH6uD

# Generate a master admin key
openssl rand -hex 16
# Output: a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6
```

---

## SSL Certificate Generation

RAYD requires HTTPS. The `install.sh` script automates this, but here's what happens:

### Automated Generation (Recommended)

When you run `install.sh` (Step 7), it will:
1. Generate a **private Certificate Authority (CA)** (once per organization)
2. Create a **server certificate** signed by your CA for your hostname

### Manual Generation (If Needed)

If you need to regenerate certificates before running install.sh:

```bash
# Create certificate directory
mkdir -p ./nginx/certs

# Generate private CA (once per organization)
openssl genrsa -out ./nginx/certs/rayd-ca.key 4096

openssl req -x509 -new -nodes \
    -key  ./nginx/certs/rayd-ca.key \
    -sha256 -days 3650 \
    -out  ./nginx/certs/rayd-ca.crt \
    -subj "/CN=RAYD Internal CA/O=Intermedic/OU=IT" \
    -addext "basicConstraints=critical,CA:true" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"

chmod 600 ./nginx/certs/rayd-ca.key

# Generate server certificate
SERVER_HOSTNAME="rayd.hospital.local"  # Change to your FQDN

openssl genrsa -out ./nginx/certs/privkey.pem 2048
chmod 600 ./nginx/certs/privkey.pem

openssl req -new \
    -key  ./nginx/certs/privkey.pem \
    -out  ./nginx/certs/server.csr \
    -subj "/CN=${SERVER_HOSTNAME}/O=Intermedic/OU=IT"

# Create certificate extension config
cat > ./nginx/certs/server-ext.cnf <<EOF
[ext]
subjectAltName     = DNS:${SERVER_HOSTNAME},IP:127.0.0.1
basicConstraints   = CA:false
keyUsage           = critical,digitalSignature,keyEncryption
extendedKeyUsage   = serverAuth
EOF

# Sign the server certificate with your CA
openssl x509 -req \
    -in      ./nginx/certs/server.csr \
    -CA      ./nginx/certs/rayd-ca.crt \
    -CAkey   ./nginx/certs/rayd-ca.key \
    -CAcreateserial \
    -out     ./nginx/certs/fullchain.pem \
    -days    825 \
    -sha256  \
    -extfile ./nginx/certs/server-ext.cnf \
    -extensions ext

# Cleanup temporary files
rm -f ./nginx/certs/server.csr ./nginx/certs/server-ext.cnf ./nginx/certs/rayd-ca.srl

echo "Certificates generated in ./nginx/certs/"
```

### Client Trust Store Setup

Each user's workstation must trust your private CA. After installation:

**On Windows (as Administrator):**
```powershell
certutil -addstore Root nginx/certs/rayd-ca.crt
# Or: double-click rayd-ca.crt → Install → Trusted Root Certificate Authorities
```

**On macOS:**
```bash
sudo security add-trusted-cert -d -r trustRoot \
    -k /Library/Keychains/System.keychain \
    nginx/certs/rayd-ca.crt
```

**On Linux (Ubuntu/Debian):**
```bash
sudo cp nginx/certs/rayd-ca.crt /usr/local/share/ca-certificates/rayd-ca.crt
sudo update-ca-certificates
```

---

## Docker Build & Deployment

### Step 1: Verify Docker and Compose

```bash
docker --version
docker compose version

# Expected output:
# Docker version 24.0.0+
# Docker Compose version 2.0.0+
```

### Step 2: Build the Application Image

```bash
cd /path/to/RAYD-Statapp
docker compose build
```

This compiles the Flask application and NLP worker. **First build takes 5–10 minutes.**

### Step 3: Start All Containers

```bash
docker compose up -d

# Verify all containers are running
docker compose ps

# Expected output:
# NAME                COMMAND                  SERVICE      STATUS
# rayd_proxy          "nginx -g 'daemon off'\"  nginx        Up (healthy)
# rayd_service        "/app/scripts/entrypo..."rayd-app     Up (healthy)
# rayd_nlp            "python /app/nlp_worke..."rayd-nlp     Up
# rayd_db             "docker-entrypoint.s..."db           Up (healthy)
```

### Step 4: Wait for Database Ready

```bash
# Monitor logs until you see "database ready"
docker compose logs -f db

# Expected (after 10–30 seconds):
# db | [5] PostgreSQL is running...
# db | LOG: database system is ready to accept connections
```

The app won't fully start until the database is healthy (check `docker compose ps` for status).

### Step 5: Verify Web UI Accessibility

```bash
# Get the container's IP (for local testing)
docker inspect rayd_proxy | grep "IPAddress"

# Or test directly
curl -k https://localhost/health 2>/dev/null | head -20

# Expected: HTTP 200 or redirect to /auth
```

---

## Initial User Setup

### Super User (SU) Creation

The default super user is created by `install.sh` with temporary credentials:

| Username | Password | Notes |
|----------|----------|-------|
| `axadmin` | `axilum2000` | **Must change on first login** |

**To manually create the SU if install.sh was skipped:**

```bash
# Connect to the database
docker exec -it rayd_db psql -U etl_user -d etl_db

# Inside psql:
-- Hash the password (example: "MySecurePassword123")
-- Use the Flask werkzeug tool (from inside the container):
docker exec rayd_service python3 -c "
from werkzeug.security import generate_password_hash
print(generate_password_hash('MySecurePassword123', method='pbkdf2:sha256'))
"

# Then insert into users table (copy the hash from above):
INSERT INTO users (username, password_hash, role, email, status, must_change_password, created_at)
VALUES ('axadmin', '<PASTED_HASH_HERE>', 'su', 'admin@hospital.local', 'active', true, NOW());
```

### Implementation User Creation

The Implementation user handles configuration (AE mappings, procedures, etc.). Create via web UI:

1. **Log in as axadmin** (https://server-hostname)
   - Username: `axadmin`
   - Password: `axilum2000`
   - Click **Change Password** when prompted

2. **Navigate to /admin/users**
   - Click **+ New User**
   - Username: `impl_engineer` (example; choose any name)
   - Role: **Implementation**
   - Email: `impl@hospital.local`
   - Status: **Active**
   - Password: Set a strong password (20+ chars; they'll change it on first login)
   - Click **Create**

3. **Share credentials** with the implementation engineer securely

---

## HL7 Mapping & Configuration

HL7 data must be mapped to RAYD's database schema before it can be visualized. This is the Implementation user's primary responsibility.

### Part 1: AE Title → Modality Mapping

**What:** The PACS sends HL7 messages with an AE Title (device identifier, e.g., "CR_01_MAIN"). You must map each AE Title to a standard radiology modality (CR, CT, MRI, XR, etc.).

**Where:** Admin UI → **Mapping Controller** → **AE Title & Modality**

**Steps:**

1. Log in as Implementation user
2. Click **Mapping Controller** (left menu)
3. Click **AE Title & Modality** tab
4. **Add New Mapping:**
   - AE Title: `CR_01_MAIN` (from PACS)
   - Canonical Modality: `CR` (standard code)
   - Daily Capacity (minutes): `480` (8-hour shift = 480 min)
   - Click **Save**

5. Repeat for every AE Title your PACS sends:
   ```
   CR_01_MAIN → CR (480 min)
   CT_01_MAIN → CT (480 min)
   CT_02_VASCULAR → CT (480 min)
   MRI_01_BODY → MRI (420 min)
   US_01_OB → US (480 min)
   ```

### Part 2: Procedure Configuration

**What:** Link procedure codes (from HIS HL7 messages) to standard procedure names and modalities.

**Where:** **Mapping Controller** → **Procedures** tab

**Steps:**

1. Import procedures from CSV or add manually
2. Map HIS procedure codes to standard names:
   ```
   HIS Code  → Standard Name       → Modality
   proc_101  → Chest X-Ray         → XR
   proc_102  → Chest PA/Lateral    → XR
   proc_203  → CT Chest            → CT
   proc_305  → MRI Brain + Contrast → MRI
   ```

3. Set average duration per procedure (for capacity calculations):
   ```
   proc_101  → 10 minutes  (XR is quick)
   proc_203  → 30 minutes  (CT with contrast)
   proc_305  → 45 minutes  (MRI full sequence)
   ```

### Part 3: Capacity Scheduling (Optional)

**What:** Define standard working hours per device to calculate utilization %.

**Where:** **Mapping Controller** → **Device Schedule**

**Steps:**

1. Set weekly schedule per AE Title:
   ```
   CR_01_MAIN:
     Monday–Friday:   08:00–17:00 (540 min)
     Saturday:        08:00–12:00 (240 min)
     Sunday:          Closed
   ```

2. Add exceptions (maintenance, holidays):
   ```
   2026-12-25 (Christmas) → Closed (0 min)
   2026-10-15 (Maintenance) → 4 hours (240 min)
   ```

---

## HL7 Connectivity Testing

### Step 1: Verify Listener is Active

```bash
# Check that port 6661 is listening
docker compose logs rayd-app | grep -i mllp

# Expected output:
# rayd_service | [2024-XX-XX XX:XX:XX] HL7 MLLP Listener started on 0.0.0.0:6661
```

### Step 2: Find Actual Sender IPs

Ask your PACS/HIS/RIS admin for the exact IPs that will send HL7. Or monitor the logs:

```bash
# Start a live tail of the app logs
docker compose logs -f rayd-app

# In a separate terminal, ask the PACS to send a test message
# Then watch the logs for the sender IP
```

### Step 3: Update Firewall Rules

Once you know the sender IPs, update `.env` and apply the firewall:

```bash
# Edit .env and set:
HL7_ALLOWED_IPS=10.20.30.40 10.20.30.41

# Apply firewall (must be root; the script refuses to run if HL7_ALLOWED_IPS is empty)
sudo -E bash scripts/hl7_firewall.sh

# Expected output:
# [hl7_firewall] port=6661 allowed='10.20.30.40 10.20.30.41'
# [hl7_firewall]   allow 10.20.30.40
# [hl7_firewall]   allow 10.20.30.41
# [hl7_firewall] applied. Current RAYD-HL7 rules:
```

### Step 4: Persist Firewall Rules Across Reboot

The firewall rules are NOT persistent by default. To keep them after reboot:

```bash
# Copy the systemd service
sudo cp scripts/rayd-hl7-firewall.service /etc/systemd/system/

# Enable it to run at boot
sudo systemctl enable --now rayd-hl7-firewall

# Verify it's active
sudo systemctl status rayd-hl7-firewall
```

### Step 5: Send Test HL7 Message

Request your PACS/HIS team to send a test ADT or ORU message. You should see it in the logs:

```bash
docker compose logs -f rayd-app | grep -i "hl7\|oru\|adt"

# Expected (example):
# rayd_service | [2024-10-15 10:23:45] HL7 ORU received: accession=2024100512345
# rayd_service | [2024-10-15 10:23:46] NLP task queued for report_id=127
```

---

## Data Ingestion Verification

### Step 1: Check Database for Incoming Data

```bash
# Connect to the database
docker exec -it rayd_db psql -U etl_user -d etl_db

# Inside psql, check for HL7 orders:
SELECT COUNT(*) as order_count FROM hl7_orders;

# Check for HL7 reports (ORU messages):
SELECT COUNT(*) as report_count FROM hl7_oru_reports;

# View recent reports:
SELECT id, accession_number, procedure_name, received_at
  FROM hl7_oru_reports
  ORDER BY received_at DESC
  LIMIT 5;
```

**Expected:** Counts should increase as HL7 messages arrive.

### Step 2: Verify NLP Processing

NLP worker processes ORU reports asynchronously (60-second poll interval):

```bash
# Check NLP worker logs
docker compose logs rayd-nlp

# Expected (every 60 seconds):
# rayd_nlp | [2024-10-15 10:25:00] Polling for unprocessed reports...
# rayd_nlp | [2024-10-15 10:25:05] Processed report_id=127 (affirmed_labels=['Pneumonia', 'Pleural Effusion'])

# Check processed reports in database:
docker exec -it rayd_db psql -U etl_user -d etl_db
SELECT COUNT(*) as analyzed_count FROM hl7_oru_analysis;
SELECT report_id, affirmed_labels, is_critical FROM hl7_oru_analysis LIMIT 5;
```

### Step 3: Test Dashboard Access

1. **Open browser:** https://your-server-hostname
2. **Log in** with a viewer user (or axadmin)
3. **Navigate to Report 22 (Main Statistics)**
   - Should show studies, modalities, physicians
   - Data should increase as HL7 arrives

4. **Navigate to ORU Analytics** (if licensed)
   - Should show critical findings word cloud
   - Should list recent reports

---

## Troubleshooting

### Issue: HTTPS Certificate Error ("Connection Not Secure")

**Symptom:** Browser shows "Your connection is not private" or "NET::ERR_CERT_AUTHORITY_INVALID"

**Root Cause:** Client machine has not trusted the private CA

**Solution:**

1. **Copy the CA certificate to the client**
   ```bash
   scp -r user@server:/path/to/RAYD-Statapp/nginx/certs/rayd-ca.crt .
   ```

2. **Install on client** (see [SSL Certificate Generation](#ssl-certificate-generation) section)

3. **Restart browser** and try again

---

### Issue: HL7 Messages Not Arriving ("Port 6661 Unreachable")

**Symptom:** PACS reports "Connection refused" when sending to port 6661

**Root Cause:** Port not exposed, or firewall blocking

**Solution:**

1. **Verify port is open on the server**
   ```bash
   ss -tlnp | grep 6661
   # Expected: tcp   LISTEN  0  5  0.0.0.0:6661  0.0.0.0:*
   ```

2. **Verify firewall allows the sender IP**
   ```bash
   sudo iptables -L RAYD-HL7 -n
   # Should show the sender IP in the whitelist
   ```

3. **Test from a remote machine**
   ```bash
   nc -zv server-ip 6661
   # If connection refused: firewall is blocking
   ```

4. **Verify HL7_ALLOWED_IPS in .env**
   ```bash
   grep HL7_ALLOWED_IPS .env
   # Should match the actual sender IPs
   ```

---

### Issue: Container Crashes on Start ("rayd_service: exited with code 1")

**Symptom:** `docker compose ps` shows rayd_service as "Exited"

**Solution:**

1. **Check logs**
   ```bash
   docker compose logs rayd-app | tail -50
   ```

2. **Common issues:**
   - Database connection timeout → wait for rayd_db to be healthy
   - Missing SECRET_KEY in .env → add it
   - Python syntax error → check requirements.txt

3. **Restart the service**
   ```bash
   docker compose down
   docker compose up -d
   ```

---

### Issue: Database Fails to Start ("pg_isready failed")

**Symptom:** `docker compose ps` shows rayd_db as "Exited" or "Unhealthy"

**Solution:**

1. **Check logs**
   ```bash
   docker compose logs db | tail -50
   ```

2. **Delete corrupted volume and restart** (WARNING: deletes all data)
   ```bash
   docker compose down
   docker volume rm rayd-statapp_postgres_data  # Adjust project name if needed
   docker compose up -d
   ```

3. **Verify disk space**
   ```bash
   df -h | grep /var/lib/docker
   # Ensure >50GB available
   ```

---

### Issue: Web UI Shows "500 Internal Server Error"

**Symptom:** https://server/ displays error, logs show Python exception

**Solution:**

1. **Check app logs**
   ```bash
   docker compose logs rayd-app | tail -100
   ```

2. **Common issues:**
   - Database schema not initialized → run migrations
   - Missing environment variable → check .env
   - HL7 parsing error → check HL7 format

3. **Restart the app**
   ```bash
   docker compose restart rayd-app
   ```

---

### Issue: ORU Reports Not Processing by NLP Worker

**Symptom:** Reports appear in database, but `hl7_oru_analysis` table stays empty

**Solution:**

1. **Check NLP worker logs**
   ```bash
   docker compose logs rayd-nlp | tail -50
   ```

2. **Restart NLP worker**
   ```bash
   docker compose restart rayd-nlp
   ```

3. **Verify medspaCy model is loaded**
   ```bash
   docker exec rayd-nlp python3 -c "import spacy; nlp=spacy.load('en_core_med7_trf'); print('OK')"
   ```

---

### Issue: High Memory Usage (Swapping/OOM Kills)

**Symptom:** Server becomes sluggish; `docker stats` shows high memory

**Solution:**

1. **Check current usage**
   ```bash
   docker stats
   ```

2. **Increase Docker memory limit** (if running Docker Desktop)
   - Right-click Docker icon → Preferences → Resources → Increase **Memory**

3. **On Linux servers**, increase system resources or add swap:
   ```bash
   free -h
   swapon -s
   # If swap is 0, add 8GB:
   sudo fallocate -l 8G /swapfile
   sudo chmod 600 /swapfile
   sudo mkswap /swapfile
   sudo swapon /swapfile
   ```

---

### Issue: Audit Logs or User Activity Not Recording

**Symptom:** Activity page is empty even after users log in

**Root Cause:** Activity logging table not initialized or disabled

**Solution:**

1. **Verify table exists**
   ```bash
   docker exec rayd_db psql -U etl_user -d etl_db -c "SELECT * FROM active_sessions LIMIT 1;"
   ```

2. **If table missing**, run migrations:
   ```bash
   docker compose down
   docker compose up -d
   # Logs should show migration success
   docker compose logs rayd-app | grep -i migration
   ```

---

## Post-Deployment Checklist

After successful installation, verify all components:

### Application

- [ ] Web UI accessible at https://hostname
- [ ] Login page loads without certificate errors
- [ ] Super User (axadmin) can log in
- [ ] Super User can change password
- [ ] Users → dashboard shows admin/implementation users created
- [ ] Mapping Controller tabs load (AE Titles, Procedures, Scheduling)

### HL7 Listener

- [ ] Port 6661 is listening (`ss -tlnp | grep 6661`)
- [ ] Firewall rules applied to whitelist sender IPs (`sudo iptables -L RAYD-HL7`)
- [ ] Test message received and logged (`docker compose logs rayd-app | grep HL7`)
- [ ] Message appears in database (`docker exec rayd_db psql -U etl_user -d etl_db -c "SELECT COUNT(*) FROM hl7_oru_reports;"`)

### NLP Worker

- [ ] Worker is running (`docker compose ps | grep rayd_nlp`)
- [ ] Logs show periodic polling (`docker compose logs rayd-nlp | tail -20`)
- [ ] ORU reports are analyzed (`SELECT COUNT(*) FROM hl7_oru_analysis;`)
- [ ] No Python errors in logs

### Database

- [ ] All tables exist (`\dt` in psql)
- [ ] Schema is at latest version (check migrations ran)
- [ ] Backups are scheduled (recommend daily, 3-day retention)
- [ ] Database size monitored (`SELECT pg_size_pretty(pg_database_size('etl_db'));`)

### Backups

- [ ] Daily PostgreSQL backups configured (e.g., via cron)
- [ ] Backup retention policy: minimum 7 days
- [ ] Test restore procedure documented
- [ ] Off-site backup copy (if required by policy)

**Example backup script:**
```bash
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
docker exec rayd_db pg_dump -U etl_user -d etl_db \
  | gzip > /backup/etl_db_${DATE}.sql.gz
find /backup -name "etl_db_*.sql.gz" -mtime +7 -delete
```

### Maintenance

- [ ] Log rotation configured (Docker handles via json-file driver)
- [ ] Disk usage monitored
- [ ] Docker system prune schedule set (`docker system prune -a --volumes` monthly)
- [ ] OS security updates applied (especially OpenSSL, curl)
- [ ] Contact info for support team documented

---

## Quick Reference

### Essential Commands

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f rayd-app        # Flask app
docker compose logs -f rayd-nlp        # NLP worker
docker compose logs -f db              # PostgreSQL
docker compose logs -f rayd_proxy      # nginx

# Execute SQL queries
docker exec -it rayd_db psql -U etl_user -d etl_db

# Rebuild after code changes
docker compose build && docker compose up -d

# Check service health
docker compose ps

# Remove stopped containers and unused volumes (monthly)
docker system prune -a --volumes --force

# Connect to app container (for debugging)
docker exec -it rayd_service bash

# Monitor resource usage
docker stats
```

### Useful SQL Queries

```sql
-- Count studies ingested
SELECT COUNT(*) as total_studies FROM hl7_oru_reports;

-- Count processed (NLP) reports
SELECT COUNT(*) as analyzed FROM hl7_oru_analysis;

-- List unprocessed reports
SELECT id, accession_number, received_at
  FROM hl7_oru_reports oru
  WHERE NOT EXISTS (SELECT 1 FROM hl7_oru_analysis a WHERE a.report_id = oru.id)
  ORDER BY received_at DESC
  LIMIT 20;

-- Database size
SELECT pg_size_pretty(pg_database_size('etl_db')) as db_size;

-- Active connections
SELECT count(*) as active_connections FROM pg_stat_activity;
```

---

## Support & Escalation

### Common Issues Escalation Matrix

| Issue | First Try | If Failed |
|-------|-----------|-----------|
| HTTPS certificate error | Client: install CA cert | Network: verify host cert validity |
| Port 6661 unreachable | Update HL7_ALLOWED_IPS | Network team: check firewall rules |
| HL7 messages not parsing | Check HL7 format/encoding | Medical: validate HL7 source |
| Database issues | Check disk space, restart db | DevOps: backup and restore |
| High memory/CPU | Restart containers, check logs | Infrastructure: scale hardware |

---

## Document Version History

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| Sep 2026 | 1.0 | Initial release | Implementation Team |

---

## Capacity & Performance

This section describes the HL7 listener's real-world throughput and capacity limits, based on stress testing.

### Performance Baseline

**Tested on:** Ryzen 7 Ultra (8 cores), 16GB RAM, NVMe SSD  
**Test date:** September 2026  
**Total messages tested:** 660 (100% success rate)

### HL7 Listener Throughput

| Load Level | Concurrent Senders | Throughput | Avg Latency | Status |
|-----------|-------------------|------------|-------------|--------|
| **Light** | 1-2 senders | 10-15 msg/sec | 74-80 ms | Optimal ✅ |
| **Recommended** | 3-5 senders | 40-50 msg/sec | 80-200 ms | Production ✅ |
| **Peak** | 20 concurrent | 62 msg/sec | 226 ms | Burst capacity |
| **Overload** | >25 concurrent | Degraded | 1,000+ ms | Avoid ❌ |

**In practical terms:**
- **Safe sustained:** 40-50 messages/sec (2,400-3,000 msg/min)
- **Peak burst:** 62 messages/sec (3,720 msg/min)
- **Maximum concurrent connections:** 20-25 (beyond this, latency degrades exponentially)

### Typical Hospital Deployment

**Example with 3 PACS systems:**

```
PACS System A: 15 msg/sec peak (morning rush)
PACS System B: 10 msg/sec average
PACS System C: 8 msg/sec average
Total: 33 msg/sec = SAFE (within 40-50 recommended range)
```

For your deployment, calculate peak load:
1. List all HL7 senders (PACS, HIS, RIS systems)
2. Estimate each sender's peak throughput (ask vendor)
3. Total should not exceed 40-50 msg/sec sustained

### Bottleneck Analysis

**Why does performance level off?**

The HL7 listener processes each message through:
1. **Parse** (2-3 ms) - HL7 syntax parsing
2. **Archive** (20-30 ms) - Store raw message in database
3. **RAY7 Screen** (50-100 ms) - **BLOCKING** validation/screening
4. **Persist Verdict** (5-10 ms) - Store screening results
5. **Project** (10-20 ms) - Write to reporting tables (hl7_orders, hl7_oru_reports)
6. **Commit** (5-10 ms) - Transaction commit

**Total per message: ~100-170 ms**

The bottleneck is **RAY7 screening**, which runs **synchronously in the ACK response window**. This is intentional for safety: every message must be validated before acknowledging to the sender.

At 20 concurrent senders, this creates natural queue depth but remains stable. Beyond 25 threads, Python's Global Interpreter Lock (GIL) causes queue backlog, rapidly degrading latency.

### Capacity Planning

**For your server specs (8 cores, 16GB RAM, NVMe):**

| Hardware Component | Utilization | Limit | Notes |
|-------------------|------------|-------|-------|
| CPU | Single-threaded GIL | ~1 core | Screening is CPU-bound parsing |
| RAM | Minimal | <2GB | Stable, no swap observed |
| Database connections | 14/25 max | 25 pool | Adequate headroom |
| Disk I/O | Low | ~50 MB/min | NVMe easily handles |
| Network | Unused | 1Gbps | Not a factor on localhost |

### Monitoring in Production

Monitor these metrics to detect overload:

```bash
# 1. Active DB connections
docker exec rayd_db psql -U etl_user -d etl_db \
  -c "SELECT count(*) FROM pg_stat_activity WHERE datname = 'etl_db';"
# Alert if > 20

# 2. Recent message count
docker exec rayd_db psql -U etl_user -d etl_db \
  -c "SELECT count(*) FROM hl7_archive WHERE received_at > NOW() - INTERVAL '1 minute';"
# Should show incoming message rate

# 3. App logs for ACK times
docker compose logs rayd-app | grep "HL7.*type="
# Watch for "time=XXms" increasing over time
```

**Alert thresholds:**
- DB connections > 20: Investigate sender or slow screening
- ACK latency > 500 ms: System overloaded or slow queries
- Message failure rate > 0.1%: Data quality issue
- Thread count > 30: Stop accepting new connections, coordinate with senders

### Scaling Beyond 62 msg/sec

If your deployment needs higher throughput, these options are available:

**Option 1: Async RAY7 Screening** (2-3x improvement)
- Move RAY7 validation to background job
- Messages briefly unscreened in database
- ACK latency drops to 50 ms, throughput ~150+ msg/sec
- Trade-off: Quarantine rules apply asynchronously

**Option 2: Multi-Process Architecture** (2-4x improvement)
- Replace Python threading with process pool
- Each process has own GIL, true parallelism
- Requires 4-8 cores (you have 8)
- Estimated throughput: 200-250 msg/sec

**Option 3: Connection Pooling** (10-15% improvement)
- Use PgBouncer or pgpool-II for better connection management
- Reduces connection overhead
- Minimal code changes

**Option 4: Hybrid (Recommended)** (3-5x improvement)
- Async RAY7 + multi-process pool
- Highest throughput with moderate complexity
- Estimated: 300+ msg/sec

Contact Intermedic engineering if you need to implement scaling options.

### Success Metrics

Your deployment is successful if:
- All HL7 messages arrive and are archived (0% message loss)
- ACK latency < 500 ms in normal operation
- Database connections never exceed pool size
- No spike in CPU or memory usage during peaks
- Data appears in dashboards within 2-5 seconds of arrival

---

**End of Installation Guide**

For questions or updates, contact your local Intermedic IT support team.
