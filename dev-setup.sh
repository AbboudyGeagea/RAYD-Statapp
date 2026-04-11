#!/bin/bash
# ─────────────────────────────────────────────────────────
#  RAYD StatsApp — Laptop Dev Setup (one command)
# ─────────────────────────────────────────────────────────
#  Usage:  ./dev-setup.sh
#  Opens:  http://localhost:6661
# ─────────────────────────────────────────────────────────
set -e

echo "══════════════════════════════════════════"
echo "  RAYD StatsApp — Dev Environment Setup"
echo "══════════════════════════════════════════"

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running. Start Docker Desktop first."
    exit 1
fi

# Create .env if missing
if [ ! -f .env ]; then
    echo "Creating default .env ..."
    cat > .env <<'EOF'
TZ=Asia/Beirut
SECRET_KEY=dev-secret-key
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_pass
POSTGRES_HOST=db
POSTGRES_PORT=5432
POSTGRES_DB=etl_db
MASTER_ADMIN_KEY=admin123
PATIENT_PORTAL_ENABLED=false
LIVE_FEED_ENABLED=true
BITNET_ENABLED=false
EOF
    echo "  .env created with dev defaults."
fi

# Clean previous containers/volumes if user wants fresh data
if docker volume ls -q | grep -q "statsapp_postgres_data\|rayd-statapp_postgres_data"; then
    echo ""
    read -p "Existing database found. Reset with fresh data? [y/N] " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Removing old containers and data..."
        docker-compose -f docker-compose.dev.yml down -v 2>/dev/null || true
    fi
fi

echo ""
echo "Building and starting containers..."
docker-compose -f docker-compose.dev.yml up -d --build

echo ""
echo "Waiting for database..."
for i in $(seq 1 30); do
    if docker exec rayd_db pg_isready -U etl_user -d etl_db > /dev/null 2>&1; then
        echo "  Database ready."
        break
    fi
    sleep 1
done

echo ""
echo "══════════════════════════════════════════"
echo "  READY!"
echo "  App:  http://localhost:6661"
echo "  DB:   localhost:5432 (etl_user/etl_pass)"
echo "══════════════════════════════════════════"
echo ""
echo "Commands:"
echo "  docker-compose -f docker-compose.dev.yml logs -f    # view logs"
echo "  docker-compose -f docker-compose.dev.yml down       # stop"
echo "  docker-compose -f docker-compose.dev.yml down -v    # stop + wipe DB"
