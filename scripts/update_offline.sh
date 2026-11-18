#!/usr/bin/env bash
# scripts/update_offline.sh — deploy the app when Docker Hub cannot be reached from the server.
#
# Why (2026-11-19): update.sh failed at "FROM python:3.11-slim": auth.docker.io timed out from the
# LAUMC server, and the base image is no longer stored there, so no normal build can start.
# This builds the new app image ON TOP OF THE IMAGE RUNNING NOW: every Python package already in it
# stays, pip adds only what requirements.txt has new, and the code is copied in. Nothing is pulled
# from Docker Hub. If PyPI cannot be reached either, the image is built without the new packages
# (Report AI then uses its fallback forecast until a normal update.sh), and the script says so.
#
# Run on the server:   cd /opt/rayd && sudo git fetch origin && sudo git reset --hard origin/LAUMC \
#                      && sudo bash scripts/update_offline.sh
# Roll back:           sudo docker tag <image>:before-offline <image> && sudo docker compose up -d rayd-app
#                      (the script prints the exact lines at the end)
#
# Only rayd-app is rebuilt. The app applies pending migrations itself when it starts (db_migrations,
# with its lock). The next normal update.sh (once Docker Hub answers) rebuilds everything from scratch.
set -euo pipefail

CONTAINER=rayd_service
cd "$(dirname "$0")/.."

IMAGE=$(docker inspect "$CONTAINER" --format '{{.Config.Image}}')
NAME=${IMAGE%%:*}
echo "[INFO]  Running image: $IMAGE"

# keep what runs now, for a roll back; the build starts from this copy
docker tag "$IMAGE" "$NAME:before-offline"

# inside the build context: the classic builder wants its Dockerfile there
DOCKERFILE=Dockerfile.offline.tmp
trap 'rm -f "$DOCKERFILE"' EXIT
cat > "$DOCKERFILE" <<EOF
FROM $NAME:before-offline
WORKDIR /app
COPY requirements.txt /app/requirements.txt
# packages already installed are kept; only new ones are downloaded
RUN pip install --no-cache-dir --timeout 60 --retries 5 -r /app/requirements.txt \\
    && echo "RAYD_PACKAGES=complete" > /app/.offline_build \\
    || echo "RAYD_PACKAGES=incomplete" > /app/.offline_build
COPY . /app
RUN chmod +x /app/scripts/entrypoint.sh
EOF

# the classic builder uses the local base image without asking any registry
echo "[INFO]  Building $IMAGE on top of $NAME:before-offline (no Docker Hub)..."
DOCKER_BUILDKIT=0 docker build -f "$DOCKERFILE" -t "$IMAGE" .

PACKAGES=$(docker run --rm --entrypoint cat "$IMAGE" /app/.offline_build 2>/dev/null || echo "RAYD_PACKAGES=unknown")
echo "[INFO]  $PACKAGES"
if [ "$PACKAGES" != "RAYD_PACKAGES=complete" ]; then
    echo "[WARN]  PyPI was not reachable: new Python packages are missing (Report AI uses its fallback forecast)."
fi

echo "[INFO]  Swapping the app container onto the new image..."
docker compose up -d --no-build rayd-app

for i in $(seq 1 30); do
    state=$(docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null || echo missing)
    [ "$state" = "running" ] && break
    sleep 2
done
echo "[OK]    $CONTAINER is $state."
docker exec "$CONTAINER" python -c "import prophet; print('[OK]    prophet', prophet.__version__)" 2>/dev/null \
    || echo "[WARN]  prophet is not installed in the new image."
echo
echo "Roll back if needed:"
echo "  sudo docker tag $NAME:before-offline $IMAGE && sudo docker compose up -d --no-build rayd-app"
