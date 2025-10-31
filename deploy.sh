#!/usr/bin/env bash

# A simple deploy.sh script to deploy the django application
# Future enhancements for production ready deploy
#   Setup health checks and rollbacks to previous commit on failure
#   Provide flags to make migrations, makemigrations, collectstatic optional
#   `systemctl restart` causes downtime   

set -euo pipefail

APP_DIR="/srv/landscape/app"
VENV="/srv/landscape/.venv"
BRANCH="main"
SERVICE_NAME="landscape"
GIT_REMOTE="origin"

echo "[deploy] cd ${APP_DIR}"
cd "${APP_DIR}"

echo "[deploy] git fetch & fast-forward to ${BRANCH}"
git fetch --all --prune
git checkout "${BRANCH}"
git pull --ff-only "${GIT_REMOTE}" "${BRANCH}"

echo "[deploy] sync dependencies (uv, frozen lock)"
"${VENV}/bin/uv" sync --frozen

echo "[deploy] run migrations"
"${VENV}/bin/uv" run python manage.py migrate --noinput

echo "[deploy] collect static"
"${VENV}/bin/uv" run python manage.py collectstatic --noinput

echo "[deploy] restart service: ${SERVICE_NAME}"
sudo systemctl restart "${SERVICE_NAME}"

echo "[deploy] done."
