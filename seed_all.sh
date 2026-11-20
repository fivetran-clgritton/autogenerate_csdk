#!/bin/bash

REPO=/opt/sdk-test-dbs/connectors
COMPOSE_DIR=/opt/sdk-test-dbs

# Fix ownership so git operations work
chown -R CLAbeel:CLAbeel "$REPO" 2>/dev/null || true

# Pull latest code — discard any local edits (VM is never the source of truth)
cd "$REPO"
git fetch origin main
git checkout -- .
git reset --hard origin/main
echo "Code updated."

cd "$COMPOSE_DIR"

# Install Java for H2/jaydebeapi (idempotent)
if ! command -v java &>/dev/null; then
    echo "Installing Java..."
    apt-get install -y --no-install-recommends default-jre-headless
fi

# Extract Mimer native client lib from the container so mimerpy can load it
MIMER_LIB=/usr/local/lib/libmimerapi.so
if [ ! -f "$MIMER_LIB" ]; then
    echo "Extracting Mimer native lib from container..."
    MIMER_CTR=$(docker ps --filter name=mimer --format '{{.ID}}' | head -1)
    if [ -n "$MIMER_CTR" ]; then
        docker cp "$MIMER_CTR":/usr/lib/libmimerapi.so "$MIMER_LIB" 2>/dev/null || \
        docker cp "$MIMER_CTR":/usr/local/lib/libmimerapi.so "$MIMER_LIB" 2>/dev/null || \
        echo "WARNING: could not find libmimerapi.so inside the Mimer container — Mimer seeding will fail"
        ldconfig 2>/dev/null || true
    else
        echo "WARNING: Mimer container not running — skipping lib extraction"
    fi
fi

# Install missing Python packages
source /opt/sdk-test-dbs/venv/bin/activate
pip install --quiet CUBRIDdb mimerpy 2>&1 | tail -5

# Ensure Exasol and Geode are up (Geode needs REST API flags from updated compose)
docker compose -f /opt/sdk-test-dbs/docker-compose.yml up -d exasol geode 2>&1 | grep -v "already allocated" || true
echo "Waiting 30s for containers to initialize..."
sleep 30

# Reset IRIS _SYSTEM password so programmatic login works
# IRIS Community requires password change on first use; force it via irisdb exec
echo "Resetting IRIS _SYSTEM password..."
echo "IRIS password already changed via portal — skipping reset"

FAILED=()

seed_db() {
    local name=$1
    shift
    echo ""
    echo "=== Seeding $name ==="
    if python /opt/sdk-test-dbs/connectors/seed_data.py "$@" 2>&1; then
        echo "=== $name: SUCCESS ==="
    else
        echo "=== $name: FAILED ==="
        FAILED+=("$name")
    fi
}

seed_db "Exasol"  --db exasol  --host localhost --password exasol
seed_db "IRIS"    --db iris    --host localhost --password I1DzGh0z0@YoJw
seed_db "CUBRID"  --db cubrid  --host localhost --password TapSnlMKuWhuiGirAWlTCquZ
seed_db "Mimer"   --db mimer   --host localhost --password cHGH1buf8XGnLS5CeuI8xjZ
seed_db "Geode"   --db geode   --host localhost
seed_db "H2"      --db h2      --host localhost

echo ""
echo "================================"
if [ ${#FAILED[@]} -eq 0 ]; then
    echo "All databases seeded successfully."
else
    echo "FAILED databases: ${FAILED[*]}"
    echo "Check the output above for details."
    exit 1
fi
