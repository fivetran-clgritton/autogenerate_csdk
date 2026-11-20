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

# Install missing Python packages
source /opt/sdk-test-dbs/venv/bin/activate
pip install --quiet CUBRIDdb mimerpy 2>&1 | tail -5

# Ensure Exasol is up (idempotent)
docker compose -f /opt/sdk-test-dbs/docker-compose.yml up -d exasol 2>&1 | grep -v "already allocated" || true
echo "Waiting 30s for Exasol to initialize..."
sleep 30

# Reset IRIS _SYSTEM password so programmatic login works
# IRIS Community requires password change on first use; force it via irisdb exec
echo "Resetting IRIS _SYSTEM password..."
docker exec iris iris session IRIS -U '%SYS' \
  '##class(Security.Users).ChangePassword("_SYSTEM","SYS","SYS")' 2>/dev/null || \
  echo "IRIS password reset skipped (may already be set)"

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
seed_db "IRIS"    --db iris    --host localhost --password SYS
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
