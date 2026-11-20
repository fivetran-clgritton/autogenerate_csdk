#!/bin/bash

cd /opt/sdk-test-dbs

# Fix ownership
chown -R CLAbeel:CLAbeel /opt/sdk-test-dbs/connectors 2>/dev/null || true

# Pull latest code
cd connectors && git pull && cd ..

# Restart Exasol with updated compose (port 8563)
docker compose up -d exasol
echo "Waiting 30s for Exasol to initialize..."
sleep 30

# Activate venv
source venv/bin/activate

FAILED=()

seed_db() {
    local name=$1
    shift
    echo ""
    echo "=== Seeding $name ==="
    if python connectors/seed_data.py "$@" 2>&1; then
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
