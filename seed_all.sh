#!/bin/bash
# Usage: sudo bash seed_all.sh [exasol iris cubrid mimer geode h2]   (no args = all six)
# Set SEED_SKIP_UPDATE=1 to run the checkout as-is instead of resetting it to origin/main.

KNOWN="exasol iris cubrid mimer geode h2"
for a in "$@"; do
    [[ " $KNOWN " == *" ${a,,} "* ]] || { echo "Unknown database '$a'. Choose from: $KNOWN" >&2; exit 2; }
done
WANT=" $(echo "${*:-$KNOWN}" | tr '[:upper:]' '[:lower:]') "
wants() { [[ "$WANT" == *" $1 "* ]]; }

ROOT=${SEED_ROOT:-/opt/sdk-test-dbs}
REPO=$ROOT/connectors

# Reset to origin/main, then re-exec: bash keeps executing the old copy of a script replaced mid-run.
if [ -z "$SEED_SKIP_UPDATE" ]; then
    git -C "$REPO" fetch origin main && git -C "$REPO" reset --hard origin/main \
        || { echo "git update failed" >&2; exit 1; }
    chown -R CLAbeel:CLAbeel "$REPO" 2>/dev/null || true
    SEED_SKIP_UPDATE=1 exec bash "$REPO/seed_all.sh" "$@"
fi

cd "$ROOT"

if wants h2 && ! command -v java &>/dev/null; then
    echo "Installing Java..."
    apt-get install -y --no-install-recommends default-jre-headless
fi

# mimerpy needs the native client library, which only ships inside the Mimer container
MIMER_LIB=/usr/local/lib/libmimerapi.so
if wants mimer && [ ! -f "$MIMER_LIB" ]; then
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

source "$ROOT/venv/bin/activate"
wants cubrid && pip install --quiet CUBRID-Python 2>&1 | tail -3
wants mimer && pip install --quiet mimerpy 2>&1 | tail -3

UP=""
for s in exasol geode; do wants "$s" && UP="$UP $s"; done
if [ -n "$UP" ]; then
    docker compose -f "$ROOT/docker-compose.yml" up -d $UP
    echo "Waiting 30s for containers to initialize..."
    sleep 30
fi

FAILED=()

seed_db() {
    local name=$1
    shift
    if ! wants "${name,,}"; then
        echo "--- Skipping $name"
        return
    fi
    echo ""
    echo "=== Seeding $name ==="
    if python "$REPO/seed_data.py" "$@" 2>&1; then
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
# Geode loads over REST, which is far slower than SQL bulk inserts, so it gets a 10x smaller dataset
seed_db "Geode"   --db geode   --host localhost --customers 100000 --orders 1000000
seed_db "H2"      --db h2      --host localhost

echo ""
echo "================================"
if [ ${#FAILED[@]} -eq 0 ]; then
    echo "All requested databases seeded successfully."
else
    echo "FAILED databases: ${FAILED[*]}"
    echo "Check the output above for details."
    exit 1
fi
