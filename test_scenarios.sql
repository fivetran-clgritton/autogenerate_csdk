-- ============================================================
-- SDK Connector Test Scenarios
-- Standard ANSI SQL — works for Vertica, Exasol, H2, Mimer.
-- VoltDB, CUBRID, IRIS have minor syntax differences noted below.
-- Run each block between connector syncs.
-- ============================================================

-- ── SCENARIO 1: Initial sync baseline ───────────────────────
-- After first fivetran sync completes, verify row counts match.
SELECT 'customers' AS tbl, COUNT(*) AS rows FROM customers
UNION ALL
SELECT 'orders',           COUNT(*)         FROM orders;
-- Expected: matches your seed counts (1M + 10M)


-- ── SCENARIO 2: Incremental — new rows ──────────────────────
-- Insert 50k new orders with updated_at > last sync watermark.
-- Then re-sync and verify only the new rows land in destination.
INSERT INTO orders (id, customer_id, amount, status, created_at, updated_at)
SELECT
    (SELECT MAX(id) FROM orders) + ROW_NUMBER() OVER (ORDER BY 1) AS id,
    FLOOR(RANDOM() * 1000000) + 1,
    ROUND(RANDOM() * 4999 + 1, 2),
    'pending',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP          -- this is the incremental cursor column
FROM orders LIMIT 50000;
-- Vertica: use RANDOM(); Exasol: use RAND(); H2/Mimer: use RAND()
-- VoltDB: INSERT ... VALUES loop via stored procedure


-- ── SCENARIO 3: Incremental — updates ───────────────────────
-- Update 10k existing orders (bump updated_at) so they re-sync.
UPDATE orders
SET status     = 'shipped',
    updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT id FROM orders
    WHERE status = 'confirmed'
    ORDER BY RANDOM()           -- Exasol/H2: ORDER BY RAND()
    LIMIT 10000
);


-- ── SCENARIO 4: Soft deletes ─────────────────────────────────
-- Mark 5k orders as 'cancelled' (connector should detect status change).
-- Soft deletes depend on updated_at being bumped — verify connector picks them up.
UPDATE orders
SET status     = 'cancelled',
    updated_at = CURRENT_TIMESTAMP
WHERE id IN (
    SELECT id FROM orders
    WHERE status = 'pending'
    ORDER BY RANDOM()
    LIMIT 5000
);


-- ── SCENARIO 5: Hard deletes ─────────────────────────────────
-- Delete 1k rows outright. Connectors using cursor-based sync
-- will NOT pick these up — this is expected behavior to document.
-- For connectors with CDC or snapshot diff, they should appear as deletes.
DELETE FROM orders
WHERE id IN (
    SELECT id FROM orders
    WHERE status = 'refunded'
    ORDER BY id
    LIMIT 1000
);
-- After sync: check destination still has these rows (cursor-based = expected gap).


-- ── SCENARIO 6: Prolonged outage simulation ──────────────────
-- Stop the container, insert rows, restart, re-sync.
-- (Shell commands — not SQL)
--
--   docker compose stop vertica
--   # wait 30 minutes (or as long as the test requires)
--   docker compose start vertica
--
-- Then run Scenario 2 inserts above and trigger a sync.
-- Verify connector reconnects and picks up backlog without duplicates.


-- ── SCENARIO 7: Mid-sync failure ─────────────────────────────
-- Start a large bulk insert on the source while connector is running.
-- Kill the connector process mid-sync (Fivetran: pause the connector).
-- Resume. Verify: no duplicate rows in destination, no data loss.
--
-- Set up a large pending batch:
INSERT INTO orders (id, customer_id, amount, status, created_at, updated_at)
SELECT
    (SELECT MAX(id) FROM orders) + ROW_NUMBER() OVER (ORDER BY 1),
    FLOOR(RANDOM() * 1000000) + 1,
    ROUND(RANDOM() * 4999 + 1, 2),
    'pending',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM orders LIMIT 500000;    -- 500k rows so sync takes a while


-- ── SCENARIO 8: Schema drift — add column ────────────────────
-- Add a new nullable column mid-sync. Connector should handle gracefully
-- (new column appears in destination on next sync, NULLs for prior rows).
ALTER TABLE orders ADD COLUMN discount_pct DECIMAL(5,2) DEFAULT NULL;

-- Populate it for new rows going forward:
UPDATE orders
SET discount_pct = ROUND(RANDOM() * 30, 2),
    updated_at   = CURRENT_TIMESTAMP
WHERE status = 'pending'
  AND discount_pct IS NULL
  AND id > (SELECT MAX(id) - 100000 FROM orders);


-- ── SCENARIO 9: Schema drift — rename / drop ─────────────────
-- Less common but important: some connectors fail on column drops.
-- Test separately; note Vertica doesn't support DROP COLUMN on
-- column-store tables without REORGANIZE.
ALTER TABLE orders ADD COLUMN notes VARCHAR(500);
-- (then sync, verify column appears)
-- ALTER TABLE orders DROP COLUMN notes;
-- (then sync, verify connector doesn't crash)


-- ── SCENARIO 10: Large payload rows ──────────────────────────
-- Insert rows with large text blobs to test serialization limits.
INSERT INTO orders (id, customer_id, amount, status, created_at, updated_at)
VALUES (
    (SELECT MAX(id) + 1 FROM orders),
    1,
    99.99,
    REPEAT('x', 10000),   -- 10KB status string (deliberately abnormal)
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);
-- H2/Exasol: REPEAT works as above
-- Vertica: REPEAT works
-- VoltDB: VARCHAR limited to 1MB


-- ── SCENARIO 11: High-volume cursor boundary ─────────────────
-- Insert rows with updated_at = exactly the last sync watermark
-- to test boundary condition (should connector include or exclude?).
-- (Replace '2024-01-15 12:34:56' with the actual last sync time)
INSERT INTO orders (id, customer_id, amount, status, created_at, updated_at)
VALUES (
    (SELECT MAX(id) + 1 FROM orders),
    42, 1.00, 'pending',
    TIMESTAMP '2024-01-15 12:34:56',
    TIMESTAMP '2024-01-15 12:34:56'   -- exact boundary
);


-- ── SCENARIO 12: Concurrent writes during sync ───────────────
-- Run this in a second session while the connector sync is running.
-- Verifies connector handles dirty reads / snapshot isolation correctly.
BEGIN;
  UPDATE orders SET amount = amount * 1.1, updated_at = CURRENT_TIMESTAMP
  WHERE id BETWEEN 1 AND 100000;
  -- hold the transaction open for 60 seconds, then commit
  -- SELECT pg_sleep(60); -- Vertica/Exasol: use equivalent wait
COMMIT;
