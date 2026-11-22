"""Run the 12 test scenarios (CLAUDE.md, "Test Scenarios") against the IRIS connector on a small fixture.

Usage (on the VM, with a scratch config that holds host, port, username, password and namespace):

    PYTHONPATH=~/connector-deps /usr/bin/python3 iris/scenarios.py --config <scratch>/config.json --work <scratch>/scenarios

The fixture is rebuilt in the FTTest schema from the seeded tables (customers id <= 1000, orders id <= 10000);
the seeded SQLUser tables are only read. Each sync is a `fivetran debug` run of a copy of connector.py in a work
directory, and after every scenario the whole destination is compared field by field with the source.
Scenarios 7 and 12 run patched copies of the connector (an injected crash; writes from a second connection
between queries), so they are deterministic. Needs sqlalchemy-iris (PYTHONPATH=~/connector-deps) and duckdb
(the `fivetran` CLI's Python has it). Adapted from exasol/scenarios.py and h2/scenarios.py.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime
from decimal import Decimal

import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

HERE = os.path.dirname(os.path.abspath(__file__))
SEEDED = "SQLUser"
SCHEMA = "FTTest"
FIXTURE = {"customers": 1000, "orders": 10000}
ORDERS_KEY = f"{SCHEMA}.orders"


def sub(source, old, new):
    """Replace exactly one occurrence, so a patch fails loudly if connector.py changes."""
    assert source.count(old) == 1, f"patch target not found exactly once: {old!r}"
    return source.replace(old, new)


def iris_url(config):
    # URL.create keeps the '@' in the password intact
    return URL.create("iris", username=config.get("username") or "_SYSTEM", password=config["password"],
                      host=config["host"], port=int(config.get("port") or 1972),
                      database=config.get("namespace") or "USER")


class Runner:
    def __init__(self, config_path, work):
        self.base = json.load(open(config_path))
        self.work = os.path.abspath(work)
        shutil.rmtree(self.work, ignore_errors=True)
        os.makedirs(os.path.join(self.work, "logs"))
        self.engine = create_engine(iris_url(self.base))
        self.config = self._write_config("config.json", {})
        self.bad_port_config = self._write_config("config_closed_port.json", {"port": "1"})
        self.results = []
        self.log_seq = 0

    # -- source -------------------------------------------------------------------------------

    def x(self, sql, params=None):
        with self.engine.begin() as conn:
            return conn.execute(text(sql), params or {}).rowcount

    def rows(self, sql, params=None):
        # No ORDER BY on selected columns here: IRIS would return them in their internal sort form.
        with self.engine.connect() as conn:
            return [tuple(row) for row in conn.execute(text(sql), params or {}).fetchall()]

    def one(self, sql, params=None):
        return self.rows(sql, params)[0][0]

    def build_fixture(self):
        for table, max_id in FIXTURE.items():
            self.x(f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
            self.x(f"CREATE TABLE {SCHEMA}.{table} AS SELECT * FROM {SEEDED}.{table} WHERE id <= {max_id}")
            self.x(f"ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT fx_{table}_pk PRIMARY KEY (id)")
            count = self.one(f"SELECT COUNT(*) FROM {SCHEMA}.{table}")
            assert count == max_id, f"fixture {table}: {count} rows, expected {max_id}"

    def insert_orders(self, first, last, updated_at):
        """Copy seeded orders first..last into the fixture with a fixed updated_at (they are not in the fixture yet)."""
        n = self.x(f"INSERT INTO {SCHEMA}.orders (id, customer_id, amount, status, created_at, updated_at) "
                   f"SELECT id, customer_id, amount, status, created_at, CAST(:ts AS TIMESTAMP) "
                   f"FROM {SEEDED}.orders WHERE id BETWEEN {first} AND {last}", {"ts": updated_at})
        assert n == last - first + 1, f"inserted {n} orders, expected {last - first + 1}"

    # -- connector runs -----------------------------------------------------------------------

    def _write_config(self, name, overrides):
        config = dict(self.base, schemas=SCHEMA, include_tables=",".join(FIXTURE), exclude_tables="",
                      cursor_columns="updated_at", cursor_overrides="")
        config.update(overrides)
        path = os.path.join(self.work, name)
        with open(path, "w") as f:
            json.dump(config, f)
        os.chmod(path, 0o600)
        return path

    def project(self, name, patch=None):
        """A fresh project directory with a (possibly patched) copy of connector.py and an empty state."""
        path = os.path.join(self.work, name)
        shutil.rmtree(path, ignore_errors=True)
        os.makedirs(path)
        self.install(path, patch)
        shutil.copy(os.path.join(HERE, "requirements.txt"), path)
        with open(os.path.join(path, "state.json"), "w") as f:
            f.write("{}")
        return path

    def install(self, path, patch=None):
        source = open(os.path.join(HERE, "connector.py")).read()
        if patch:
            source = patch(source)
        with open(os.path.join(path, "connector.py"), "w") as f:
            f.write(source)

    def sync(self, path, label, config=None):
        self.log_seq += 1
        log_path = os.path.join(self.work, "logs", f"{self.log_seq:02d}_{label}.log")
        command = ["fivetran", "debug", "--configuration", config or self.config,
                   "--state", os.path.join(path, "state.json"), "--naming", "SOURCE", "--non-interactive"]
        with open(log_path, "w") as log:
            proc = subprocess.run(command, cwd=path, stdout=log, stderr=subprocess.STDOUT,
                                  env=dict(os.environ, PYTHONUNBUFFERED="1"), timeout=1800)
        out = open(log_path).read()
        upserts = re.search(r"Upserts\s*\|\s*(\d+)", out)
        return {"rc": proc.returncode, "ok": "SYNC SUCCEEDED" in out, "log": log_path, "text": out,
                "upserts": int(upserts.group(1)) if upserts else None}

    def state(self, path):
        return json.load(open(os.path.join(path, "state.json")))

    def expected_reads(self, path):
        """Rows the next incremental sync should read: cursor >= saved cursor (the previous high-water mark), plus NULL cursors."""
        tables = self.state(path).get("tables", {})
        total = 0
        for table in FIXTURE:
            cursor = tables[f"{SCHEMA}.{table}"]["cursor"]
            total += self.one(f"SELECT COUNT(*) FROM {SCHEMA}.{table} WHERE updated_at IS NULL "
                              f"OR updated_at >= CAST(:c AS TIMESTAMP)", {"c": cursor})
        return total

    # -- destination comparison ---------------------------------------------------------------

    def compare(self, path, expected_extra=None, expected_diff_ids=None):
        """Field-by-field comparison of every source row with the destination.

        expected_extra: {table: ids} rows deleted at the source that a cursor sync cannot see.
        expected_diff_ids: {table: ids} rows whose differences are an expected, documented gap.
        Returns (ok, summary, details).
        """
        expected_extra = expected_extra or {}
        expected_diff_ids = expected_diff_ids or {}
        dst = duckdb.connect(os.path.join(path, "files", "warehouse.db"), read_only=True)
        ok, summary, details = True, [], {}
        try:
            for table in FIXTURE:
                positions = self.rows(
                    "SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t", {"s": SCHEMA, "t": table})
                columns = [name for name, _ in sorted(positions, key=lambda p: p[1])]
                id_pos = columns.index("id")
                source = {}
                for row in self.rows(f"SELECT {', '.join(chr(34) + c + chr(34) for c in columns)} FROM {SCHEMA}.{table}"):
                    source[row[id_pos]] = row
                dschema, dtable = next((s, t) for s, t in dst.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables").fetchall()
                    if s.lower() == SCHEMA.lower() and t.lower() == table)
                dcols = {r[0].lower(): r[0] for r in dst.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
                    [dschema, dtable]).fetchall()}
                missing_columns = [c for c in columns if c.lower() not in dcols]
                present = [c for c in columns if c.lower() in dcols]
                ref = f'"{dschema}"."{dtable}"'
                dest_rows = dst.execute(
                    f"SELECT {', '.join(chr(34) + dcols[c.lower()] + chr(34) for c in present)} FROM {ref}").fetchall()
                dest_id = present.index("id")
                dest = {r[dest_id]: r for r in dest_rows}
                duplicates = len(dest_rows) - len(dest)
                deleted_flagged = dst.execute(f'SELECT COUNT(*) FROM {ref} WHERE "{dcols["_fivetran_deleted"]}"').fetchone()[0]

                missing = sorted(set(source) - set(dest))
                extra = sorted(set(dest) - set(source))
                diffs = {}
                for key in set(source) & set(dest):
                    s_row = dict(zip(columns, source[key]))
                    d_row = dict(zip(present, dest[key]))
                    for column in present:
                        sv, dv = s_row[column], d_row[column]
                        if isinstance(dv, datetime):
                            dv = dv.replace(tzinfo=None)
                        if isinstance(sv, Decimal) or isinstance(dv, Decimal):
                            sv, dv = (None if v is None else Decimal(str(v)) for v in (sv, dv))
                        if sv != dv:
                            diffs.setdefault(key, []).append(column)
                unexpected_extra = sorted(set(extra) - set(expected_extra.get(table, ())))
                unexpected_diffs = sorted(set(diffs) - set(expected_diff_ids.get(table, ())))
                table_ok = not (missing_columns or missing or unexpected_extra or unexpected_diffs or duplicates)
                ok = ok and table_ok
                summary.append(f"{table}: source {len(source):,}, destination {len(dest_rows):,}"
                               + (f", {duplicates} duplicate keys" if duplicates else "")
                               + (f", missing columns {missing_columns}" if missing_columns else "")
                               + (f", {len(missing)} missing rows" if missing else "")
                               + (f", {len(extra)} rows only in destination" if extra else "")
                               + (f", {len(diffs)} rows differ" if diffs else "")
                               + (f", {deleted_flagged} flagged deleted" if deleted_flagged else ""))
                details[table] = {"missing": missing[:10], "extra": extra[:20],
                                  "diffs": {k: diffs[k] for k in sorted(diffs)[:10]}, "diff_ids": sorted(diffs),
                                  "dest_columns": sorted(dcols)}
        finally:
            dst.close()
        return ok, "; ".join(summary), details

    # -- reporting ----------------------------------------------------------------------------

    def record(self, number, name, verdict, detail):
        self.results.append((number, name, verdict, detail))
        print(f"[{verdict}] {number}. {name}: {detail}", flush=True)

    def amend(self, problem):
        number, name, _, detail = self.results[-1]
        self.results[-1] = (number, name, "FAIL", f"{detail}; {problem}")
        print(f"[FAIL] {number}. {name}: {problem}", flush=True)

    def check_sync(self, result, expected_upserts=None):
        problems = []
        if not result["ok"]:
            problems.append(f"sync failed (rc {result['rc']}, log {result['log']})")
        if expected_upserts is not None and result["upserts"] != expected_upserts:
            problems.append(f"upserts {result['upserts']} != expected {expected_upserts}")
        return problems


# ---------------------------------------------------------------------------------------------
# Patched connector copies
# ---------------------------------------------------------------------------------------------


def crash_after_checkpoints(count):
    """Checkpoint every 1,000 rows and raise after the given number of checkpoints (scenario 7)."""
    def patch(source):
        source = sub(source, "CHECKPOINT_ROW_CAP = 100_000", "CHECKPOINT_ROW_CAP = 1_000")
        source = sub(source, "MIN_CHECKPOINT_ROW_CAP = 10_000", "MIN_CHECKPOINT_ROW_CAP = 1_000")
        source = sub(source, "class _Checkpointer:", "_CRASH = {'checkpoints': 0}\n\n\nclass _Checkpointer:")
        source = sub(source, "        op.checkpoint(state=self.state)\n",
                     "        op.checkpoint(state=self.state)\n"
                     "        _CRASH['checkpoints'] += 1\n"
                     f"        if _CRASH['checkpoints'] >= {count}:\n"
                     f"            raise RuntimeError('scenario 7: injected failure after {count} checkpoints')\n")
        return source
    return patch


def writes_between_queries(hook_path):
    """1,000 rows per query; before the third orders query, run the statements in hook_path on a second connection."""
    def patch(source):
        source = sub(source, "BATCH_SIZE = 50_000", "BATCH_SIZE = 1_000")  # orders has a LONGVARCHAR by now: 1,000 anyway
        source = sub(source, "def _read_rows(", (
            "def _scenario_hook(timer):\n"
            f"    if timer.key != {ORDERS_KEY!r} or timer.queries != 2 or _HOOK['done']:\n"
            "        return\n"
            "    _HOOK['done'] = True\n"
            "    import json\n"
            f"    spec = json.load(open({hook_path!r}))\n"
            "    hook_engine = create_engine(URL.create('iris', **spec['url']))\n"
            "    with hook_engine.begin() as hook_conn:\n"
            "        for statement in spec['statements']:\n"
            "            hook_conn.execute(text(statement))\n"
            "    hook_engine.dispose()\n"
            "    log.info(f\"scenario 12: ran {len(spec['statements'])} statements on a second connection\")\n"
            "\n\n_HOOK = {'done': False}\n\n\ndef _read_rows("))
        source = sub(source, "        result = conn.execute(text(sql), query_params)\n",
                     "        _scenario_hook(timer)\n        result = conn.execute(text(sql), query_params)\n")
        return source
    return patch


# ---------------------------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------------------------


def run(r: Runner):
    r.build_fixture()
    main = r.project("main")

    # 1. Initial sync baseline
    res = r.sync(main, "s01_initial")
    problems = r.check_sync(res, expected_upserts=sum(FIXTURE.values()))
    ok, summary, _ = r.compare(main)
    r.record(1, "Initial sync baseline", "PASS" if ok and not problems else "FAIL",
             f"{res['upserts']} upserts; {summary}" + (f"; {problems}" if problems else ""))

    def step(number, name, label, expected_extra=None, expected_diff_ids=None, verdict_if_ok="PASS", note=""):
        expected = r.expected_reads(main)
        res = r.sync(main, label)
        problems = r.check_sync(res, expected_upserts=expected)
        ok, summary, details = r.compare(main, expected_extra, expected_diff_ids)
        verdict = verdict_if_ok if ok and not problems else "FAIL"
        r.record(number, name, verdict, f"{res['upserts']} upserts (expected {expected}); {summary}"
                 + (f"; {problems}" if problems else "") + (f"; {note}" if note else ""))
        return res, details

    # 2. New rows
    r.insert_orders(10001, 10500, "2025-02-02 00:00:00")
    step(2, "Incremental: 500 new rows", "s02_insert")

    # 3. Updates (100 rows sharing one new updated_at: ties on the cursor)
    r.x(f"UPDATE {SCHEMA}.orders SET amount = amount + 1.11, updated_at = CAST('2025-02-03 00:00:00' AS TIMESTAMP) "
        "WHERE id BETWEEN 101 AND 200")
    step(3, "Incremental: 100 updated rows", "s03_update")

    # 4. Soft deletes
    r.x(f"UPDATE {SCHEMA}.orders SET status = 'cancelled', updated_at = CAST('2025-02-04 00:00:00' AS TIMESTAMP) "
        "WHERE id BETWEEN 201 AND 250")
    step(4, "Soft deletes: 50 rows set to cancelled", "s04_soft_delete")

    # 5. Hard deletes: expected gap for a cursor-based sync
    r.x(f"DELETE FROM {SCHEMA}.orders WHERE id BETWEEN 251 AND 260")
    deleted = {"orders": list(range(251, 261))}
    step(5, "Hard deletes: 10 rows", "s05_hard_delete", expected_extra=deleted, verdict_if_ok="PASS (known gap)",
         note="the 10 deleted rows stay in the destination, not flagged deleted: cursor-based sync cannot see deletes")

    # 6. Outage: the database is unreachable, then a backlog arrives
    before = open(os.path.join(main, "state.json")).read()
    res = r.sync(main, "s06_outage", config=r.bad_port_config)
    after = open(os.path.join(main, "state.json")).read()
    error = next((line.strip() for line in res["text"].splitlines() if "Could not connect to IRIS" in line), "")
    retries = len(re.findall(r"Connecting to IRIS at \S+ failed", res["text"]))
    outage_problems = []
    if res["ok"] or res["rc"] == 0:
        outage_problems.append("sync against a closed port did not fail")
    if before != after:
        outage_problems.append("state changed by the failed sync")
    if "Could not connect to IRIS" not in res["text"]:
        outage_problems.append("no 'Could not connect to IRIS' error in the log")
    r.insert_orders(10501, 10700, "2025-02-06 00:00:00")
    expected = r.expected_reads(main)
    res2 = r.sync(main, "s06_after_outage")
    outage_problems += r.check_sync(res2, expected_upserts=expected)
    ok, summary, _ = r.compare(main, deleted)
    r.record(6, "Outage, then 200-row backlog", "PASS" if ok and not outage_problems else "FAIL",
             f"failed sync rc {res['rc']} after {retries} logged retries, state unchanged: {before == after}, "
             f"error: {error[:240]!r}; then {res2['upserts']} upserts (expected {expected}); {summary}"
             + (f"; {outage_problems}" if outage_problems else ""))

    # 7. Mid-sync failure, in its own destination from empty state, with a bulk insert landing before the resume
    crash = r.project("s07", patch=crash_after_checkpoints(5))
    res = r.sync(crash, "s07_crash")
    crash_problems = []
    if res["ok"] or "injected failure" not in res["text"]:
        crash_problems.append("injected failure did not happen")
    saved = r.state(crash)
    source_of_state = "state file"
    if not saved.get("tables"):
        # If fivetran debug does not write the state file on failure, take the last checkpoint from the log.
        recorded = re.findall(r"checkpoint recorded: (\{.*\})\s*$", res["text"], re.MULTILINE)
        saved = json.loads(recorded[-1]) if recorded else {}
        with open(os.path.join(crash, "state.json"), "w") as f:
            json.dump(saved, f)
        source_of_state = "last 'checkpoint recorded' line of the log"
    orders_state = saved.get("tables", {}).get(ORDERS_KEY, {})
    mid_table = "after" in orders_state
    dst = duckdb.connect(os.path.join(crash, "files", "warehouse.db"), read_only=True)
    rows_after_crash = dst.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."orders"').fetchone()[0]
    dst.close()
    r.insert_orders(10701, 11000, "2025-02-07 00:00:00")
    r.install(crash)  # resume with the real connector
    res2 = r.sync(crash, "s07_resume")
    crash_problems += r.check_sync(res2)
    if not mid_table:
        crash_problems.append("saved state was not mid-table (no 'after' position for orders)")
    if "resuming incremental sync" not in res2["text"]:
        crash_problems.append("resume did not continue the interrupted pass")
    ok, summary, _ = r.compare(crash, deleted)
    r.record(7, "Mid-sync failure and resume", "PASS" if ok and not crash_problems else "FAIL",
             f"crashed after 5 checkpoints with {rows_after_crash:,} orders in the destination; resumed from the "
             f"{source_of_state} (mid-table: {mid_table}, after={orders_state.get('after')}); 300 rows inserted before "
             f"the resume; resume upserted {res2['upserts']}; {summary}"
             + (f"; {crash_problems}" if crash_problems else ""))

    # The main destination has not seen ids 10701-11000 yet; they arrive with the next sync (scenario 8).

    # 8. Add a column
    r.x(f"ALTER TABLE {SCHEMA}.orders ADD discount_pct DECIMAL(5,2)")
    r.x(f"UPDATE {SCHEMA}.orders SET discount_pct = MOD(id, 50) + 0.25, "
        "updated_at = CAST('2025-02-08 00:00:00' AS TIMESTAMP) WHERE id BETWEEN 301 AND 400")
    res, details = step(8, "Schema drift: add discount_pct, set on 100 rows", "s08_add_column", deleted)
    if "discount_pct" not in details["orders"]["dest_columns"]:
        r.amend("discount_pct missing in destination")
    if "new columns discount_pct" not in res["text"]:
        r.amend("no new-column warning")
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    discount = dst.execute(f'SELECT COUNT("discount_pct"), COUNT(*) FROM "{SCHEMA}"."orders"').fetchone()
    dtype = dst.execute("SELECT data_type FROM information_schema.columns WHERE table_name = 'orders' "
                        "AND column_name = 'discount_pct'").fetchone()[0]
    dst.close()
    if discount[0] != 100:
        r.amend(f"{discount[0]} destination rows have discount_pct, expected 100")
    r.results[-1] = r.results[-1][:3] + (r.results[-1][3] + f"; destination: {discount[0]} of {discount[1]} rows "
                                         f"have discount_pct, type {dtype}",)

    # 9. Add then drop a column
    r.x(f"ALTER TABLE {SCHEMA}.orders ADD notes VARCHAR(500)")
    r.x(f"UPDATE {SCHEMA}.orders SET notes = 'note ' || id, updated_at = CAST('2025-02-09 00:00:00' AS TIMESTAMP) "
        "WHERE id BETWEEN 401 AND 410")
    step(9, "Schema drift: add notes (step 1 of 2)", "s09a_add_notes", deleted)
    r.x(f"ALTER TABLE {SCHEMA}.orders DROP COLUMN notes")
    r.x(f"UPDATE {SCHEMA}.orders SET updated_at = CAST('2025-02-09 12:00:00' AS TIMESTAMP) WHERE id BETWEEN 401 AND 420")
    res_drop, _ = step(9, "Schema drift: drop notes (step 2 of 2)", "s09b_drop_notes", deleted)
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    notes = dst.execute(f'SELECT COUNT(*), COUNT("notes") FROM "{SCHEMA}"."orders" WHERE "id" BETWEEN 401 AND 420').fetchone()
    dst.close()
    drift_logged = "columns no longer in the source: notes" in res_drop["text"]
    r.record(9, "Schema drift: after the drop", "PASS" if res_drop["ok"] and drift_logged else "FAIL",
             f"drop logged: {drift_logged}; destination keeps notes; of the 20 rows re-synced after the drop, "
             f"{notes[1]} still have a notes value")

    # 10. Large payloads: about 1 MB of text per row (UTF-8), including multi-byte characters, in a LONGVARCHAR
    r.x(f"ALTER TABLE {SCHEMA}.orders ADD payload LONGVARCHAR")
    r.insert_orders(11001, 11005, "2025-02-10 00:00:00")
    r.x(f"UPDATE {SCHEMA}.orders SET payload = REPEAT('payload ' || id || ' é€ ', 50000) WHERE id BETWEEN 11001 AND 11005")
    _, details = step(10, "Large payloads: 5 rows of ~1 MB", "s10_large_payload", deleted)
    src_payload = dict(r.rows(f"SELECT id, payload FROM {SCHEMA}.orders WHERE payload IS NOT NULL"))
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    dst_payload = dict(dst.execute(f'SELECT "id", "payload" FROM "{SCHEMA}"."orders" WHERE "payload" IS NOT NULL').fetchall())
    dst.close()

    def digest(values):
        return {k: (len(v), len(v.encode()), hashlib.sha256(v.encode()).hexdigest()[:12]) for k, v in values.items()}

    src_digest, dst_digest = digest(src_payload), digest(dst_payload)
    sizes = sorted(v[:2] for v in src_digest.values())
    note = (f"{len(src_digest)} payloads of {sizes[0][0]:,}-{sizes[-1][0]:,} characters "
            f"({sizes[0][1]:,}-{sizes[-1][1]:,} UTF-8 bytes); length and sha256 equal: {src_digest == dst_digest}")
    r.results[-1] = r.results[-1][:3] + (r.results[-1][3] + f"; {note}",)
    if len(src_digest) != 5 or src_digest != dst_digest:
        r.amend("payload length/hash mismatch")
    print(f"     {note}", flush=True)

    # 11. Rows written exactly at the saved cursor value
    cursor = r.state(main)["tables"][ORDERS_KEY]["cursor"]
    max_updated = r.one(f"SELECT MAX(updated_at) FROM {SCHEMA}.orders")
    n = r.x(f"INSERT INTO {SCHEMA}.orders (id, customer_id, amount, status, created_at, updated_at) "
            f"SELECT id, customer_id, amount, status, created_at, CAST(:c AS TIMESTAMP) "
            f"FROM {SEEDED}.orders WHERE id BETWEEN 11101 AND 11120", {"c": cursor})
    assert n == 20
    step(11, "Cursor boundary: 20 rows at exactly the saved cursor", "s11_boundary", deleted,
         note=f"saved cursor {cursor}, source max before the insert {max_updated}")

    # 12. Concurrent writes between the queries of one sync
    r.x(f"UPDATE {SCHEMA}.orders SET updated_at = DATEADD('s', id, CAST('2025-02-12 00:00:00' AS TIMESTAMP)) "
        "WHERE id BETWEEN 1001 AND 4000")
    hook_path = os.path.join(r.work, "s12_hook.json")
    statements = [
        # already read in this sync (ids 1001-~2975 come back in the first two queries): moved past the reader
        f"UPDATE {SCHEMA}.orders SET amount = amount + 5, updated_at = CAST('2025-02-13 00:00:00' AS TIMESTAMP) "
        "WHERE id BETWEEN 1001 AND 1050",
        # not read yet
        f"UPDATE {SCHEMA}.orders SET amount = amount + 7, updated_at = CAST('2025-02-13 00:00:01' AS TIMESTAMP) "
        "WHERE id BETWEEN 3901 AND 3950",
        # new rows
        f"INSERT INTO {SCHEMA}.orders (id, customer_id, amount, status, created_at, updated_at) "
        f"SELECT id, customer_id, amount, status, created_at, CAST('2025-02-13 00:00:02' AS TIMESTAMP) "
        f"FROM {SEEDED}.orders WHERE id BETWEEN 11201 AND 11250",
        # a late commit carrying an old timestamp, below the cursor: a cursor-based sync cannot see it
        f"UPDATE {SCHEMA}.orders SET amount = amount + 9, updated_at = CAST('2024-06-01 00:00:00' AS TIMESTAMP) "
        "WHERE id BETWEEN 501 AND 510",
    ]
    url = {"username": r.base.get("username") or "_SYSTEM", "password": r.base["password"], "host": r.base["host"],
           "port": int(r.base.get("port") or 1972), "database": r.base.get("namespace") or "USER"}
    with open(hook_path, "w") as f:
        json.dump({"url": url, "statements": statements}, f)
    os.chmod(hook_path, 0o600)
    r.install(main, patch=writes_between_queries(hook_path))
    res = r.sync(main, "s12_concurrent")
    r.install(main)
    os.remove(hook_path)
    hook_ran = "scenario 12: ran 4 statements" in res["text"]
    late = {"orders": list(range(501, 511))}
    # IRIS reads to the end of the table (no upper bound fixed at the start of the pass), so rows moved
    # past the reader are read again later in the same sync: only the late commit should differ.
    ok1, summary1, details1 = r.compare(main, deleted, late)
    behind_stale = [k for k in details1["orders"]["diff_ids"] if 1001 <= k <= 1050]
    res2 = r.sync(main, "s12_followup")
    ok2, summary2, details2 = r.compare(main, deleted, late)
    late_missed = details2["orders"]["diff_ids"] == list(range(501, 511))
    problems = [] if hook_ran else ["hook did not run"]
    problems += r.check_sync(res) + r.check_sync(res2)
    r.record(12, "Concurrent writes during a sync", "PASS (known gap)" if ok1 and ok2 and late_missed and not problems else "FAIL",
             f"writes ran between queries 2 and 3: {hook_ran}; sync upserted {res['upserts']}, after it: {summary1} "
             f"({len(behind_stale)} of the 50 rows updated behind the reader stale); follow-up upserted {res2['upserts']}; "
             f"after the follow-up: {summary2}; rows updated with an old timestamp (late commit) missed: {late_missed}"
             + (f"; {problems}" if problems else ""))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="scratch config with host, port, username, password, namespace")
    parser.add_argument("--work", required=True, help="work directory (recreated)")
    args = parser.parse_args()
    r = Runner(args.config, args.work)
    try:
        run(r)
    finally:
        r.engine.dispose()
        print("\n| # | Scenario | Result | Detail |\n|---|---|---|---|")
        for number, name, verdict, detail in r.results:
            print(f"| {number} | {name} | {verdict} | {detail} |")
        with open(os.path.join(r.work, "results.json"), "w") as f:
            json.dump(r.results, f, indent=1, default=str)


if __name__ == "__main__":
    main()
