"""Run the 12 test scenarios (CLAUDE.md, "Test Scenarios") against the Mimer SQL connector on a small fixture.

Usage (on the VM, with a scratch config that holds the connection settings):

    export PYTHONPATH=~/connector-deps LD_LIBRARY_PATH=/usr/local/lib
    /usr/bin/python3 mimer/scenarios.py --config <scratch>/config.json --work <scratch>/scenarios

The fixture is rebuilt in the FTTest schema (databank FTTESTDB) from the seeded tables (customers id <= 1000,
orders id <= 10000); the seeded SYSADM tables are only read. Mimer has no CREATE TABLE ... AS SELECT, so the
fixture tables are created with the seeded column definitions and filled with INSERT ... SELECT. Each sync is a
`fivetran debug` run of a copy of connector.py in a work directory, and after every scenario the whole
destination is compared field by field with the source. Scenarios 7 and 12 run patched copies of the connector
(an injected crash; writes from a second connection in the middle of a read), so they are deterministic.
Needs mimerpy and duckdb (the `fivetran` CLI's Python has duckdb).
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

import ctypes

import duckdb
import mimerpy
from mimerpy import cursorPy, mimerapi

HERE = os.path.dirname(os.path.abspath(__file__))
SEEDED = "SYSADM"
SCHEMA = "FTTest"
DATABANK = "FTTESTDB"
FIXTURE = {"customers": 1000, "orders": 10000}
DDL = {
    "customers": "id INTEGER NOT NULL PRIMARY KEY, name NVARCHAR(100), email NVARCHAR(200), "
                 "created_at TIMESTAMP(6), updated_at TIMESTAMP(6), status NVARCHAR(20)",
    "orders": "id INTEGER NOT NULL PRIMARY KEY, customer_id INTEGER, amount DECIMAL(10,2), status NVARCHAR(20), "
              "created_at TIMESTAMP(6), updated_at TIMESTAMP(6)",
}
ORDERS = f"{SCHEMA}.orders"


def sub(text, old, new):
    """Replace exactly one occurrence, so a patch fails loudly if connector.py changes."""
    assert text.count(old) == 1, f"patch target not found exactly once: {old!r}"
    return text.replace(old, new)


def _set_nclob_chunked(statement_ptr, parameter_number, text):
    """Test-side replacement for mimerpy 1.3.11's NCLOB parameter setter.

    mimerpy passes the UTF-8 byte length to MimerSetLob, but Mimer expects the length in characters for an
    NCLOB, and it sends the value in one MimerSetNclobData8 call. Values above about 64k characters fail with
    -19118 "Client and server disagreed on LOB length". This only affects writing LOB parameters (the
    connector reads LOBs through mimerGetNclobData8, which works), so it is patched here for scenario 10.
    """
    sp = int(statement_ptr)
    handle = mimerapi._cached_stmt_handle(sp)
    if text is None:
        return int(mimerapi._MimerSetNull(handle, mimerapi._arg_i16(parameter_number)))
    lob = mimerapi.MimerLob()
    rc = mimerapi._MimerSetLob(handle, mimerapi._arg_i16(parameter_number), ctypes.c_size_t(len(text)), ctypes.byref(lob))
    for start in range(0, len(text), 20000):
        if rc < 0:
            break
        raw = text[start:start + 20000].encode("utf-8")
        rc = mimerapi._MimerSetNclobData8(ctypes.byref(lob), mimerapi._set_utf8_cstring_keep(sp, raw), ctypes.c_size_t(len(raw)))
    return int(rc)


def connect(base):
    conn = mimerpy.connect(dsn=base.get("database") or "mimerdb", user=base.get("username") or "SYSADM",
                           password=base["password"])
    conn.autocommit = True
    cursorPy.set_funcs[mimerapi.MIMER_TYPE_NCLOB] = _set_nclob_chunked
    return conn


class Runner:
    def __init__(self, config_path, work):
        self.base = json.load(open(config_path))
        self.work = os.path.abspath(work)
        shutil.rmtree(self.work, ignore_errors=True)
        os.makedirs(os.path.join(self.work, "logs"))
        self.src = connect(self.base)
        self.config = self._write_config("config.json", {})
        self.bad_port_config = self._write_config("config_closed_port.json", {"port": "1"})
        self.results = []
        self.log_seq = 0

    # -- source -------------------------------------------------------------------------------

    def x(self, sql, params=None):
        cursor = self.src.cursor()
        try:
            cursor.execute(sql, params) if params else cursor.execute(sql)
            return cursor.fetchall() if cursor.description else None
        finally:
            cursor.close()

    def one(self, sql, params=None):
        return self.x(sql, params)[0][0]

    def build_fixture(self):
        existing = {t for (t,) in self.x("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?", [SCHEMA])}
        for table, max_id in FIXTURE.items():
            if table in existing:
                self.x(f"DROP TABLE {SCHEMA}.{table} CASCADE")
            self.x(f"CREATE TABLE {SCHEMA}.{table} ({DDL[table]}) IN {DATABANK}")
            self.x(f"INSERT INTO {SCHEMA}.{table} SELECT * FROM {SEEDED}.{table} WHERE id <= {max_id}")

    def insert_orders(self, first, last, updated_at):
        """Copy seeded orders first..last into the fixture with a fixed updated_at (they are not in the fixture yet)."""
        self.x(f"INSERT INTO {ORDERS} (id, customer_id, amount, status, created_at, updated_at) "
               f"SELECT id, customer_id, amount, status, created_at, TIMESTAMP '{updated_at}' "
               f"FROM {SEEDED}.orders WHERE id BETWEEN {first} AND {last}")

    # -- connector runs -----------------------------------------------------------------------

    def _write_config(self, name, overrides):
        config = dict(self.base, schemas=SCHEMA, include_tables=",".join(FIXTURE), exclude_tables="",
                      cursor_columns="updated_at", cursor_overrides="", **overrides)
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
        text = open(log_path).read()
        upserts = re.search(r"Upserts\s*\|\s*(\d+)", text)
        return {"rc": proc.returncode, "ok": "SYNC SUCCEEDED" in text, "log": log_path, "text": text,
                "upserts": int(upserts.group(1)) if upserts else None}

    def state(self, path):
        return json.load(open(os.path.join(path, "state.json")))

    def expected_reads(self, path):
        """Rows the next incremental sync should read: cursor >= saved cursor, plus NULL-cursor rows."""
        tables = self.state(path).get("tables", {})
        total = 0
        for table in FIXTURE:
            cursor = tables[f"{SCHEMA}.{table}"]["cursor"]
            total += self.one(f"SELECT COUNT(*) FROM {SCHEMA}.{table} WHERE updated_at IS NULL "
                              f"OR updated_at >= CAST(? AS TIMESTAMP(6))", [cursor])
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
                columns = [r[0] for r in self.x(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    "ORDER BY ORDINAL_POSITION", [SCHEMA, table])]
                source = {r[0]: r for r in self.x(
                    f"SELECT {', '.join(columns)} FROM {SCHEMA}.{table}")}
                dschema, dtable = next((s, t) for s, t in dst.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables").fetchall()
                    if s.lower() == SCHEMA.lower() and t.lower() == table)
                dcols = {r[0].lower(): r[0] for r in dst.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
                    [dschema, dtable]).fetchall()}
                missing_columns = [c for c in columns if c.lower() not in dcols]
                present = [c for c in columns if c.lower() in dcols]
                ref = f'"{dschema}"."{dtable}"'
                dest_rows = dst.execute(f"SELECT {', '.join(chr(34) + dcols[c.lower()] + chr(34) for c in present)} FROM {ref}").fetchall()
                dest = {r[0]: r for r in dest_rows}
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
                        if isinstance(sv, datetime) and isinstance(dv, datetime):
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
                                  "diffs": {k: diffs[k] for k in sorted(diffs)[:10]},
                                  "dest_columns": sorted(dcols), "dest_ref": ref}
        finally:
            dst.close()
        return ok, "; ".join(summary), details

    # -- reporting ----------------------------------------------------------------------------

    def record(self, number, name, verdict, detail):
        self.results.append((number, name, verdict, detail))
        print(f"[{verdict}] {number}. {name}: {detail}", flush=True)

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


def writes_during_read(hook_path, after_rows):
    """Small fetch batches; once `after_rows` orders rows have been read, run the statements in hook_path on a
    second connection while the server cursor is still open (scenario 12)."""
    def patch(source):
        source = sub(source, "BATCH_SIZE = 10_000", "BATCH_SIZE = 500")
        source = sub(source, "LOB_BATCH_SIZE = 100", "LOB_BATCH_SIZE = 50")
        source = sub(source, "def _read_rows(", (
            "def _scenario_hook(timer, rows):\n"
            "    if timer.key != 'FTTest.orders' or _HOOK['done']:\n"
            "        return\n"
            "    _HOOK['rows'] += rows\n"
            f"    if _HOOK['rows'] < {after_rows}:\n"
            "        return\n"
            "    _HOOK['done'] = True\n"
            "    import json\n"
            f"    spec = json.load(open({hook_path!r}))\n"
            "    hook_conn = _mimerpy().connect(dsn=spec['dsn'], user=spec['user'], password=spec['password'])\n"
            "    hook_conn.autocommit = True\n"
            "    hook_cursor = hook_conn.cursor()\n"
            "    for statement in spec['statements']:\n"
            "        hook_cursor.execute(statement)\n"
            "    hook_cursor.close()\n"
            "    hook_conn.close()\n"
            "    log.info(f\"scenario 12: ran {len(spec['statements'])} statements on a second connection after {_HOOK['rows']} rows\")\n"
            "\n\n_HOOK = {'done': False, 'rows': 0}\n\n\ndef _read_rows("))
        source = sub(source, "            rows = cursor.fetchmany(batch_size)\n",
                     "            _scenario_hook(timer, len(batch))\n            rows = cursor.fetchmany(batch_size)\n")
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
    r.x(f"UPDATE {ORDERS} SET amount = amount + 1.11, updated_at = TIMESTAMP '2025-02-03 00:00:00' "
        "WHERE id BETWEEN 101 AND 200")
    step(3, "Incremental: 100 updated rows", "s03_update")

    # 4. Soft deletes
    r.x(f"UPDATE {ORDERS} SET status = 'cancelled', updated_at = TIMESTAMP '2025-02-04 00:00:00' "
        "WHERE id BETWEEN 201 AND 250")
    step(4, "Soft deletes: 50 rows set to cancelled", "s04_soft_delete")

    # 5. Hard deletes: expected gap for a cursor-based sync
    r.x(f"DELETE FROM {ORDERS} WHERE id BETWEEN 251 AND 260")
    deleted = {"orders": list(range(251, 261))}
    step(5, "Hard deletes: 10 rows", "s05_hard_delete", expected_extra=deleted, verdict_if_ok="PASS (known gap)",
         note="the 10 deleted rows stay in the destination, not flagged deleted: cursor-based sync cannot see deletes")

    # 6. Outage: the database is unreachable, then a backlog arrives
    before = open(os.path.join(main, "state.json")).read()
    res = r.sync(main, "s06_outage", config=r.bad_port_config)
    after = open(os.path.join(main, "state.json")).read()
    error = next((line.strip() for line in res["text"].splitlines() if "Cannot reach Mimer" in line), "")
    outage_problems = []
    if res["ok"] or res["rc"] == 0:
        outage_problems.append("sync against a closed port did not fail")
    if before != after:
        outage_problems.append("state changed by the failed sync")
    if not error:
        outage_problems.append("no clear connection error in the log")
    r.insert_orders(10501, 10700, "2025-02-06 00:00:00")
    expected = r.expected_reads(main)
    res2 = r.sync(main, "s06_after_outage")
    outage_problems += r.check_sync(res2, expected_upserts=expected)
    ok, summary, _ = r.compare(main, deleted)
    r.record(6, "Outage, then 200-row backlog", "PASS" if ok and not outage_problems else "FAIL",
             f"failed sync rc {res['rc']}, state unchanged: {before == after}, error: {error[:200]!r}; "
             f"then {res2['upserts']} upserts (expected {expected}); {summary}"
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
        # fivetran debug does not write the state file when the sync fails; production keeps every checkpoint.
        # Take the last checkpoint the SDK recorded from the log.
        recorded = re.findall(r"checkpoint recorded: (\{.*\})\s*$", res["text"], re.MULTILINE)
        saved = json.loads(recorded[-1]) if recorded else {}
        with open(os.path.join(crash, "state.json"), "w") as f:
            json.dump(saved, f)
        source_of_state = "last 'checkpoint recorded' line of the log"
    orders_state = saved.get("tables", {}).get(ORDERS, {})
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
    ok, summary, _ = r.compare(crash, deleted)
    r.record(7, "Mid-sync failure and resume", "PASS" if ok and not crash_problems else "FAIL",
             f"crashed after 5 checkpoints with {rows_after_crash:,} orders in the destination; resumed from the {source_of_state} "
             f"(mid-table: {mid_table}, after={orders_state.get('after')}); 300 rows inserted before the resume; "
             f"resume upserted {res2['upserts']}; {summary}"
             + (f"; {crash_problems}" if crash_problems else ""))

    # 7 left 300 new rows in the source, which the main destination has not seen yet: they come with scenario 8.

    # 8. Add a column
    r.x(f"ALTER TABLE {ORDERS} ADD COLUMN discount_pct DECIMAL(5,2)")
    r.x(f"UPDATE {ORDERS} SET discount_pct = MOD(id, 50) + 0.25, updated_at = TIMESTAMP '2025-02-08 00:00:00' "
        "WHERE id BETWEEN 301 AND 400")
    res, details = step(8, "Schema drift: add discount_pct, set on 100 rows", "s08_add_column", deleted)
    if "discount_pct" not in details["orders"]["dest_columns"]:
        r.results[-1] = (8, r.results[-1][1], "FAIL", r.results[-1][3] + "; discount_pct missing in destination")
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    set_rows = dst.execute(f'SELECT COUNT("discount_pct") FROM {details["orders"]["dest_ref"]}').fetchone()[0]
    dst.close()
    r.results[-1] = r.results[-1][:3] + (r.results[-1][3] + f"; {set_rows} destination rows have a discount_pct",)

    # 9. Add then drop a column
    r.x(f"ALTER TABLE {ORDERS} ADD COLUMN notes NVARCHAR(500)")
    r.x(f"UPDATE {ORDERS} SET notes = 'note ' || CAST(id AS VARCHAR(10)), updated_at = TIMESTAMP '2025-02-09 00:00:00' "
        "WHERE id BETWEEN 401 AND 410")
    step(9, "Schema drift: add notes (step 1 of 2)", "s09a_add_notes", deleted)
    r.x(f"ALTER TABLE {ORDERS} DROP COLUMN notes CASCADE")
    r.x(f"UPDATE {ORDERS} SET updated_at = TIMESTAMP '2025-02-09 12:00:00' WHERE id BETWEEN 401 AND 420")
    res_drop, details = step(9, "Schema drift: drop notes (step 2 of 2)", "s09b_drop_notes", deleted)
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    notes = dst.execute(f'SELECT COUNT(*), COUNT("notes") FROM {details["orders"]["dest_ref"]} WHERE "id" BETWEEN 401 AND 420').fetchone()
    dst.close()
    drift_logged = "columns no longer in the source: notes" in res_drop["text"]
    r.record(9, "Schema drift: after the drop", "PASS" if res_drop["ok"] and drift_logged else "FAIL",
             f"drop logged: {drift_logged}; destination keeps notes; of the 20 rows re-synced after the drop, "
             f"{notes[1]} still have a notes value")

    # 10. Large payloads: about 1M characters of text per row, including multi-byte characters
    r.x(f"ALTER TABLE {ORDERS} ADD COLUMN payload NCLOB(4M)")
    r.insert_orders(11001, 11005, "2025-02-10 00:00:00")
    hashes = {}
    for order_id in range(11001, 11006):
        value = (f"payload {order_id} é€ 😀 " * 60000)[:1_000_000 + order_id - 11001]
        hashes[order_id] = (len(value), hashlib.sha256(value.encode()).hexdigest())
        r.x(f"UPDATE {ORDERS} SET payload = ? WHERE id = ?", [value, order_id])
    sizes = r.x(f"SELECT MIN(CHAR_LENGTH(payload)), MAX(CHAR_LENGTH(payload)) FROM {ORDERS} WHERE payload IS NOT NULL")[0]
    res, details = step(10, "Large payloads: 5 rows of ~1M characters", "s10_large_payload", deleted,
                        note=f"source payload lengths {sizes[0]:,}-{sizes[1]:,} characters, compared in full")
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    arrived = {i: (len(v), hashlib.sha256(v.encode()).hexdigest()) for i, v in dst.execute(
        f'SELECT "id", "payload" FROM {details["orders"]["dest_ref"]} WHERE "payload" IS NOT NULL').fetchall()}
    dst.close()
    intact = arrived == hashes
    if not intact:
        r.results[-1] = (10, r.results[-1][1], "FAIL", r.results[-1][3] + "; payload length/hash mismatch")
    r.results[-1] = r.results[-1][:3] + (r.results[-1][3] + f"; length and SHA-256 equal for all 5: {intact}",)

    # 11. Rows written exactly at the saved cursor value
    cursor = r.state(main)["tables"][ORDERS]["cursor"]
    max_updated = r.one(f"SELECT MAX(updated_at) FROM {ORDERS}")
    r.x(f"INSERT INTO {ORDERS} (id, customer_id, amount, status, created_at, updated_at) "
        f"SELECT id, customer_id, amount, status, created_at, CAST(? AS TIMESTAMP(6)) "
        f"FROM {SEEDED}.orders WHERE id BETWEEN 11101 AND 11120", [cursor])
    step(11, "Cursor boundary: 20 rows at exactly the saved cursor", "s11_boundary", deleted,
         note=f"saved cursor {cursor}, source max before the insert {max_updated}")

    # 12. Concurrent writes from a second connection while the orders cursor is open
    r.x(f"UPDATE {ORDERS} SET updated_at = TIMESTAMP '2025-02-12 00:00:00' + CAST(id AS INTERVAL SECOND(9)) "
        "WHERE id BETWEEN 1001 AND 4000")
    hook_path = os.path.join(r.work, "s12_hook.json")
    statements = [
        # already read in this sync (the hook runs after about 2,000 rows: ids up to ~3000): moved past the reader
        f"UPDATE {ORDERS} SET amount = amount + 5, updated_at = TIMESTAMP '2025-02-13 00:00:00' WHERE id BETWEEN 1001 AND 1050",
        # not read yet
        f"UPDATE {ORDERS} SET amount = amount + 7, updated_at = TIMESTAMP '2025-02-13 00:00:01' WHERE id BETWEEN 3901 AND 3950",
        # new rows
        f"INSERT INTO {ORDERS} (id, customer_id, amount, status, created_at, updated_at) "
        f"SELECT id, customer_id, amount, status, created_at, TIMESTAMP '2025-02-13 00:00:02' FROM {SEEDED}.orders WHERE id BETWEEN 11201 AND 11250",
        # a late commit carrying an old timestamp, below the cursor: a cursor-based sync cannot see it
        f"UPDATE {ORDERS} SET amount = amount + 9, updated_at = TIMESTAMP '2024-06-01 00:00:00' WHERE id BETWEEN 501 AND 510",
    ]
    with open(hook_path, "w") as f:
        json.dump(dict(dsn=r.base.get("database") or "mimerdb", user=r.base.get("username") or "SYSADM",
                       password=r.base["password"], statements=statements), f)
    os.chmod(hook_path, 0o600)
    expected_snapshot = r.expected_reads(main)
    r.install(main, patch=writes_during_read(hook_path, after_rows=2000))
    # The test server allows two concurrent connections (-16218 "Configured number of users exceeded" on the
    # third), so the runner's own connection is closed while the connector and the hook's writer are connected.
    r.src.close()
    res = r.sync(main, "s12_concurrent")
    r.src = connect(r.base)
    r.install(main)
    hook_ran = "scenario 12: ran 4 statements" in res["text"]
    late = {"orders": list(range(501, 511))}
    res2 = r.sync(main, "s12_followup")
    ok2, summary2, details2 = r.compare(main, deleted, late)
    late_missed = sorted(details2["orders"]["diffs"]) == list(range(501, 511))
    problems = [] if hook_ran else ["hook did not run"]
    problems += r.check_sync(res) + r.check_sync(res2)
    r.record(12, "Concurrent writes during a sync", "PASS (known gap)" if ok2 and not problems else "FAIL",
             f"writes ran on a second connection while the orders cursor was open: {hook_ran}; sync upserted {res['upserts']} "
             f"(rows matching before the writes: {expected_snapshot}), follow-up {res2['upserts']}; "
             f"after the follow-up: {summary2}; rows updated with an old timestamp (late commit) missed: {late_missed}"
             + (f"; {problems}" if problems else ""))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="scratch config with host, port, database, username, password")
    parser.add_argument("--work", required=True, help="work directory (recreated)")
    args = parser.parse_args()
    r = Runner(args.config, args.work)
    try:
        run(r)
    finally:
        print("\n| # | Scenario | Result | Detail |\n|---|---|---|---|")
        for number, name, verdict, detail in r.results:
            print(f"| {number} | {name} | {verdict} | {detail} |")
        with open(os.path.join(r.work, "results.json"), "w") as f:
            json.dump(r.results, f, indent=1, default=str)


if __name__ == "__main__":
    main()
