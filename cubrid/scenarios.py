"""Run the 12 test scenarios (CLAUDE.md, "Test Scenarios") against the CUBRID connector on a small fixture.

Usage (on the VM, with a scratch config that holds the connection settings and jdbc_jar):

    PYTHONPATH=~/connector-deps /usr/bin/python3 cubrid/scenarios.py --config <scratch>/config.json --work <scratch>/scenarios

The fixture is rebuilt in the FTTEST schema from the seeded tables (customers id <= 1000, orders id <= 10000);
the seeded DBA tables are only read. Each sync is a `fivetran debug` run of a copy of connector.py in a work
directory, and after every scenario the whole destination is compared field by field with the source.
Scenarios 7 and 12 run patched copies of the connector (an injected crash; writes from a second connection
in the middle of a streamed read), so they are deterministic. Needs JPype1 and duckdb.
Adapted from exasol/scenarios.py (branch exasol-general).
"""
import argparse
import json
import os
import re
import shutil
import subprocess
from datetime import datetime
from decimal import Decimal

import duckdb
import jpype

HERE = os.path.dirname(os.path.abspath(__file__))
SEEDED = "DBA"
SCHEMA = "FTTEST"
FIXTURE = {"customers": 1000, "orders": 10000}
ORDER_COLUMNS = "id, customer_id, amount, status, created_at, updated_at"


def sub(text, old, new):
    """Replace exactly one occurrence, so a patch fails loudly if connector.py changes."""
    assert text.count(old) == 1, f"patch target not found exactly once: {old!r}"
    return text.replace(old, new)


def jdbc_url(cfg):
    return f"jdbc:cubrid:{cfg['host']}:{cfg['port']}:{cfg['database']}:::?zeroDateTimeBehavior=convertToNull"


class Runner:
    def __init__(self, config_path, work):
        self.base = json.load(open(config_path))
        self.work = os.path.abspath(work)
        shutil.rmtree(self.work, ignore_errors=True)
        os.makedirs(os.path.join(self.work, "logs"))
        jpype.startJVM("-Xmx256m", classpath=[self.base["jdbc_jar"]], convertStrings=True)
        jpype.JClass("cubrid.jdbc.driver.CUBRIDDriver")
        props = jpype.JClass("java.util.Properties")()
        props.setProperty("user", self.base["username"])
        props.setProperty("password", self.base["password"])
        self.src = jpype.JClass("java.sql.DriverManager").getConnection(jdbc_url(self.base), props)
        self.src.setAutoCommit(True)
        self.config = self._write_config("config.json", {})
        self.bad_port_config = self._write_config("config_closed_port.json", {"port": "1"})
        self.results = []
        self.log_seq = 0

    # -- source -------------------------------------------------------------------------------

    def x(self, sql):
        statement = self.src.createStatement()
        try:
            statement.execute(sql)
        finally:
            statement.close()

    def rows(self, sql):
        """All rows as tuples of strings (getString), the form the comparison normalises against."""
        statement = self.src.createStatement()
        try:
            result = statement.executeQuery(sql)
            n = result.getMetaData().getColumnCount()
            out = []
            while result.next():
                out.append(tuple(result.getString(i) for i in range(1, n + 1)))
            return out
        finally:
            statement.close()

    def one(self, sql):
        return self.rows(sql)[0][0]

    def build_fixture(self):
        for table, max_id in FIXTURE.items():
            self.x(f"DROP TABLE IF EXISTS {SCHEMA}.{table}")
            self.x(f"CREATE TABLE {SCHEMA}.{table} AS SELECT * FROM {SEEDED}.{table} WHERE id <= {max_id}")
            self.x(f"ALTER TABLE {SCHEMA}.{table} ADD CONSTRAINT pk_fx_{table} PRIMARY KEY (id)")

    def insert_orders(self, first, last, updated_at):
        """Copy seeded orders first..last into the fixture with a fixed updated_at (they are not in the fixture yet)."""
        self.x(f"INSERT INTO {SCHEMA}.orders ({ORDER_COLUMNS}) "
               f"SELECT id, customer_id, amount, status, created_at, DATETIME'{updated_at}' "
               f"FROM {SEEDED}.orders WHERE id BETWEEN {first} AND {last}")

    def columns(self, table):
        return [r[0] for r in self.rows(
            f"SELECT attr_name FROM db_attribute WHERE owner_name = '{SCHEMA}' AND class_name = '{table}' "
            "AND attr_type = 'INSTANCE' ORDER BY def_order")]

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
            total += int(self.one(f"SELECT COUNT(*) FROM {SCHEMA}.{table} WHERE updated_at IS NULL "
                                  f"OR updated_at >= DATETIME'{cursor}'"))
        return total

    # -- destination comparison ---------------------------------------------------------------

    @staticmethod
    def same(sv, dv):
        """Source value (getString text) against the destination's DuckDB value."""
        if sv is None or dv is None:
            return sv is None and dv is None
        if isinstance(dv, datetime):
            return dv.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")[:23] == sv
        if isinstance(dv, (Decimal, int, float)) and not isinstance(dv, bool):
            return Decimal(sv) == Decimal(str(dv))
        return sv == str(dv)

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
                columns = self.columns(table)
                source = {int(r[0]): r for r in self.rows(f"SELECT {', '.join(columns)} FROM {SCHEMA}.{table}")}
                dcols = {r[0].lower(): r[0] for r in dst.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
                    [SCHEMA, table]).fetchall()}
                missing_columns = [c for c in columns if c not in dcols]
                present = [c for c in columns if c in dcols]
                ref = f'"{SCHEMA}"."{table}"'
                dest_rows = dst.execute(f"SELECT {', '.join(chr(34) + dcols[c] + chr(34) for c in present)} FROM {ref}").fetchall()
                dest = {r[0]: r for r in dest_rows}
                duplicates = len(dest_rows) - len(dest)
                deleted_flagged = dst.execute(f'SELECT COUNT(*) FROM {ref} WHERE "_fivetran_deleted"').fetchone()[0]

                missing = sorted(set(source) - set(dest))
                extra = sorted(set(dest) - set(source))
                diffs = {}
                for key in set(source) & set(dest):
                    s_row = dict(zip(columns, source[key]))
                    d_row = dict(zip(present, dest[key]))
                    for column in present:
                        if not self.same(s_row[column], d_row[column]):
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
                                  "dest_columns": sorted(dcols)}
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


def writes_mid_stream(hook_path):
    """Small fetch groups and chunks; once 1,000 FTTEST.orders rows have been read, run the statements in
    hook_path on a second connection while the connector's query is still open (scenario 12).

    Later incremental syncs read each table with one streamed query (no chunks), so this is "during the
    query"; first syncs read in 1,000-row primary-key chunks with the patch, so there it falls between chunks.
    """
    def patch(source):
        source = sub(source, "FETCH_SIZE = 10_000", "FETCH_SIZE = 500")
        source = sub(source, "CHUNK_ROWS = 500_000", "CHUNK_ROWS = 1_000")
        source = sub(source, "READ_GROUP = 1_000", "READ_GROUP = 500")
        source = sub(source, "def _read_rows(", (
            "def _scenario_hook(timer, rows_read):\n"
            "    if timer.key != 'FTTEST.orders' or rows_read < 1000 or _HOOK['done']:\n"
            "        return\n"
            "    _HOOK['done'] = True\n"
            f"    spec = json.load(open({hook_path!r}))\n"
            "    props = jpype.JClass('java.util.Properties')()\n"
            "    props.setProperty('user', spec['user'])\n"
            "    props.setProperty('password', spec['password'])\n"
            "    hook_conn = jpype.JClass('java.sql.DriverManager').getConnection(spec['url'], props)\n"
            "    hook_conn.setAutoCommit(True)\n"
            "    statement = hook_conn.createStatement()\n"
            "    for sql in spec['statements']:\n"
            "        statement.executeUpdate(sql)\n"
            "    statement.close()\n"
            "    hook_conn.close()\n"
            "    log.info(f\"scenario 12: ran {len(spec['statements'])} statements on a second connection\")\n"
            "\n\n_HOOK = {'done': False}\n\n\ndef _read_rows("))
        source = sub(source, "                fetch_started = time.perf_counter()\n",
                     "                _scenario_hook(timer, rows_before + row_count)\n"
                     "                fetch_started = time.perf_counter()\n")
        source = sub(source, "    while True:\n        where = list(conditions)\n",
                     "    rows_before = 0\n    while True:\n        where = list(conditions)\n")
        source = sub(source, "        after = position\n", "        after = position\n        rows_before += row_count\n")
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
    r.x(f"UPDATE {SCHEMA}.orders SET amount = amount + 1.11, updated_at = DATETIME'2025-02-03 00:00:00' "
        "WHERE id BETWEEN 101 AND 200")
    step(3, "Incremental: 100 updated rows", "s03_update")

    # 4. Soft deletes
    r.x(f"UPDATE {SCHEMA}.orders SET status = 'cancelled', updated_at = DATETIME'2025-02-04 00:00:00' "
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
    error = next((line.strip() for line in res["text"].splitlines() if "Could not connect to CUBRID" in line), "")
    outage_problems = []
    if res["ok"] or res["rc"] == 0:
        outage_problems.append("sync against a closed port did not fail")
    if before != after:
        outage_problems.append("state changed by the failed sync")
    if not error:
        outage_problems.append("no 'Could not connect to CUBRID' message")
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
    orders_state = saved.get("tables", {}).get(f"{SCHEMA}.orders", {})
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
             f"(mid-table: {mid_table}, saved orders state {json.dumps(orders_state)}); 300 rows inserted before the resume; "
             f"resume upserted {res2['upserts']}; {summary}"
             + (f"; {crash_problems}" if crash_problems else ""))

    # 8. Add a column
    r.x(f"ALTER TABLE {SCHEMA}.orders ADD COLUMN discount_pct NUMERIC(5,2) DEFAULT NULL")
    r.x(f"UPDATE {SCHEMA}.orders SET discount_pct = MOD(id, 50) + 0.25, updated_at = DATETIME'2025-02-08 00:00:00' "
        "WHERE id BETWEEN 301 AND 400")
    res, details = step(8, "Schema drift: add discount_pct, set on 100 rows", "s08_add_column", deleted)
    if "discount_pct" not in details["orders"]["dest_columns"]:
        r.results[-1] = (8, r.results[-1][1], "FAIL", r.results[-1][3] + "; discount_pct missing in destination")
    if "new columns discount_pct" not in res["text"]:
        r.results[-1] = (8, r.results[-1][1], "FAIL", r.results[-1][3] + "; no drift warning for discount_pct")

    # 9. Add then drop a column
    r.x(f"ALTER TABLE {SCHEMA}.orders ADD COLUMN notes VARCHAR(500)")
    r.x(f"UPDATE {SCHEMA}.orders SET notes = 'note ' || id, updated_at = DATETIME'2025-02-09 00:00:00' "
        "WHERE id BETWEEN 401 AND 410")
    step(9, "Schema drift: add notes (step 1 of 2)", "s09a_add_notes", deleted)
    r.x(f"ALTER TABLE {SCHEMA}.orders DROP COLUMN notes")
    r.x(f"UPDATE {SCHEMA}.orders SET updated_at = DATETIME'2025-02-09 12:00:00' WHERE id BETWEEN 401 AND 420")
    res_drop, _ = step(9, "Schema drift: drop notes (step 2 of 2)", "s09b_drop_notes", deleted)
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    notes = dst.execute(f'SELECT COUNT(*), COUNT("notes") FROM "{SCHEMA}"."orders" WHERE "id" BETWEEN 401 AND 420').fetchone()
    dst.close()
    drift_logged = "columns no longer in the source: notes" in res_drop["text"]
    r.record(9, "Schema drift: after the drop", "PASS" if res_drop["ok"] and drift_logged else "FAIL",
             f"drop logged: {drift_logged}; destination keeps notes; of the 20 rows re-synced after the drop, "
             f"{notes[1]} still have a notes value")

    # 10. Large payloads: about 1 MB per row in a utf8 column, including multi-byte characters.
    # The non-ASCII part is built with CHR(n USING utf8) from UTF-8 bytes (é = C3A9, € = E282AC), because SQL
    # text sent by the driver is encoded in the database charset (iso88591 here), which has no €; the ASCII
    # part is cast to utf8_bin so CONCAT accepts the mix.
    # 50,000 repetitions of 'payload <id> é€ ' is 1,000,000 bytes, under the default 1 MB string_max_size_bytes.
    r.x(f"ALTER TABLE {SCHEMA}.orders ADD COLUMN payload STRING COLLATE utf8_bin")
    r.insert_orders(11001, 11005, "2025-02-10 00:00:00")
    r.x(f"UPDATE {SCHEMA}.orders SET payload = REPEAT(CONCAT(CAST(CONCAT('payload ', id, ' ') AS STRING COLLATE utf8_bin), "
        f"CHR(50089 USING utf8), CHR(14844588 USING utf8), CHR(32 USING utf8)), 50000) WHERE id BETWEEN 11001 AND 11005")
    sizes = r.rows(f"SELECT MIN(CHAR_LENGTH(payload)), MAX(CHAR_LENGTH(payload)), MIN(OCTET_LENGTH(payload)), "
                   f"MAX(OCTET_LENGTH(payload)), COUNT(payload) FROM {SCHEMA}.orders")[0]
    res, _ = step(10, "Large payloads: 5 rows of ~1 MB", "s10_large_payload", deleted,
                  note=f"{sizes[4]} payloads, {int(sizes[0]):,}-{int(sizes[1]):,} characters / "
                       f"{int(sizes[2]):,}-{int(sizes[3]):,} bytes, compared in full")
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    payloads = dict(dst.execute(f'SELECT "id", "payload" FROM "{SCHEMA}"."orders" WHERE "payload" IS NOT NULL').fetchall())
    dst.close()
    intact = sorted(k for k, v in payloads.items() if v == f"payload {k} \u00e9\u20ac " * 50000)
    r.results[-1] = (10, r.results[-1][1], r.results[-1][2],
                     r.results[-1][3] + f"; destination payloads equal to the expected Python string: {len(intact)} of 5")
    if sizes[4] != "5" or int(sizes[2]) != 1_000_000 or len(intact) != 5:
        r.results[-1] = (10, r.results[-1][1], "FAIL", r.results[-1][3] + "; payloads not written or delivered as intended")

    # 11. Rows written exactly at the saved cursor value
    cursor = r.state(main)["tables"][f"{SCHEMA}.orders"]["cursor"]
    max_updated = r.one(f"SELECT MAX(updated_at) FROM {SCHEMA}.orders")
    r.x(f"INSERT INTO {SCHEMA}.orders ({ORDER_COLUMNS}) "
        f"SELECT id, customer_id, amount, status, created_at, DATETIME'{cursor}' "
        f"FROM {SEEDED}.orders WHERE id BETWEEN 11101 AND 11120")
    step(11, "Cursor boundary: 20 rows at exactly the saved cursor", "s11_boundary", deleted,
         note=f"saved cursor {cursor}, source max {max_updated}")

    # 12. Concurrent writes from a second connection in the middle of one sync's streamed read.
    # DATETIME + n adds milliseconds in CUBRID, so id * 1000 spreads the rows one second apart.
    r.x(f"UPDATE {SCHEMA}.orders SET updated_at = DATETIME'2025-02-12 00:00:00' + id * 1000 WHERE id BETWEEN 1001 AND 4000")
    hook_path = os.path.join(r.work, "s12_hook.json")
    statements = [
        # already read in this sync (ids 1001-2000 come back before the hook runs): moved past the reader
        f"UPDATE {SCHEMA}.orders SET amount = amount + 5, updated_at = DATETIME'2025-02-13 00:00:00' WHERE id BETWEEN 1001 AND 1050",
        # not read yet
        f"UPDATE {SCHEMA}.orders SET amount = amount + 7, updated_at = DATETIME'2025-02-13 00:00:01' WHERE id BETWEEN 3901 AND 3950",
        # new rows
        f"INSERT INTO {SCHEMA}.orders ({ORDER_COLUMNS}) SELECT id, customer_id, amount, status, created_at, "
        f"DATETIME'2025-02-13 00:00:02' FROM {SEEDED}.orders WHERE id BETWEEN 11201 AND 11250",
        # a late commit carrying an old timestamp, below the cursor: a cursor-based sync cannot see it
        f"UPDATE {SCHEMA}.orders SET amount = amount + 9, updated_at = DATETIME'2024-06-01 00:00:00' WHERE id BETWEEN 501 AND 510",
    ]
    with open(hook_path, "w") as f:
        json.dump({"url": jdbc_url(r.base), "user": r.base["username"], "password": r.base["password"],
                   "statements": statements}, f)
    os.chmod(hook_path, 0o600)
    r.install(main, patch=writes_mid_stream(hook_path))
    res = r.sync(main, "s12_concurrent")
    r.install(main)
    hook_ran = "scenario 12: ran 4 statements" in res["text"]
    late = {"orders": list(range(501, 511))}
    ok1, summary1, details1 = r.compare(main, deleted, {"orders": list(range(501, 511)) + list(range(3901, 3951)) + list(range(1001, 1051))})
    res2 = r.sync(main, "s12_followup")
    ok2, summary2, details2 = r.compare(main, deleted, late)
    late_missed = sorted(details2["orders"]["diffs"]) == list(range(501, 511))
    problems = [] if hook_ran else ["hook did not run"]
    problems += r.check_sync(res) + r.check_sync(res2)
    r.record(12, "Concurrent writes during a sync", "PASS (known gap)" if ok2 and not problems else "FAIL",
             f"writes ran mid-stream after 1,000 orders rows: {hook_ran}; sync upserted {res['upserts']} ({summary1}), "
             f"follow-up {res2['upserts']}; after the follow-up: {summary2}; rows updated with an old timestamp "
             f"(late commit) missed: {late_missed}" + (f"; {problems}" if problems else ""))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="scratch config with host, port, database, username, password, jdbc_jar")
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
