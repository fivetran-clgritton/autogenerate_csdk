"""Run the 12 test scenarios (CLAUDE.md, "Test Scenarios") against the Geode connector on a small fixture.

Usage (on the VM, with a scratch config that holds the connection settings, e.g. {"host": "localhost", "rest_port": "8081"}):

    /usr/bin/python3 geode/scenarios.py --config <scratch>/config.json --work <scratch>/scenarios

Geode has no schemas, so the fixture is two regions of its own: fttest_customers (REPLICATE, seeded entries with
id <= 1000) and fttest_orders (PARTITION, seeded entries with id <= 10000), recreated with gfsh in the `geode`
container and filled through the REST API with the same keys and values as the seeded regions, which are only
read. Each sync is a `fivetran debug` run of a copy of connector.py in a work directory, and after every scenario
the whole destination is compared field by field with the source. Scenarios 7 and 12 run patched copies of the
connector (an injected crash; writes from a second client between ordered queries), so they are deterministic.
Needs requests, duckdb (the `fivetran` CLI's Python has both) and `docker exec` access to the Geode container.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta

import duckdb
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURE = {"fttest_customers": ("customers", 1000, "REPLICATE"), "fttest_orders": ("orders", 10000, "PARTITION")}
CONTAINER = "geode"
DEST_SCHEMA = "tester"  # where `fivetran debug` writes tables when no schema is given
PUT_BATCH = 200


def sub(text, old, new):
    """Replace exactly one occurrence, so a patch fails loudly if connector.py changes."""
    assert text.count(old) == 1, f"patch target not found exactly once: {old!r}"
    return text.replace(old, new)


class Geode:
    """Test-side access to Geode through the REST API (and gfsh for region DDL)."""

    def __init__(self, base):
        self.base = base
        self.s = requests.Session()

    def oql(self, query):
        # The REST API URL-decodes q twice (see _double_decode_safe in connector.py)
        r = self.s.get(f"{self.base}/queries/adhoc", params={"q": query.replace("%", "%25").replace("+", "%2B")}, timeout=600)
        r.raise_for_status()
        return r.json()

    def entries(self, region, where=""):
        rows = self.oql(f"SELECT e.key AS k, e.value AS v FROM /{region}.entrySet e {where}")
        return {row["k"]: row["v"] for row in rows}

    def by_ids(self, region, first, last):
        found = {}
        ids = list(range(first, last + 1))
        for start in range(0, len(ids), 300):
            keys = ", ".join(f"'{i}'" for i in ids[start:start + 300])
            found.update(self.entries(region, f"WHERE e.key IN SET({keys})"))
        return found

    def put(self, region, docs):
        items = list(docs.items())
        for start in range(0, len(items), PUT_BATCH):
            batch = items[start:start + PUT_BATCH]
            keys = ",".join(str(k) for k, _ in batch)
            r = self.s.put(f"{self.base}/{region}/{keys}", data=json.dumps([d for _, d in batch]),
                           headers={"Content-Type": "application/json"}, timeout=300)
            if not r.ok:
                raise RuntimeError(f"PUT {region} failed ({r.status_code}): {r.text[:300]}")

    def delete(self, region, keys):
        r = self.s.delete(f"{self.base}/{region}/{','.join(str(k) for k in keys)}", timeout=60)
        if not r.ok:
            raise RuntimeError(f"DELETE {region} failed ({r.status_code}): {r.text[:300]}")

    def count(self, region, where=""):
        return self.oql(f"SELECT count(*) FROM /{region}.entrySet e {where}")[0]


def gfsh(*commands):
    args = ["docker", "exec", CONTAINER, "gfsh", "-e", "connect --locator=localhost[10334]"]
    for command in commands:
        args += ["-e", command]
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gfsh failed:\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}")


class Runner:
    def __init__(self, config_path, work):
        self.base_config = json.load(open(config_path))
        self.work = os.path.abspath(work)
        shutil.rmtree(self.work, ignore_errors=True)
        os.makedirs(os.path.join(self.work, "logs"))
        scheme = "https" if str(self.base_config.get("use_https", "")).lower() == "true" else "http"
        self.geode = Geode(f"{scheme}://{self.base_config['host']}:{self.base_config.get('rest_port') or 8081}/geode/v1")
        self.config = self._write_config("config.json", {})
        self.bad_port_config = self._write_config("config_closed_port.json", {"rest_port": "1"})
        self.results = []
        self.log_seq = 0

    # -- source -------------------------------------------------------------------------------

    def build_fixture(self):
        commands = []
        for region, (_, _, region_type) in FIXTURE.items():
            commands += [f"destroy region --name=/{region} --if-exists", f"create region --name=/{region} --type={region_type}"]
        gfsh(*commands)
        for region, (seeded, max_id, _) in FIXTURE.items():
            docs = self.geode.entries(seeded, f"WHERE e.value.id <= {max_id}L")
            assert len(docs) == max_id, f"{seeded}: expected {max_id} seeded entries, found {len(docs)}"
            self.geode.put(region, docs)

    def insert_orders(self, first, last, updated_at, extra=None):
        """Copy seeded orders first..last into the fixture with a fixed updated_at (they are not in the fixture yet)."""
        docs = self.geode.by_ids("orders", first, last)
        for doc in docs.values():
            doc["updated_at"] = updated_at
            doc.update(extra or {})
        self.geode.put("fttest_orders", docs)

    def update_orders(self, first, last, change):
        docs = self.geode.by_ids("fttest_orders", first, last)
        for key, doc in docs.items():
            change(int(key), doc)
        self.geode.put("fttest_orders", docs)
        return docs

    # -- connector runs -----------------------------------------------------------------------

    def _write_config(self, name, overrides):
        config = dict(self.base_config, include_tables=",".join(FIXTURE), exclude_tables="",
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
        """Entries the next incremental sync should read: cursor >= saved cursor, plus entries without one."""
        tables = self.state(path).get("tables", {})
        total = 0
        for region in FIXTURE:
            cursor = tables[region]["cursor"]
            total += self.geode.count(region, f"WHERE e.value.updated_at >= '{cursor}' OR e.value.updated_at = null "
                                              "OR IS_UNDEFINED(e.value.updated_at)")
        return total

    # -- destination comparison ---------------------------------------------------------------

    def compare(self, path, expected_extra=None, expected_diff_ids=None):
        """Field-by-field comparison of every source entry with the destination.

        expected_extra: {region: keys} entries deleted at the source that a cursor sync cannot see.
        expected_diff_ids: {region: keys} entries whose differences are an expected, documented gap.
        Returns (ok, summary, details).
        """
        expected_extra = expected_extra or {}
        expected_diff_ids = expected_diff_ids or {}
        dst = duckdb.connect(os.path.join(path, "files", "warehouse.db"), read_only=True)
        ok, summary, details = True, [], {}
        try:
            for region in FIXTURE:
                source = self.geode.entries(region)
                fields = sorted({f for doc in source.values() for f in doc})
                dcols = [r[0] for r in dst.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
                    "ORDER BY ordinal_position", [DEST_SCHEMA, region]).fetchall()]
                missing_columns = [f for f in fields if f not in dcols]
                ref = f'"{DEST_SCHEMA}"."{region}"'
                rows = dst.execute(f"SELECT {', '.join(chr(34) + c + chr(34) for c in dcols)} FROM {ref}").fetchall()
                dest = {r[dcols.index("_key")]: dict(zip(dcols, r)) for r in rows}
                duplicates = len(rows) - len(dest)
                deleted_flagged = sum(1 for r in dest.values() if r["_fivetran_deleted"])

                missing = sorted(set(source) - set(dest), key=int)
                extra = sorted(set(dest) - set(source), key=int)
                diffs = {}
                compared = [c for c in dcols if not c.startswith("_fivetran") and c != "_key"]
                for key in set(source) & set(dest):
                    for column in compared:
                        if not _equal(source[key].get(column), dest[key][column]):
                            diffs.setdefault(key, []).append(column)
                unexpected_extra = sorted(set(extra) - set(expected_extra.get(region, ())))
                unexpected_diffs = sorted(set(diffs) - set(expected_diff_ids.get(region, ())))
                table_ok = not (missing_columns or missing or unexpected_extra or unexpected_diffs or duplicates)
                ok = ok and table_ok
                summary.append(f"{region}: source {len(source):,}, destination {len(rows):,}"
                               + (f", {duplicates} duplicate keys" if duplicates else "")
                               + (f", missing columns {missing_columns}" if missing_columns else "")
                               + (f", {len(missing)} missing rows" if missing else "")
                               + (f", {len(extra)} rows only in destination" if extra else "")
                               + (f", {len(diffs)} rows differ" if diffs else "")
                               + (f", {deleted_flagged} flagged deleted" if deleted_flagged else ""))
                details[region] = {"missing": missing[:10], "extra": extra[:20],
                                   "diffs": {k: diffs[k] for k in sorted(diffs, key=int)[:10]},
                                   "dest_columns": dcols, "dest_types": dict(dst.execute(
                                       "SELECT column_name, data_type FROM information_schema.columns "
                                       "WHERE table_schema = ? AND table_name = ?", [DEST_SCHEMA, region]).fetchall())}
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


def _equal(source_value, dest_value):
    """Source values are JSON (timestamps as ISO strings); destination values are typed."""
    if isinstance(dest_value, datetime) and isinstance(source_value, str):
        return datetime.fromisoformat(source_value) == dest_value.replace(tzinfo=None)
    if isinstance(dest_value, float) and isinstance(source_value, (int, float)):
        return float(source_value) == dest_value
    return source_value == dest_value


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
    """1,000 entries per ordered query; before the third fttest_orders query, run the writes in hook_path from a
    second client."""
    def patch(source):
        source = sub(source, "KEY_BATCH_SIZE = 100_000", "KEY_BATCH_SIZE = 1_000")
        source = sub(source, "def _ordered_positions(", (
            "def _scenario_hook(timer):\n"
            "    if timer.key != 'fttest_orders' or timer.queries != 2 or _HOOK['done']:\n"
            "        return\n"
            "    _HOOK['done'] = True\n"
            f"    spec = json.load(open({hook_path!r}))\n"
            "    session = requests.Session()\n"
            "    for write in spec['writes']:\n"
            "        keys = ','.join(write['docs'])\n"
            "        r = session.put(f\"{spec['base']}/{write['region']}/{keys}\", data=json.dumps(list(write['docs'].values())),\n"
            "                        headers={'Content-Type': 'application/json'}, timeout=120)\n"
            "        r.raise_for_status()\n"
            "    log.info(f\"scenario 12: ran {len(spec['writes'])} writes from a second client\")\n"
            "\n\n_HOOK = {'done': False}\n\n\ndef _ordered_positions("))
        source = sub(source, "            rows = client.oql(query)\n        except GeodeError as e:\n            hint",
                     "            _scenario_hook(timer)\n            rows = client.oql(query)\n        except GeodeError as e:\n            hint")
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
    problems = r.check_sync(res, expected_upserts=sum(n for _, n, _ in FIXTURE.values()))
    ok, summary, details = r.compare(main)
    types = details["fttest_orders"]["dest_types"]
    r.record(1, "Initial sync baseline", "PASS" if ok and not problems else "FAIL",
             f"{res['upserts']} upserts; {summary}; orders types: "
             + ", ".join(f"{c} {t}" for c, t in types.items() if not c.startswith("_fivetran"))
             + (f"; {problems}" if problems else ""))

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
    r.insert_orders(10001, 10500, "2025-02-02T00:00:00")
    step(2, "Incremental: 500 new rows", "s02_insert")

    # 3. Updates (100 rows sharing one new updated_at: ties on the cursor)
    def bump_amount(i, doc):
        doc["amount"] = round(doc["amount"] + 1.11, 2)
        doc["updated_at"] = "2025-02-03T00:00:00"
    r.update_orders(101, 200, bump_amount)
    step(3, "Incremental: 100 updated rows", "s03_update")

    # 4. Soft deletes
    r.update_orders(201, 250, lambda i, doc: doc.update(status="cancelled", updated_at="2025-02-04T00:00:00"))
    step(4, "Soft deletes: 50 rows set to cancelled", "s04_soft_delete")

    # 5. Hard deletes: expected gap for a cursor-based sync
    r.geode.delete("fttest_orders", range(251, 261))
    deleted = {"fttest_orders": [str(i) for i in range(251, 261)]}
    step(5, "Hard deletes: 10 rows", "s05_hard_delete", expected_extra=deleted, verdict_if_ok="PASS (known gap)",
         note="the 10 deleted rows stay in the destination, not flagged deleted: cursor-based sync cannot see deletes")

    # 6. Outage: the REST API is unreachable, then a backlog arrives
    before = open(os.path.join(main, "state.json")).read()
    res = r.sync(main, "s06_outage", config=r.bad_port_config)
    after = open(os.path.join(main, "state.json")).read()
    error = next((line.strip() for line in res["text"].splitlines() if "Could not reach the Geode REST API" in line), "")
    outage_problems = []
    if res["ok"] or res["rc"] == 0:
        outage_problems.append("sync against a closed port did not fail")
    if before != after:
        outage_problems.append("state changed by the failed sync")
    r.insert_orders(10501, 10700, "2025-02-06T00:00:00")
    expected = r.expected_reads(main)
    res2 = r.sync(main, "s06_after_outage")
    outage_problems += r.check_sync(res2, expected_upserts=expected)
    ok, summary, _ = r.compare(main, deleted)
    r.record(6, "Outage, then 200-row backlog", "PASS" if ok and not outage_problems else "FAIL",
             f"failed sync rc {res['rc']}, state unchanged: {before == after}, error: {error[:160]!r}; "
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
        # If `fivetran debug` did not write the state file on failure, take the last checkpoint from the log;
        # production keeps every checkpoint.
        recorded = re.findall(r"checkpoint recorded: (\{.*\})\s*$", res["text"], re.MULTILINE)
        saved = json.loads(recorded[-1]) if recorded else {}
        with open(os.path.join(crash, "state.json"), "w") as f:
            json.dump(saved, f)
        source_of_state = "last 'checkpoint recorded' line of the log"
    orders_state = saved.get("tables", {}).get("fttest_orders", {})
    mid_table = "after" in orders_state
    dst = duckdb.connect(os.path.join(crash, "files", "warehouse.db"), read_only=True)
    rows_after_crash = dst.execute(f'SELECT COUNT(*) FROM "{DEST_SCHEMA}"."fttest_orders"').fetchone()[0]
    dst.close()
    r.insert_orders(10701, 11000, "2025-02-07T00:00:00")
    r.install(crash)  # resume with the real connector
    res2 = r.sync(crash, "s07_resume")
    crash_problems += r.check_sync(res2)
    if not mid_table:
        crash_problems.append("saved state was not mid-table (no 'after' position for fttest_orders)")
    ok, summary, _ = r.compare(crash, deleted)
    r.record(7, "Mid-sync failure and resume", "PASS" if ok and not crash_problems else "FAIL",
             f"crashed after 5 checkpoints with {rows_after_crash:,} orders in the destination; resumed from the {source_of_state} "
             f"(mid-table: {mid_table}); 300 rows inserted before the resume; resume upserted {res2['upserts']}; {summary}"
             + (f"; {crash_problems}" if crash_problems else ""))

    # 8. Add a field (Geode's equivalent of ADD COLUMN: new entries carry it)
    r.update_orders(301, 400, lambda i, doc: doc.update(discount_pct=(i % 50) + 0.25, updated_at="2025-02-08T00:00:00"))
    res, details = step(8, "Schema drift: add discount_pct, set on 100 rows", "s08_add_column", deleted)
    present = "discount_pct" in details["fttest_orders"]["dest_columns"]
    warned = "new fields discount_pct" in res["text"]
    r.results[-1] = (8, r.results[-1][1], r.results[-1][2] if present and warned else "FAIL",
                     r.results[-1][3] + f"; column present: {present} ({details['fttest_orders']['dest_types'].get('discount_pct')}); "
                     f"drift warning: {warned}")

    # 9. Add then drop a field
    r.update_orders(401, 410, lambda i, doc: doc.update(notes=f"note {i}", updated_at="2025-02-09T00:00:00"))
    res_add, _ = step(9, "Schema drift: add notes (step 1 of 2)", "s09a_add_notes", deleted)

    def drop_notes(i, doc):
        doc.pop("notes", None)
        doc["updated_at"] = "2025-02-09T12:00:00"
    r.update_orders(401, 420, drop_notes)
    res_drop, _ = step(9, "Schema drift: drop notes (step 2 of 2)", "s09b_drop_notes", deleted)
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    notes = dst.execute(f'SELECT COUNT(*), COUNT("notes") FROM "{DEST_SCHEMA}"."fttest_orders" WHERE CAST("_key" AS INT) BETWEEN 401 AND 420').fetchone()
    dst.close()
    drift_logged = "fields no longer in the sampled entries: notes" in res_drop["text"]
    r.record(9, "Schema drift: after the drop", "PASS" if res_drop["ok"] and drift_logged and notes[1] == 0 else "FAIL",
             f"drop logged: {drift_logged}; destination keeps the notes column; of the 20 rows re-synced after the drop, "
             f"{notes[1]} still have a notes value")

    # 10. Large payloads: about 1 MB of text per entry, including multi-byte characters
    r.insert_orders(11001, 11005, "2025-02-10T00:00:00")
    r.update_orders(11001, 11005, lambda i, doc: doc.update(payload=f"payload {i} é€ " * 50000))
    payloads = {k: v["payload"] for k, v in r.geode.by_ids("fttest_orders", 11001, 11005).items()}
    sizes = sorted(len(p) for p in payloads.values())
    res, _ = step(10, "Large payloads: 5 rows of ~1M characters", "s10_large_payload", deleted,
                  note=f"payload lengths {sizes[0]:,}-{sizes[-1]:,} characters, compared in full")
    dst = duckdb.connect(os.path.join(main, "files", "warehouse.db"), read_only=True)
    dest_payloads = dict(dst.execute(f'SELECT "_key", "payload" FROM "{DEST_SCHEMA}"."fttest_orders" WHERE "payload" IS NOT NULL').fetchall())
    dst.close()
    hashes_match = all(hashlib.sha256(dest_payloads.get(k, "").encode()).hexdigest() == hashlib.sha256(p.encode()).hexdigest()
                       for k, p in payloads.items())
    r.results[-1] = (10, r.results[-1][1], r.results[-1][2] if hashes_match else "FAIL",
                     r.results[-1][3] + f"; sha256 of all 5 payloads match: {hashes_match}")

    # 11. Rows written exactly at the saved cursor value
    cursor = r.state(main)["tables"]["fttest_orders"]["cursor"]
    r.insert_orders(11101, 11120, cursor)
    step(11, "Cursor boundary: 20 rows at exactly the saved cursor", "s11_boundary", deleted, note=f"saved cursor {cursor}")

    # 12. Concurrent writes between the ordered queries of one sync
    base = datetime(2025, 2, 12)
    r.update_orders(1001, 4000, lambda i, doc: doc.update(updated_at=(base + timedelta(seconds=i)).isoformat()))
    current = r.geode.by_ids("fttest_orders", 1, 4000)
    writes = []

    def changed(first, last, change):
        docs = {str(i): dict(current[str(i)]) for i in range(first, last + 1)}
        for key, doc in docs.items():
            change(int(key), doc)
        writes.append({"region": "fttest_orders", "docs": docs})
    # already read in this sync (ids 1001-3000 come back in the first two queries): moved past the reader
    changed(1001, 1050, lambda i, d: d.update(amount=round(d["amount"] + 5, 2), updated_at="2025-02-13T00:00:00"))
    # not read yet
    changed(3901, 3950, lambda i, d: d.update(amount=round(d["amount"] + 7, 2), updated_at="2025-02-13T00:00:01"))
    # new rows
    new_docs = r.geode.by_ids("orders", 11201, 11250)
    for doc in new_docs.values():
        doc["updated_at"] = "2025-02-13T00:00:02"
    writes.append({"region": "fttest_orders", "docs": new_docs})
    # a late commit carrying an old timestamp, below the cursor: a cursor-based sync cannot see it
    changed(501, 510, lambda i, d: d.update(amount=round(d["amount"] + 9, 2), updated_at="2024-06-01T00:00:00"))
    hook_path = os.path.join(r.work, "s12_hook.json")
    with open(hook_path, "w") as f:
        json.dump({"base": r.geode.base, "writes": writes}, f)
    r.install(main, patch=writes_between_queries(hook_path))
    res = r.sync(main, "s12_concurrent")
    r.install(main)
    hook_ran = f"scenario 12: ran {len(writes)} writes" in res["text"]
    late = {"fttest_orders": [str(i) for i in range(501, 511)]}
    ok1, summary1, _ = r.compare(main, deleted, late)
    res2 = r.sync(main, "s12_followup")
    ok2, summary2, details2 = r.compare(main, deleted, late)
    late_missed = sorted(details2["fttest_orders"]["diffs"], key=int) == late["fttest_orders"]
    problems = [] if hook_ran else ["hook did not run"]
    problems += r.check_sync(res) + r.check_sync(res2)
    r.record(12, "Concurrent writes during a sync", "PASS (known gap)" if ok1 and ok2 and not problems else "FAIL",
             f"writes ran between ordered queries 2 and 3: {hook_ran}; sync upserted {res['upserts']}, follow-up {res2['upserts']}; "
             f"right after the sync: {summary1}; after the follow-up: {summary2}; rows updated with an old timestamp "
             f"(late commit) missed: {late_missed}" + (f"; {problems}" if problems else ""))


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="scratch config with host and rest_port (and credentials, if any)")
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
