"""Compare a full sync of the seeded Geode regions (customers, orders) with the source.

Usage (on the VM, after `fivetran debug --naming SOURCE` of the seeded regions in <project>):

    /usr/bin/python3 geode/compare_full_sync.py --project <project dir> [--host localhost --rest-port 8081]

Checks per region: row counts, `_fivetran_deleted`, distinct ids and keys, sums of the numeric fields, min/max of
the timestamps, per-status counts, and a field-by-field comparison of 2,000 random entries. Source figures come
from OQL aggregates; the seeded regions are only read.
"""
import argparse
import json
import os
import random
from datetime import datetime
from decimal import Decimal

import duckdb
import requests

REGIONS = {
    "customers": {"sums": ["id"], "timestamps": ["created_at", "updated_at"]},
    "orders": {"sums": ["id", "customer_id", "amount"], "timestamps": ["created_at", "updated_at"]},
}
SAMPLE = 2000


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--rest-port", default="8081")
    args = parser.parse_args()
    base = f"http://{args.host}:{args.rest_port}/geode/v1"
    session = requests.Session()

    def oql(query):
        r = session.get(f"{base}/queries/adhoc", params={"q": query.replace("%", "%25").replace("+", "%2B")}, timeout=900)
        r.raise_for_status()
        return r.json()

    dst = duckdb.connect(os.path.join(args.project, "files", "warehouse.db"), read_only=True)
    all_ok = True
    for region, spec in REGIONS.items():
        ref = f'tester."{region}"'
        checks = []

        def check(name, source, dest):
            ok = source == dest
            checks.append((name, ok, source, dest))

        src_count = oql(f"SELECT count(*) FROM /{region}")[0]
        dest_count, deleted, distinct_ids, distinct_keys = dst.execute(
            f'SELECT COUNT(*), COUNT(*) FILTER (WHERE _fivetran_deleted), COUNT(DISTINCT id), COUNT(DISTINCT _key) FROM {ref}').fetchone()
        check("count", src_count, dest_count)
        check("flagged deleted", 0, deleted)
        check("distinct ids", oql(f"SELECT count(DISTINCT e.value.id) FROM /{region}.entrySet e")[0], distinct_ids)
        check("distinct keys", src_count, distinct_keys)
        for field in spec["sums"]:
            if field == "amount":
                # Doubles: Geode's OQL sum() loses precision (2,499,456,260.00 for an exact 2,499,456,336.75 on
                # the seeded orders), so sum every value exactly on both sides
                source = sum(Decimal(repr(v)) for v in oql(f"SELECT e.value.{field} FROM /{region}.entrySet e"))
                dest = sum(Decimal(repr(r[0])) for r in dst.execute(f'SELECT "{field}" FROM {ref}').fetchall())
            else:
                source = oql(f"SELECT sum(e.value.{field}) AS s FROM /{region}.entrySet e")[0]
                source = source["s"] if isinstance(source, dict) else source
                dest = dst.execute(f'SELECT SUM("{field}") FROM {ref}').fetchone()[0]
            check(f"sum({field})", source, dest)
        for field in spec["timestamps"]:
            row = oql(f"SELECT min(e.value.{field}) AS lo, max(e.value.{field}) AS hi FROM /{region}.entrySet e")[0]
            lo, hi, nonnull = dst.execute(f'SELECT MIN("{field}"), MAX("{field}"), COUNT("{field}") FROM {ref}').fetchone()
            check(f"min({field})", datetime.fromisoformat(row["lo"]), lo)
            check(f"max({field})", datetime.fromisoformat(row["hi"]), hi)
            check(f"non-null {field}", src_count, nonnull)
        source_status = {r["status"]: r["0"] for r in oql(f"SELECT e.value.status, count(*) FROM /{region}.entrySet e GROUP BY e.value.status")}
        dest_status = dict(dst.execute(f'SELECT status, COUNT(*) FROM {ref} GROUP BY status').fetchall())
        check("per-status counts", source_status, dest_status)

        # 2,000 random entries, every field
        random.seed(7)
        keys = [str(k) for k in random.sample(range(1, src_count + 1), SAMPLE)]
        source_rows = {}
        for start in range(0, SAMPLE, 300):
            chunk = ", ".join(f"'{k}'" for k in keys[start:start + 300])
            source_rows.update({r["k"]: r["v"] for r in oql(f"SELECT e.key AS k, e.value AS v FROM /{region}.entrySet e WHERE e.key IN SET({chunk})")})
        cur = dst.execute(f"SELECT * FROM {ref} WHERE _key IN ({', '.join('?' for _ in keys)})", keys)
        names = [d[0] for d in cur.description]
        dest_rows = {r[names.index("_key")]: dict(zip(names, r)) for r in cur.fetchall()}
        mismatches = 0
        for key, doc in source_rows.items():
            row = dest_rows.get(key)
            if row is None:
                mismatches += 1
                continue
            for field, value in doc.items():
                dest_value = row.get(field)
                if isinstance(dest_value, datetime):
                    value = datetime.fromisoformat(value)
                elif isinstance(dest_value, float):
                    value = float(value)
                if value != dest_value:
                    mismatches += 1
                    break
        check(f"{SAMPLE}-row field-by-field sample (rows found in source)", len(keys), len(source_rows))
        check(f"{SAMPLE}-row field-by-field sample (mismatching rows)", 0, mismatches)

        types = dst.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'tester' "
                            "AND table_name = ? ORDER BY ordinal_position", [region]).fetchall()
        print(f"\n== {region}: destination types {', '.join(f'{c} {t}' for c, t in types)}")
        for name, ok, source, dest in checks:
            all_ok = all_ok and ok
            print(f"  [{'ok' if ok else 'MISMATCH'}] {name}: source {json.dumps(source, default=str)}, destination {json.dumps(dest, default=str)}")
    print("\nALL CHECKS PASSED" if all_ok else "\nSOME CHECKS FAILED")


if __name__ == "__main__":
    main()
