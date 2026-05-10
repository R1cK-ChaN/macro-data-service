"""Compute SQLite + ClickHouse baseline for the cutover migration (issue #140).

Runs on the local laptop against the live (or snapshotted) DBs and on the
VPS post-restore against the migrated DBs. Same tool both sides so the
comparison is like-for-like.

Output: a JSON document on stdout.

```
{
  "sqlite": {
    "file_sha256": "<hex>",
    "tables": {"<name>": <row count>, ...}
  },
  "clickhouse": {
    "tables": {"<name>": <row count>, ...},
    "hashes": {"<name>": "<sum-of-cityHash64-rows>", ...}
  }
}
```

Pure stdlib — no third-party deps so we can scp this file to the VPS and
run it with the system python3 if the .venv isn't installed yet.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sqlite_baseline(db_path: Path) -> dict:
    if not db_path.is_file():
        raise SystemExit(f"sqlite db not found: {db_path}")
    out: dict = {"file_sha256": _file_sha256(db_path), "tables": {}}
    con = sqlite3.connect(str(db_path))
    try:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        for (name,) in rows:
            cnt = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            out["tables"][name] = int(cnt)
    finally:
        con.close()
    return out


def _ch(client: list[str], query: str) -> str:
    """Run a single ClickHouse query and return stripped stdout."""
    r = subprocess.run(
        client + ["--query", query],
        capture_output=True, text=True, check=True,
    )
    return r.stdout.strip()


def clickhouse_baseline(client: list[str], database: str) -> dict:
    tables_tsv = _ch(
        client,
        f"SELECT name FROM system.tables WHERE database = '{database}' ORDER BY name FORMAT TSV",
    )
    tables = [t for t in tables_tsv.splitlines() if t]
    out: dict = {"tables": {}, "hashes": {}}
    for t in tables:
        cnt = int(_ch(client, f"SELECT count() FROM `{database}`.`{t}`"))
        # Content hash — order-independent uint64 sum over per-row
        # cityHash64. Identical rowsets → identical sum, even if part
        # ordering on disk differs across hosts.
        h = _ch(
            client,
            f"SELECT toString(sum(cityHash64(*))) FROM `{database}`.`{t}`",
        )
        out["tables"][t] = cnt
        out["hashes"][t] = h or "0"
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--sqlite-db", type=Path,
        default=Path(os.environ.get(
            "ANALYST_MACRO_DATA_DB_PATH", "/var/lib/macro-data/engine.db",
        )),
        help="Path to engine.db (default: $ANALYST_MACRO_DATA_DB_PATH or /var/lib/macro-data/engine.db).",
    )
    ap.add_argument(
        "--clickhouse-database",
        default=os.environ.get("CLICKHOUSE_DATABASE", "market"),
    )
    ap.add_argument(
        "--clickhouse-via", default="local", choices=("local", "docker"),
        help="Use local clickhouse-client (default) or `docker exec <container> clickhouse-client`.",
    )
    ap.add_argument(
        "--docker-container", default="macro-data-clickhouse",
        help="Docker container name when --clickhouse-via=docker.",
    )
    args = ap.parse_args(argv)

    if args.clickhouse_via == "local":
        client = ["clickhouse-client"]
    else:
        client = ["docker", "exec", "-i", args.docker_container, "clickhouse-client"]

    result = {
        "sqlite": sqlite_baseline(args.sqlite_db),
        "clickhouse": clickhouse_baseline(client, args.clickhouse_database),
    }
    json.dump(result, sys.stdout, sort_keys=True, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
